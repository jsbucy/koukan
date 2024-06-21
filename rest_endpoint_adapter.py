from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple )

from abc import ABC, abstractmethod
import logging
import time
from threading import Condition, Lock
import json

from flask import (
    Request as FlaskRequest,
    Response as FlaskResponse,
    jsonify )
from werkzeug.datastructures import ContentRange
import werkzeug.http

from deadline import Deadline
from response import Response as MailResponse
from blob import Blob, InlineBlob, WritableBlob

from rest_service_handler import Handler, HandlerFactory
from filter import (
    AsyncFilter,
    SyncFilter,
    TransactionMetadata,
    WhichJson )
from executor import Executor

from rest_schema import BlobUri, make_blob_uri, make_tx_uri, parse_blob_uri


# Wrapper for vanilla/sync Filter that provides async interface for REST
class SyncFilterAdapter(AsyncFilter):
    class BlobWriter(WritableBlob):
        blob : InlineBlob
        def __init__(self, parent):
            self.parent = parent
            self.blob = InlineBlob(b'')

        def append_data(self, offset : int, d : bytes,
                        content_length : Optional[int] = None
                        ) -> Tuple[bool, int, Optional[int]]:
            rv = self.blob.append_data(offset, d, content_length)
            self.parent.blob_wakeup()
            return rv

    executor : Executor
    filter : SyncFilter
    prev_tx : TransactionMetadata
    tx : TransactionMetadata
    mu : Lock
    cv : Condition
    inflight : bool = False
    rest_id : str
    _last_update : float
    _version = 0
    body_blob : Optional[BlobWriter] = None

    def __init__(self, executor : Executor, filter : SyncFilter, rest_id : str):
        self.executor = executor
        self.mu = Lock()
        self.cv = Condition(self.mu)
        self.filter = filter
        self.rest_id = rest_id
        self._last_update = time.monotonic()
        self.prev_tx = TransactionMetadata()
        self.tx = TransactionMetadata()
        self.tx.rest_id = rest_id

    def version(self):
        return self._version

    # for use in ttl/gc idle calcuation
    # returns now if there is an update inflight i.e. do not gc
    def last_update(self) -> float:
        if self.inflight:
            return time.monotonic()
        return self._last_update

    def _update(self):
        try:
            while self._update_once():
                pass
        except Exception:
            logging.exception('SyncFilterAdapter._update_once() %s',
                              self.rest_id)
            with self.mu:
                self.inflight = False
                # TODO an exception here is unexpected so
                # it's probably good enough to guarantee that callers
                # fail quickly. Though it might be more polite to
                # populate responses for infligt req fields here with
                # a temp/internal error.
                self.prev_tx = self.tx = None
                self.cv.notify_all()
            raise

    def _update_once(self):
        with self.mu:
            assert self.inflight
            delta = self.prev_tx.delta(self.tx)  # new reqs
            assert delta is not None
            logging.debug('SyncFilterAdapter._update_once() '
                          'downstream_delta %s', delta)
            self.prev_tx = self.tx.copy()
            if not delta.req_inflight() and (
                    self.tx.body_blob is None or
                    self.tx.body_blob.len() != self.tx.body_blob.content_length() or
                    self.tx.data_response is not None):
                self.inflight = False
                return False

        upstream_delta = self.filter.on_update(self.tx, delta)

        logging.debug('SyncFilterAdapter._update_once() tx after upstream %s',
                      self.tx)
        with self.mu:
            self._version += 1
            self.cv.notify_all()
            self._last_update = time.monotonic()
        return True

    def _tx_done_locked(self, tx : TransactionMetadata):
        return not tx.req_inflight(self.tx)

    def _get_locked(self, tx, timeout) -> bool:
        return self.cv.wait_for(lambda: self._tx_done_locked(tx), timeout)

    def update(self,
               tx : TransactionMetadata,
               tx_delta : TransactionMetadata,
               timeout : Optional[float] = None
               ) -> Optional[TransactionMetadata]:
        with self.mu:
            txx = self.tx.merge(tx_delta)

            # if downstream tx body blob grew
            #   append to self.tx.body_blob

            logging.debug('SyncFilterAdapter.updated merged %s', txx)

            if txx is None:
                return None
            self.tx = txx
            self._version += 1

            if not self.inflight:
                fut = self.executor.submit(lambda: self._update())
                # TODO we need a better way to report this error but
                # throwing here will -> http 500
                assert fut is not None
                self.inflight = True
            self._get_locked(self.tx, timeout)
            upstream_delta = tx.delta(self.tx)
            assert tx.merge_from(upstream_delta) is not None  # XXX
            return upstream_delta

    # TODO it looks like this effectively returns an empty tx on timeout
    def get(self, timeout : Optional[float] = None
            ) -> Optional[TransactionMetadata]:
        with self.mu:
            tx = self.tx.copy()
            self._get_locked(tx, timeout)
            return self.tx.copy()


    def get_blob_writer(self,
                        create : bool,
                        blob_rest_id : Optional[str] = None,
                        tx_body : Optional[bool] = None,
                        copy_from_tx_body : Optional[str] = None
                        ) -> Optional[WritableBlob]:
        if (not tx_body or
            copy_from_tx_body is not None):
            raise NotImplementedError()
        if create and self.body_blob is None:
            self.body_blob = SyncFilterAdapter.BlobWriter(self)
        return self.body_blob


    def blob_wakeup(self):
        logging.debug('SyncFilterAdapter.blob_wakeup %s', self.body_blob)
        tx = self.get(0)
        assert tx is not None
        tx_delta = TransactionMetadata()
        if not tx.body_blob:
            tx_delta.body_blob = self.body_blob.blob
        tx.merge_from(tx_delta)
        self.update(tx, tx_delta)


MAX_TIMEOUT=30


class RestEndpointAdapter(Handler):
    CHUNK_SIZE = 1048576
    async_filter : Optional[AsyncFilter]
    _tx_rest_id : str

    _blob_rest_id : str
    rest_id_factory : Optional[Callable[[], str]]
    http_host : Optional[str] = None

    def __init__(self, async_filter : Optional[AsyncFilter] = None,
                 tx_rest_id=None,
                 blob_rest_id=None,
                 rest_id_factory : Optional[Callable[[], str]] = None,
                 http_host : Optional[str] = None):
        self.async_filter = async_filter
        self._tx_rest_id = tx_rest_id
        self._blob_rest_id = blob_rest_id
        self.rest_id_factory = rest_id_factory
        self.http_host = http_host

    def blob_rest_id(self):
        return self._blob_rest_id

    def _get_timeout(self, req : FlaskRequest
                     ) -> Tuple[Optional[int], FlaskResponse]:
        # https://datatracker.ietf.org/doc/id/draft-thomson-hybi-http-timeout-00.html
        # return 0 i.e. no waiting if header not present
        if not (timeout_header := req.headers.get('request-timeout', None)):
            return 0, None
        timeout = None
        try:
            timeout = min(int(timeout_header), MAX_TIMEOUT)
        except ValueError:
            return None, FlaskResponse(status=400, response=[
                'invalid request-timeout header'])
        return timeout, None

    def _etag(self, version : int) -> str:
        return '%d' % version

    def create_tx(self, request : FlaskRequest) -> Optional[FlaskResponse]:
        # TODO if request doesn't have remote_addr or is not from a
        # well-known/trusted peer (i.e. smtp gateway), set remote_addr to wsgi
        # environ REMOTE_ADDR or HTTP_X_FORWARDED_FOR
        # TODO only accept smtp_meta from trusted peer i.e. the
        # well-known address of the gateway

        tx = TransactionMetadata.from_json(request.json, WhichJson.REST_CREATE)
        if tx is None:
            return FlaskResponse(status=400, response=['invalid request'])
        tx.host = self.http_host
        prev_tx = tx.copy()
        body = tx.body
        timeout, err = self._get_timeout(request)
        if self.async_filter is None:
            return FlaskResponse(
                status=500, respose=['internal error creating transaction'])
        upstream = self.async_filter.update(tx, tx.copy(), timeout)
        if upstream is None:
            return FlaskResponse(status=400, respose=['bad request'])
        assert tx.rest_id is not None
        self._tx_rest_id = tx.rest_id

        resp = FlaskResponse(status = 201)
        resp.headers.set('location', make_tx_uri(tx.rest_id))
        resp.set_etag(self._etag(self.async_filter.version()))
        resp.content_type = 'application/json'
        tx.body = body
        del tx.body_blob
        # XXX does POST /tx/123/message_builder return the tx json??
        resp.set_data(json.dumps(tx.to_json(WhichJson.REST_READ)))
        return resp

    def get_tx(self, request : FlaskRequest) -> FlaskResponse:
        timeout, err = self._get_timeout(request)
        if err is not None:
            return err
        if self.async_filter is None:
            return FlaskResponse(
                status=404, response=['transaction not found'])
        tx = self.async_filter.get(timeout)
        if tx is None:
            return FlaskResponse(status=500, response=['get tx'])

        resp = FlaskResponse(status = 200)
        # xxx move to separate "rest schema"
        resp.set_etag(self._etag(self.async_filter.version()))
        resp.content_type = 'application/json'
        if tx.body_blob is not None:
            tx.body = ''
            del tx.body_blob
        resp.set_data(json.dumps(tx.to_json(WhichJson.REST_READ)))
        return resp

    def patch_tx(self, request : FlaskRequest,
                 message_builder : bool = False) -> FlaskResponse:
        logging.debug('RestEndpointAdapter.patch %s %s',
                      self._tx_rest_id, request.json)
        timeout, err = self._get_timeout(request)
        if err is not None:
            return err
        deadline = Deadline(timeout)
        if message_builder:
            downstream_delta = TransactionMetadata()
            downstream_delta.message_builder = request.json
        else:
            downstream_delta = TransactionMetadata.from_json(
                request.json, WhichJson.REST_UPDATE)
        if downstream_delta is None:
            return FlaskResponse(status=400, response=['invalid request'])
        body = downstream_delta.body
        if err is not None:
            return err
        req_etag = request.headers.get('if-match', None)
        if req_etag is None:
            return FlaskResponse(
                status=400, response=['etags required for update'])
        req_etag = req_etag.strip('"')

        # TODO optimization in the case of StorageWriterFilter, this
        # should be able to return the tx that was just loaded in the cursor
        tx = self.async_filter.get(deadline.deadline_left())
        if tx is None:
            return FlaskResponse(
                status=500,
                response=['RestEndpointAdapter.patch_tx timeout reading tx'])

        if tx.merge_from(downstream_delta) is None:
            return FlaskResponse(
                status=400,
                response=['RestEndpointAdapter.patch_tx merge failed'])

        if req_etag != self._etag(self.async_filter.version()):
            logging.debug('RestEndpointAdapter.patch_tx conflict %s %s',
                          req_etag, self._etag(self.async_filter.version()))
            return FlaskResponse(
                status=412, response=['update conflict'])
        upstream = self.async_filter.update(
            tx, downstream_delta, deadline.deadline_left())
        if upstream is None:
            return FlaskResponse(
                status=400,
                response=['RestEndpointAdapter.patch_tx bad request'])

        resp = FlaskResponse(status=200)
        # xxx move to separate "rest schema"
        resp.set_etag(self._etag(self.async_filter.version()))
        resp.content_type = 'application/json'
        logging.debug('RestEndpointAdapter.patch_tx %s %s',
                      self._tx_rest_id, downstream_delta)
        tx.body = body
        del tx.body_blob
        resp.set_data(json.dumps(tx.to_json(WhichJson.REST_READ)))
        return resp

    def _get_range(self, request : FlaskRequest
                   ) -> Tuple[Optional[FlaskResponse], Optional[ContentRange]]:
        if (range_header := request.headers.get('content-range', None)) is None:
            return None, ContentRange(
                'bytes', 0, request.content_length, request.content_length)

        range = werkzeug.http.parse_content_range_header(
            request.headers.get('content-range'))
        logging.info('put_blob content-range: %s', range)
        if not range or range.units != 'bytes':
            return FlaskResponse(400, 'bad range'), None
        # no idea if underlying stack enforces this
        assert(range.stop - range.start == request.content_length)
        return None, range

    def create_blob(self, request : FlaskRequest,
                    tx_body : bool = False) -> FlaskResponse:
        logging.debug('RestEndpointAdapter.create_blob %s %s blob %s tx %s',
                      request, request.headers, self._blob_rest_id,
                      self._tx_rest_id)

        self._blob_rest_id = self.rest_id_factory()

        chunked = False
        if 'upload' not in request.args:
            if 'content-range' in request.headers:
                return FlaskResponse(
                    status=400,
                    response=['content-range only with chunked uploads'])
        elif request.args.keys() != {'upload'} or (
                request.args['upload'] != 'chunked'):
            return FlaskResponse(status=400, response=['bad query'])
        elif request.args.get('upload', None) == 'chunked':
            if request.data:
                return FlaskResponse(
                    status=400, response=['unimplemented metadata upload'])
            chunked = True

        logging.debug('RestEndpointAdapter.create_blob before create')

        if (blob := self.async_filter.get_blob_writer(
                create=True, blob_rest_id=self._blob_rest_id,
                tx_body=tx_body)) is None:
            return FlaskResponse(
                status=500, response=['internal error creating blob'])

        if not chunked:
            range_err, range = self._get_range(request)
            if range_err:
                return range_err
            rest_resp = self._put_blob(request, range, blob)

        resp = FlaskResponse(status=201)
        resp.status_code = 201
        blob_uri = make_blob_uri(
            self._tx_rest_id,
            blob=self._blob_rest_id if not tx_body else None,
            tx_body=tx_body)
        resp.headers.set('location', blob_uri)
        return resp

    def put_blob(self, request : FlaskRequest,
                 blob_rest_id : Optional[str] = None,
                 tx_body : bool = False) -> FlaskResponse:
        if blob_rest_id is not None:
            self._blob_rest_id = blob_rest_id
        range = None

        range_err, range = self._get_range(request)
        if range_err:
            return range_err

        blob = self.async_filter.get_blob_writer(
            create = False,
            blob_rest_id=self._blob_rest_id, tx_body=tx_body)

        if blob is None:
            return FlaskResponse(status=404, response=['unknown blob'])

        resp = self._put_blob(request, range, blob)
        logging.debug('RestEndpointAdapter.put_blob %s %s', resp, tx_body)
        if resp.status_code != 200 or not tx_body:
            return resp

        return resp


    def _put_blob(self, request : FlaskRequest, content_range : ContentRange,
                  blob : WritableBlob) -> FlaskResponse:
        logging.debug('RestEndpointAdapter._put_blob %s content-range: %s',
                      self._blob_rest_id, content_range)
        resp = FlaskResponse()
        bytes_read = 0
        content_length = result_len = None

        while b := request.stream.read(self.CHUNK_SIZE):
            appended, result_len, content_length = blob.append_data(
                content_range.start + bytes_read, b, content_range.length)
            logging.debug(
                'RestEndpointAdapter._put_blob %s %s %d %s',
                self._blob_rest_id, appended, result_len, content_length)

            if not appended:
                resp.status = 416
                resp.response = ['invalid range']
                break
            bytes_read += len(b)

        resp = jsonify({})
        # cf RestTransactionHandler.build_resp() regarding content-range
        resp.headers.set('content-range',
                         ContentRange('bytes', 0, result_len, content_length))
        return resp


class EndpointFactory(ABC):
    @abstractmethod
    def create(self, http_host : str) -> Optional[AsyncFilter]:
        pass

    @abstractmethod
    def get(self, rest_id : str) -> Optional[AsyncFilter]:
        pass

class RestEndpointAdapterFactory(HandlerFactory):
    endpoint_factory : EndpointFactory
    rest_id_factory : Callable[[], str]

    def __init__(self, endpoint_factory,
                 rest_id_factory : Callable[[], str]):
        self.endpoint_factory = endpoint_factory
        self.rest_id_factory = rest_id_factory

    def create_tx(self, http_host) -> RestEndpointAdapter:
        endpoint = self.endpoint_factory.create(http_host)
        return RestEndpointAdapter(
            async_filter=endpoint,
            http_host=http_host,
            rest_id_factory=self.rest_id_factory)

    def get_tx(self, tx_rest_id) -> RestEndpointAdapter:
        return RestEndpointAdapter(
            async_filter=self.endpoint_factory.get(tx_rest_id),
            tx_rest_id=tx_rest_id,
            rest_id_factory=self.rest_id_factory)
