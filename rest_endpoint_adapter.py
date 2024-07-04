from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union )

from abc import ABC, abstractmethod
import logging
import time
from threading import Condition, Lock
import json
import asyncio

from flask import (
    Request as FlaskRequest,
    Response as FlaskResponse,
    jsonify )
from werkzeug.datastructures import ContentRange
import werkzeug.http

from fastapi import (
    Request as FastApiRequest,
    Response as FastApiResponse )
from fastapi.responses import JSONResponse as FastApiJsonResponse

HttpRequest = Union[FlaskRequest, FastApiRequest]
HttpResponse = Union[FlaskResponse, FastApiResponse]

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


# runs SyncFilter on Executor with AsyncFilter interface for
# RestHandler -> SyncFilterAdapter -> SmtpEndpoint
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
    # transaction has reached a final status: data response or cancelled
    done : bool = False

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

    def idle(self, now : float, ttl : float, done_ttl : float):
        with self.mu:
            if self.inflight:
                return False
            t = done_ttl if self.done else ttl
            return (now - self._last_update) > t

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
            if not delta.req_inflight() and not delta.cancelled and (
                    self.tx.body_blob is None or
                    self.tx.body_blob.len() != self.tx.body_blob.content_length() or
                    self.tx.data_response is not None):
                self.inflight = False
                return False

        upstream_delta = self.filter.on_update(self.tx, delta)

        logging.debug('SyncFilterAdapter._update_once() tx after upstream %s',
                      self.tx)
        with self.mu:
            self.done = self.tx.cancelled or self.tx.data_response is not None
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
                        ) -> Optional[WritableBlob]:
        if not tx_body:
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


class RestHandler(Handler):
    CHUNK_SIZE = 1048576
    executor : Optional[Executor] = None
    async_filter : Optional[AsyncFilter]
    _tx_rest_id : str

    _blob_rest_id : str
    rest_id_factory : Optional[Callable[[], str]]
    http_host : Optional[str] = None

    # _put_blob
    range : Optional[ContentRange] = None
    blob : Optional[WritableBlob] = None
    bytes_read : Optional[int] = None

    def __init__(self,
                 executor : Optional[Executor] = None,
                 async_filter : Optional[AsyncFilter] = None,
                 tx_rest_id=None,
                 blob_rest_id=None,
                 rest_id_factory : Optional[Callable[[], str]] = None,
                 http_host : Optional[str] = None):
        self.executor = executor
        self.async_filter = async_filter
        self._tx_rest_id = tx_rest_id
        self._blob_rest_id = blob_rest_id
        self.rest_id_factory = rest_id_factory
        self.http_host = http_host

    def blob_rest_id(self):
        return self._blob_rest_id

    def response(self, req : HttpRequest,
                 code : int = 200,
                 msg : Optional[str] = None,
                 resp_json : Optional[dict] = None,
                 headers : Optional[List[Tuple[str,str]]] = None,
                 etag : Optional[str] = None
                 ) -> HttpResponse:
        if isinstance(req, FlaskRequest):
            resp = FlaskResponse(status=code)
            if etag:
                resp.set_etag(etag)
            if headers:
                for k,v in headers:
                    resp.headers.set(k,v)
            if json is not None:
                resp.content_type = 'application/json'
                resp.set_data(json.dumps(resp_json))
            if msg is not None:
                resp.content_type = 'text/plain'
                resp.content = msg
            return resp

        headers_dict={}
        if headers:
            headers_dict.update({k:str(v) for k,v in headers})
        if etag is not None:
            headers_dict['etag'] = etag
        if json is not None:
            return FastApiJsonResponse(
                status_code=code,
                content=resp_json,
                headers=headers_dict)
        return FastApiResponse(status=code, content=msg, headers=headers_dict)

    async def handle_async(self, request, fn) -> HttpResponse:
        logging.debug('RestHandler.handle_async %s', request)
        cfut = self.executor.submit(fn, 0)
        if cfut is None:
            return self.response(request, code=500, msg='failed to schedule')
        fut = asyncio.wrap_future(cfut)
        await fut
        resp = fut.result()
        logging.debug('RestHandler.handle_async %s', resp)
        return resp

    def _get_timeout(self, req : HttpRequest
                     ) -> Tuple[Optional[int], HttpResponse]:
        # https://datatracker.ietf.org/doc/id/draft-thomson-hybi-http-timeout-00.html
        # return 0 i.e. no waiting if header not present
        if not (timeout_header := req.headers.get('request-timeout', None)):
            return 0, None
        timeout = None
        try:
            timeout = min(int(timeout_header), MAX_TIMEOUT)
        except ValueError:
            return None, self.response(
                req, 400, msg='invalid request-timeout header')
        return timeout, None

    def _etag(self, version : int) -> str:
        return '%d' % version

    def create_tx(self, request : HttpRequest, req_json : dict
                  ) -> Optional[HttpResponse]:
        # TODO if request doesn't have remote_addr or is not from a
        # well-known/trusted peer (i.e. smtp gateway), set remote_addr to wsgi
        # environ REMOTE_ADDR or HTTP_X_FORWARDED_FOR
        # TODO only accept smtp_meta from trusted peer i.e. the
        # well-known address of the gateway
        logging.debug('RestHandler.create_tx %s %s', request, req_json)
        tx = TransactionMetadata.from_json(req_json, WhichJson.REST_CREATE)
        if tx is None:
            return self.response(request, code=400, msg='invalid request')
        tx.host = self.http_host
        prev_tx = tx.copy()
        body = tx.body
        timeout, err = self._get_timeout(request)
        if self.async_filter is None:
            return self.response(
                request, code=500, msg='internal error creating transaction')
        upstream = self.async_filter.update(tx, tx.copy(), timeout)
        if upstream is None:
            return self.response(request, code=400, msg='bad request')
        assert tx.rest_id is not None
        self._tx_rest_id = tx.rest_id

        tx.body = body
        del tx.body_blob
        resp = self.response(
            request, code=201,
            resp_json=tx.to_json(WhichJson.REST_READ),
            headers=[('location', make_tx_uri(tx.rest_id))],
            etag=self._etag(self.async_filter.version()))
        logging.debug('RestHandler.create_tx %s', resp)
        return resp

    def get_tx(self, request : HttpRequest) -> HttpResponse:
        timeout, err = self._get_timeout(request)
        if err is not None:
            return err
        if self.async_filter is None:
            return self.response(
                request, code=404, msg='transaction not found')
        tx = self.async_filter.get(timeout)
        if tx is None:
            return self.response(request, code=500, msg='get tx')


        if tx.body_blob is not None:
            tx.body = ''
            del tx.body_blob

        if tx.body_blob is not None:
            tx.body = ''
            del tx.body_blob
        return self.response(
            request,
            etag=self._etag(self.async_filter.version()),
            resp_json=tx.to_json(WhichJson.REST_READ))

    def patch_tx(self, request : HttpRequest,
                 req_json : dict,
                 message_builder : bool = False
                 ) -> HttpResponse:
        logging.debug('RestHandler.patch %s %s',
                      self._tx_rest_id, req_json)
        timeout, err = self._get_timeout(request)
        if err is not None:
            return err
        deadline = Deadline(timeout)
        if message_builder:
            downstream_delta = TransactionMetadata()
            downstream_delta.message_builder = req_json
        else:
            downstream_delta = TransactionMetadata.from_json(
                req_json, WhichJson.REST_UPDATE)
        if downstream_delta is None:
            return self.response(request, code=400, msg='invalid request')
        body = downstream_delta.body
        if err is not None:
            return err
        req_etag = request.headers.get('if-match', None)
        if req_etag is None:
            return self.response(
                request, code=400, msg='etags required for update')
        req_etag = req_etag.strip('"')

        # TODO optimization in the case of StorageWriterFilter, this
        # should be able to return the tx that was just loaded in the cursor
        tx = self.async_filter.get(deadline.deadline_left())
        if tx is None:
            return self.response(
                request,
                code=500,
                msg='RestHandler.patch_tx timeout reading tx')

        if tx.merge_from(downstream_delta) is None:
            return self.response(
                request, code=400,
                msg='RestHandler.patch_tx merge failed')

        if req_etag != self._etag(self.async_filter.version()):
            logging.debug('RestHandler.patch_tx conflict %s %s',
                          req_etag, self._etag(self.async_filter.version()))
            return self.response(request, code=412, msg='update conflict')
        upstream = self.async_filter.update(
            tx, downstream_delta, deadline.deadline_left())
        if upstream is None:
            return self.response(
                request, code=400,
                msg='RestHandler.patch_tx bad request')

        tx.body = body
        del tx.body_blob
        return self.response(
            request, etag=self._etag(self.async_filter.version()),
            resp_json=tx.to_json(WhichJson.REST_READ))

    def _get_range(self, request : HttpRequest
                   ) -> Tuple[Optional[HttpResponse], Optional[ContentRange]]:
        content_length = int(request.headers.get('content-length'))
        if (range_header := request.headers.get('content-range', None)) is None:
            return None, ContentRange(
                'bytes', 0, content_length, content_length)
                #request.content_length, request.content_length)

        range = werkzeug.http.parse_content_range_header(
            request.headers.get('content-range'))
        logging.info('put_blob content-range: %s', range)
        if not range or range.units != 'bytes':
            return self.response(request, 400, 'bad range'), None
        # no idea if underlying stack enforces this
        assert(range.stop - range.start == content_length) #request.content_length)
        return None, range

    def create_blob(self, request : HttpRequest,
                    tx_body : bool = False,
                    req_upload : Optional[str] = None) -> HttpResponse:
        logging.debug('RestHandler.create_blob')
        resp = self._create_blob(request, tx_body, req_upload)
        logging.debug('RestHandler.create_blob %s', resp.status)
        if resp.status_code != 201:
            return resp

        if req_upload is None:
            self.bytes_read = 0
            while b := request.stream.read(self.CHUNK_SIZE):
                logging.debug('RestHandler.create_blob chunk %d', len(b))
                self._put_blob_chunk(request, b)

        return resp

    def _create_blob(self, request : HttpRequest,
                    tx_body : bool = False,
                    req_upload : Optional[str] = None) -> HttpResponse:
        logging.debug('RestHandler._create_blob %s %s blob %s tx %s',
                      request, request.headers, self._blob_rest_id,
                      self._tx_rest_id)

        self._blob_rest_id = self.rest_id_factory()

        if req_upload is not None and req_upload != 'chunked':
            return self.response(request, code=400, msg='bad param: upload=')
        chunked = req_upload is not None and req_upload == 'chunked'
        if not chunked and 'content-range' in request.headers:
            return self.response(
                request, code=400,
                msg='content-range only with chunked uploads')
        if chunked and int(request.headers.get('content-length', '0')) > 0:
            return self.response(
                request, code=400, msg='unimplemented metadata upload')

        logging.debug('RestHandler.create_blob before create')

        if (blob := self.async_filter.get_blob_writer(
                create=True, blob_rest_id=self._blob_rest_id,
                tx_body=tx_body)) is None:
            return self.response(
                request, code=500, msg='internal error creating blob')
        self.blob = blob
        if not chunked:
            range_err, range = self._get_range(request)
            if range_err:
                return range_err
            self.range = range
            #rest_resp = self._put_blob(request, range, blob)

        blob_uri = make_blob_uri(
            self._tx_rest_id,
            blob=self._blob_rest_id if not tx_body else None,
            tx_body=tx_body)
        return self.response(request, code=201,
                             headers=[('location', blob_uri)])

    async def create_blob_async(
            self, request : HttpRequest,
            tx_body : bool,
            req_upload : Optional[str] = None
            ) -> HttpResponse:
        logging.debug('RestHandler.create_blob_async')
        cfut = self.executor.submit(
            lambda: self._create_blob(
                request, tx_body, req_upload=req_upload), 0)
        if cfut is None:
            return self.response(request, code=500, msg='failed to schedule')
        fut = asyncio.wrap_future(cfut)
        await fut
        resp = fut.result()
        if resp is None or resp.status_code != 201:
           return fut.result()
        resp = await self._put_blob_async(request)
        if resp is not None and resp.status_code != 200:
            return resp

        blob_uri = make_blob_uri(
            self._tx_rest_id,
            blob=self._blob_rest_id if not tx_body else None,
            tx_body=tx_body)
        return self.response(request, code=201,
                             headers=[('location', blob_uri)])

    def put_blob(self, request : HttpRequest,
                 blob_rest_id : Optional[str] = None,
                 tx_body : bool = False) -> HttpResponse:
        logging.debug('RestHandler.put_blob %s', request.headers)
        err = self._put_blob(request, blob_rest_id, tx_body)
        if err:
            return err
        self.bytes_read = 0
        while b := request.stream.read(self.CHUNK_SIZE):
            logging.debug('RestHandler.put_blob chunk %d', len(b))
            resp = self._put_blob_chunk(request, b)
            if resp.status_code != 200:
                return resp
        return resp

    def _put_blob(self, request : HttpRequest,
                 blob_rest_id : Optional[str] = None,
                 tx_body : bool = False) -> HttpResponse:
        if blob_rest_id is not None:
            self._blob_rest_id = blob_rest_id
        range = None

        range_err, range = self._get_range(request)
        if range_err:
            return range_err
        self.range = range

        blob = self.async_filter.get_blob_writer(
            create = False,
            blob_rest_id=self._blob_rest_id, tx_body=tx_body)

        if blob is None:
            return self.response(request, code=404, msg='unknown blob')

        self.blob = blob

        return None

    async def put_blob_async(
            self, request : HttpRequest,
            tx_body : bool = False,
            blob_rest_id : Optional[str] = None) -> HttpResponse:
        cfut = self.executor.submit(
            lambda: self._put_blob(request, blob_rest_id, tx_body), 0)
        if cfut is None:
            return self.response(request, code=500, msg='failed to schedule')
        fut = asyncio.wrap_future(cfut)
        await fut
        if fut.result() is not None:
           return fut.result()
        return await self._put_blob_async(request)

    async def _put_blob_async(
            self, request : HttpRequest) -> HttpResponse:
        logging.debug('RestHandler._put_blob_async')
        b = bytes()
        self.bytes_read = 0
        async for chunk in request.stream():
            while chunk:
                to_go = self.CHUNK_SIZE - len(b)
                count = min(to_go, len(chunk))
                b += chunk[0:count]
                if len(b) < self.CHUNK_SIZE:
                    assert count == len(chunk)
                    break
                resp = await self._put_blob_chunk_async(request, b)
                if resp is not None:
                    return resp
                b = bytes()
                chunk = chunk[count:]
        if b:
            return await self._put_blob_chunk_async(request, b)

    async def _put_blob_chunk_async(
            self, request : HttpRequest, b : bytes
    ) -> Optional[HttpResponse]:
        cfut = self.executor.submit(
            lambda: self._put_blob_chunk(request, b), 0)
        if cfut is None:
            return self.response(request, code=500, msg='failed to schedule')
        fut = asyncio.wrap_future(cfut)
        await fut
        return fut.result()

    def _put_blob_chunk(self, request : HttpRequest, b : bytes) -> HttpResponse:
        logging.debug('RestHandler._put_blob_chunk %s content-range: %s %d',
                      self._blob_rest_id, self.range, len(b))

        content_length = result_len = None

        appended, result_len, content_length = self.blob.append_data(
            self.range.start + self.bytes_read, b, self.range.length)
        logging.debug(
            'RestHandler._put_blob_chunk %s %s %d %s',
            self._blob_rest_id, appended, result_len, content_length)

        headers=[('content-range',
                  ContentRange('bytes', 0, result_len, content_length))]

        if not appended:
            return self.response(
                request,
                code = 416,
                msg = 'invalid range',
                headers=headers)

        self.bytes_read += len(b)

        return self.response(request, resp_json={}, headers=headers)

    def cancel_tx(self, request : HttpRequest) -> HttpResponse:
        logging.debug('RestHandler.cancel_tx %s', self._tx_rest_id)
        tx = self.async_filter.get(0)
        if tx is None:
            return self.response(request)
        delta = TransactionMetadata(cancelled=True)
        assert tx.merge_from(delta) is not None
        assert tx.cancelled
        self.async_filter.update(tx, delta)
        return self.response(request)


class EndpointFactory(ABC):
    @abstractmethod
    def create(self, http_host : str) -> Optional[AsyncFilter]:
        pass

    @abstractmethod
    def get(self, rest_id : str) -> Optional[AsyncFilter]:
        pass

class RestHandlerFactory(HandlerFactory):
    executor : Executor
    endpoint_factory : EndpointFactory
    rest_id_factory : Callable[[], str]

    def __init__(self, executor,
                 endpoint_factory,
                 rest_id_factory : Callable[[], str]):
        self.executor = executor
        self.endpoint_factory = endpoint_factory
        self.rest_id_factory = rest_id_factory

    def create_tx(self, http_host) -> RestHandler:
        endpoint = self.endpoint_factory.create(http_host)
        return RestHandler(
            self.executor,
            async_filter=endpoint,
            http_host=http_host,
            rest_id_factory=self.rest_id_factory)

    def get_tx(self, tx_rest_id) -> RestHandler:
        return RestHandler(
            self.executor,
            async_filter=self.endpoint_factory.get(tx_rest_id),
            tx_rest_id=tx_rest_id,
            rest_id_factory=self.rest_id_factory)
