# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
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
from functools import partial

from urllib.parse import urljoin

from werkzeug.datastructures import ContentRange
import werkzeug.http

from fastapi import (
    Request as FastApiRequest,
    Response as FastApiResponse )
from fastapi.responses import (
    JSONResponse as FastApiJsonResponse,
    PlainTextResponse )

HttpRequest = FastApiRequest
HttpResponse = FastApiResponse

from httpx import Client, Response as HttpxResponse

from koukan.deadline import Deadline
from koukan.response import Response as MailResponse
from koukan.blob import Blob, InlineBlob, WritableBlob

from koukan.rest_service_handler import Handler, HandlerFactory
from koukan.filter import (
    AsyncFilter,
    SyncFilter,
    TransactionMetadata,
    WhichJson )
from koukan.executor import Executor

from koukan.rest_schema import BlobUri, make_blob_uri, make_tx_uri, parse_blob_uri
from koukan.version_cache import IdVersion
from koukan.storage_schema import VersionConflictException


# runs SyncFilter on Executor with AsyncFilter interface for
# RestHandler -> SyncFilterAdapter -> SmtpEndpoint

# TODO I'm unhappy with how complicated this has become. I have
# considered writing a memory-backed TransactionCursor and running
# SmtpEndpoint on OutputHandler instead. In the past I didn't think
# that would actually be simpler than this but now I'm not so sure...
class SyncFilterAdapter(AsyncFilter):
    class BlobWriter(WritableBlob):
        # all accessed with parent.mu
        parent : "SyncFilterAdapter"
        offset : int = 0
        # queue of staged appends, these are propagated to
        # parent.body in _update_once()
        q : List[bytes]
        content_length : Optional[int] = None
        def __init__(self, parent):
            self.parent = parent
            self.q = []

        def len(self):
            return self.offset

        def append_data(self, offset : int, d : bytes,
                        content_length : Optional[int] = None
                        ) -> Tuple[bool, int, Optional[int]]:
            with self.parent.mu:
                assert self.content_length is None or (
                    content_length == self.content_length)
                if self.content_length is not None and (
                        self.offset + len(d) > self.content_length):
                    return False, self.offset, self.content_length
                if offset != self.offset:
                    return False, self.offset, None
                self.q.append(d)
                self.offset += len(d)
                self.content_length = content_length
            self.parent._blob_wakeup()
            return True, self.offset, content_length

    executor : Executor
    filter : SyncFilter
    prev_tx : TransactionMetadata
    tx : TransactionMetadata
    mu : Lock
    cv : Condition
    inflight : bool = False
    rest_id : str
    _last_update : float
    blob_writer : Optional[BlobWriter] = None
    body : Optional[InlineBlob] = None
    # transaction has reached a final status: data response or cancelled
    # (used for gc)
    done : bool = False
    id_version : IdVersion

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
        self.id_version = IdVersion(db_id=1, rest_id='1', version=1)

    def incremental(self):
        return True

    def idle(self, now : float, ttl : float, done_ttl : float):
        with self.mu:
            t = done_ttl if self.done else ttl
            idle = now - self._last_update
            timedout = idle > t
            if self.inflight:
                if timedout:
                    logging.warning('maybe stuck %s idle %d',
                                    self.rest_id, idle)
                return False
            return timedout

    def version(self):
        return self.id_version.get()

    def wait(self, version, timeout) -> bool:
        return self.id_version.wait(
            version=version, timeout=timeout)

    async def wait_async(self, version, timeout) -> bool:
        return await self.id_version.wait_async(
            version=version, timeout=timeout)

    def _update(self):
        try:
            while self._update_once():
                pass
        except Exception:
            logging.exception('SyncFilterAdapter._update_once() %s',
                              self.rest_id)
            with self.mu:
                # The upstream SyncFilter is supposed to return tx
                # error responses and not throw
                # exceptions. i.e. SmtpEndpoint is supposed to convert
                # all smtplib/socket exceptions to error responses but there
                # may be bugs.
                self.tx.fill_inflight_responses(MailResponse(
                    450, 'internal error: unexpected exception in '
                    'SyncFilterAdapter'))
                self.cv.notify_all()
            raise
        finally:
            with self.mu:
                self.inflight = False
                self.cv.notify_all()

    def _update_once(self):
        with self.mu:
            assert self.inflight
            delta = self.prev_tx.delta(self.tx)  # new reqs
            assert delta is not None
            logging.debug('SyncFilterAdapter._update_once() '
                          'downstream_delta %s staged %s', delta,
                          len(self.blob_writer.q) if self.blob_writer else None)
            self.prev_tx = self.tx.copy()

            # propagate staged appends from blob_writer to body
            if self.blob_writer is not None and self.blob_writer.q:
                # this may be moot because we early-return
                # if the blob isn't finalized anyway but: we haven't
                # really spelled out whether body is only in the
                # delta the first time or every time that it grows?
                delta.body = self.body
                for b in self.blob_writer.q:
                    self.body.append_data(
                        self.body.len(), b,
                        self.blob_writer.content_length)
                    logging.debug('append %d %s', self.body.len(),
                                  self.body.content_length())
                self.blob_writer.q = []

            if not self.tx.req_inflight() and not delta.cancelled:
                return False
            tx = self.tx.copy()
        upstream_delta = self.filter.on_update(tx, delta)

        logging.debug('SyncFilterAdapter._update_once() '
                      'tx after upstream %s', tx)
        with self.mu:
            assert self.tx.merge_from(upstream_delta) is not None
            # TODO closer to req_inflight() logic i.e. tx has reached
            # a final state due to an error
            self.done = self.tx.cancelled or self.tx.data_response is not None
            version = self.id_version.get()
            version += 1
            self.cv.notify_all()
            self.id_version.update(version=version)
            self._last_update = time.monotonic()
        return True

    def update(self,
               tx : TransactionMetadata,
               tx_delta : TransactionMetadata
               ) -> Optional[TransactionMetadata]:
        if tx.retry is not None or tx.notification is not None:
            err = TransactionMetadata()
            tx.fill_inflight_responses(
                MailResponse(500, 'internal err/invalid transaction fields'),
                err)
            tx.merge_from(err)
            return err

        with self.mu:
            version = self.id_version.get()
            # xxx bootstrap
            if version > 1 and tx.version != version:
                raise VersionConflictException

            # try this non-destructively to see if the delta is valid...
            if self.tx.merge(tx_delta) is None:
                # bad delta, xxx this should throw an exception distinct
                # from VersionConflictException, cannot make forward progress
                return None
            # ... before committing to self.tx
            self.tx.merge_from(tx_delta)
            logging.debug('SyncFilterAdapter.updated merged %s', self.tx)

            version = self.id_version.get()
            version += 1
            self.id_version.update(version)

            if not self.inflight:
                fut = self.executor.submit(lambda: self._update(), 0)
                # TODO we need a better way to report this error but
                # throwing here will -> http 500
                assert fut is not None
                self.inflight = True
            # no longer waits for inflight
            delta = TransactionMetadata(rest_id=self.rest_id)
            delta.version = version
            tx.merge_from(delta)
            assert delta.version is not None
            return delta

    def get(self) -> Optional[TransactionMetadata]:
        with self.mu:
            tx = self.tx.copy()
            tx.version = self.id_version.get()
            return tx

    def get_blob_writer(self,
                        create : bool,
                        blob_rest_id : Optional[str] = None,
                        tx_body : Optional[bool] = None,
                        ) -> Optional[WritableBlob]:
        if not tx_body:
            raise NotImplementedError()
        if create and self.blob_writer is None:
            with self.mu:
                assert self.body is None
                assert self.blob_writer is None
                self.body = InlineBlob(b'')
                self.tx.body = self.body
                self.blob_writer = SyncFilterAdapter.BlobWriter(self)
        return self.blob_writer


    def _blob_wakeup(self):
        logging.debug('SyncFilterAdapter.blob_wakeup %s',
                      [len(b) for b in self.blob_writer.q])
        tx = self.get()
        assert tx is not None
        # shenanigans: empty update, _update_once() will dequeue from
        # BlobWriter.q to self.body
        tx_delta = TransactionMetadata()
        self.update(tx, tx_delta)


MAX_TIMEOUT=30


class RestHandler(Handler):
    chunk_size : int
    executor : Optional[Executor] = None
    async_filter : Optional[AsyncFilter]
    _tx_rest_id : Optional[str]

    _blob_rest_id : Optional[str] = None
    rest_id_factory : Optional[Callable[[], str]]
    http_host : Optional[str] = None

    # _put_blob
    range : Optional[ContentRange] = None
    blob : Optional[WritableBlob] = None
    bytes_read : Optional[int] = None
    endpoint_yaml : Optional[dict] = None
    session_uri : Optional[str] = None
    service_uri : Optional[str] = None
    HTTP_CLIENT = Callable[[str], HttpxResponse]
    client : HTTP_CLIENT

    def __init__(self,
                 executor : Optional[Executor] = None,
                 async_filter : Optional[AsyncFilter] = None,
                 tx_rest_id : Optional[str] = None,
                 blob_rest_id : Optional[str] = None,
                 rest_id_factory : Optional[Callable[[], str]] = None,
                 http_host : Optional[str] = None,
                 chunk_size : int = 1048576,
                 endpoint_yaml : Optional[dict] = None,
                 session_uri : Optional[str] = None,
                 service_uri : Optional[str] = None,
                 client : Optional[HTTP_CLIENT] = None):
        self.executor = executor
        self.async_filter = async_filter
        self._tx_rest_id = tx_rest_id
        self._blob_rest_id = blob_rest_id
        self.rest_id_factory = rest_id_factory
        self.http_host = http_host
        self.chunk_size = chunk_size
        if endpoint_yaml:
            self.endpoint_yaml = endpoint_yaml
        else:
            self.endpoint_yaml = {}
        self.session_uri = session_uri
        self.service_uri = service_uri
        if client is not None:
            self.client = client
        else:
            client = Client(follow_redirects=True)
            self.client = client.get

    def blob_rest_id(self):
        return self._blob_rest_id

    def response(self, req : HttpRequest,
                 code : int = 200,
                 msg : Optional[str] = None,
                 resp_json : Optional[dict] = None,
                 headers : Optional[List[Tuple[str,str]]] = None,
                 etag : Optional[str] = None
                 ) -> HttpResponse:
        headers_dict={}
        if headers:
            headers_dict.update({k:str(v) for k,v in headers})
        if etag is not None:
            headers_dict['etag'] = etag
        if resp_json is not None:
            return FastApiJsonResponse(
                status_code=code,
                content=resp_json,
                headers=headers_dict)
        return PlainTextResponse(
            status_code=code, content=msg, headers=headers_dict)

    def _ping_tx(self, session_uri, tx_rest_id):
        logging.debug('ping tx %s', self._tx_rest_id)

        # TODO HEAD makes more sense but this should work for now
        try:
            uri = urljoin(session_uri, make_tx_uri(tx_rest_id))
            resp = self.client(uri)
            logging.debug('%s %d', uri, resp.status_code)
        except:
            logging.exception('_ping_tx')


    def _maybe_schedule_ping(self, request, session_uri, tx_rest_id
                             ) -> Optional[HttpResponse]:
        if session_uri is None:
            return None

        if self.executor.submit(
                partial(self._ping_tx, session_uri,
                        self._tx_rest_id)) is None:
            return self.response(request, code=500, msg='schedule ping tx')
        return None

    async def handle_async(self, request : FastApiRequest, fn
                           ) -> FastApiResponse:
        logging.debug('RestHandler.handle_async req %s', request)
        timeout, err = self._get_timeout(request)
        if err is not None:
            return err
        deadline = Deadline(timeout)
        cfut = self.executor.submit(fn, 0)
        if cfut is None:
            return self.response(request, code=500, msg='failed to schedule')
        fut = asyncio.wrap_future(cfut)
        if not await asyncio.wait_for(fut, deadline.deadline_left()):
            cfut.cancel()
            return self.response(request, code=500, msg='timeout')
        resp = fut.result()
        logging.debug('RestHandler.handle_async resp %s', resp)
        return resp

    def _get_timeout(self, req : HttpRequest
                     ) -> Tuple[Optional[int], HttpResponse]:
        # https://datatracker.ietf.org/doc/id/draft-thomson-hybi-http-timeout-00.html
        if not (timeout_header := req.headers.get('request-timeout', None)):
            return MAX_TIMEOUT, None
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

        # no inflight waiting -> no timeout logic

        if self.async_filter is None:
            return self.response(
                request, code=500, msg='internal error creating transaction')
        tx = TransactionMetadata.from_json(
            req_json, WhichJson.REST_CREATE)
        if not self.async_filter.incremental():
            if tx.mail_from is None or len(tx.rcpt_to) != 1:
                return self.response(
                    request, code=400, msg='transaction creation to '
                    'non-incremental endpoint must contain mail_from and '
                    'exactly 1 rcpt_to')

        if tx is None:
            return self.response(request, code=400, msg='invalid tx json')
        tx.host = self.http_host
        body = tx.body

        upstream = self.async_filter.update(tx, tx.copy())
        if upstream is None or tx.rest_id is None:
            return self.response(request, code=400, msg='bad request')
        assert upstream.version is not None
        self._tx_rest_id = tx.rest_id

        tx.body = body
        # return uri qualified to session or service per self.endpoint_yaml
        tx_path = make_tx_uri(tx.rest_id)
        if self.endpoint_yaml.get('rest_lro', False) is False:
            tx_uri = urljoin(self.session_uri, tx_path)
        elif self.service_uri is not None:
            tx_uri = urljoin(self.service_uri, tx_path)
        else:
            tx_uri = tx_path
        resp = self.response(
            request, code=201,
            resp_json=tx.to_json(WhichJson.REST_READ),
            headers=[('location', tx_uri)],
            etag=self._etag(upstream.version))
        logging.debug('RestHandler._create %s', resp)
        return resp

    def _get_tx(self) -> Optional[TransactionMetadata]:
        tx = self.async_filter.get()
        if tx is None:
            return None
        logging.debug('_get_tx %s', tx)
        return tx

    def _get_tx_resp(self, request, tx):
        tx_out = tx.copy()
        return self.response(
            request,
            etag=self._etag(tx.version) if tx else None,
            resp_json=tx_out.to_json(WhichJson.REST_READ))

    def _check_etag(self, etag, cached_version) -> bool:
        etag = etag.strip('"')
        logging.debug(
            'RestHandler._check_etag %s etag %s cached_version %s',
            self._tx_rest_id, etag, cached_version)
        return self._etag(cached_version) == etag

    async def _get_tx_async(
            self, request
    ) -> Tuple[Optional[HttpResponse], Optional[TransactionMetadata]]:
        cfut = self.executor.submit(lambda: self._get_tx())
        if cfut is None:
            return self.response(
                request, code=500, msg='get tx async schedule read'), None
        afut = asyncio.wrap_future(cfut)
        try:
            # wait ~forever here, this is a point read
            # xxx fixed timeout? ignore deadline?
            await asyncio.wait_for(afut, None)
        except TimeoutError:
            # unexpected
            return self.response(
                request, code=500, msg='get tx async read'), None
        if not afut.done():
            return self.response(request, code=500,
                                 msg='get tx async read fut done'), None
        if afut.result() is None:
            return self.response(request, code=404, msg='unknown tx'), None
        return None, afut.result()

    async def get_tx_async(self, request : HttpRequest) -> HttpResponse:
        if self.async_filter is None:
            return self.response(
                request, code=404, msg='transaction not found')

        timeout, err = self._get_timeout(request)
        if err is not None:
            return err

        etag = request.headers.get('if-none-match', None)
        err, tx = await self._get_tx_async(request)

        if err is not None:
            return err
        if (timeout is None or etag is None or
            not self._check_etag(etag, tx.version)):
            return self._get_tx_resp(request, tx)
        elif (timeout is not None and etag is not None and
              tx.session_uri is not None):
            return self.response(
                request, code=307, headers=[
                    ('location', urljoin(tx.session_uri,
                                         make_tx_uri(self._tx_rest_id)))])

        deadline = Deadline(timeout)

        wait_result = await self.async_filter.wait_async(
            tx.version, deadline.deadline_left())
        if not wait_result:
            return self.response(request, code=304, msg='unchanged',
                                 headers=[('etag', self._etag(tx.version))])
        err, tx = await self._get_tx_async(request)
        if err is not None:
            return err
        return self._get_tx_resp(request, tx)

    def patch_tx(self, request : HttpRequest, req_json : dict) -> HttpResponse:
        logging.debug('RestHandler.patch_tx %s %s',
                      self._tx_rest_id, req_json)
        downstream_delta = TransactionMetadata.from_json(
            req_json, WhichJson.REST_UPDATE)
        if downstream_delta is None:
            return self.response(request, code=400, msg='invalid request')
        body = downstream_delta.body
        req_etag = request.headers.get('if-match', None)
        if req_etag is None:
            return self.response(
                request, code=400, msg='etags required for update')
        req_etag = req_etag.strip('"')

        tx = self.async_filter.get()
        if tx is None:
            return self.response(
                request,
                code=500,
                msg='RestHandler.patch_tx timeout reading tx')
        if not self.async_filter.incremental():
            return self.response(
                request, code=400,
                msg='endpoint does not accept incremental updates')

        if tx.merge_from(downstream_delta) is None:
            return self.response(
                request, code=400,
                msg='RestHandler.patch_tx merge failed')

        # TODO should these 412s set the etag?
        if req_etag != self._etag(tx.version):
            logging.debug('RestHandler.patch_tx conflict %s %s',
                          req_etag, self._etag(tx.version))
            return self.response(request, code=412, msg='update conflict')
        try:
            upstream_delta = self.async_filter.update(tx, downstream_delta)
        except VersionConflictException:
            return self.response(request, code=412, msg='update conflict')
        if upstream_delta is None:
            return self.response(
                request, code=400,
                msg='RestHandler.patch_tx bad request')

        if (err := self._maybe_schedule_ping(
                request, tx.session_uri, self._tx_rest_id)):
            return err

        tx.body = body
        return self.response(
            request, etag=self._etag(upstream_delta.version),
            resp_json=tx.to_json(WhichJson.REST_READ))


    def _get_range(self, request : HttpRequest
                   ) -> Tuple[Optional[HttpResponse], Optional[ContentRange]]:
        if ('content-length' not in request.headers or
            'content-range' not in request.headers):
            return None, None
        content_length = int(request.headers.get('content-length'))
        range = werkzeug.http.parse_content_range_header(
            request.headers.get('content-range'))
        logging.info('put_blob content-range: %s', range)
        if not range or range.units != 'bytes':
            return self.response(request, 400, 'bad range'), None
        # no idea if underlying stack enforces this
        assert(range.stop - range.start == content_length)
        return None, range

    def _create_body(self, request : HttpRequest,
                     req_upload : Optional[str] = None) -> HttpResponse:
        logging.debug('RestHandler._create_body %s %s blob %s tx %s',
                      request, request.headers, self._blob_rest_id,
                      self._tx_rest_id)

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

        logging.debug('RestHandler._create_blob before create')

        if (blob := self.async_filter.get_blob_writer(
                create=True, tx_body=True)) is None:
            return self.response(
                request, code=500, msg='internal error creating blob')
        self.blob = blob
        if not chunked:
            range_err, range = self._get_range(request)
            if range_err:
                return range_err
            self.range = range

        blob_uri = make_blob_uri(self._tx_rest_id, tx_body=True)
        return self.response(request, code=201,
                             headers=[('location', blob_uri)])

    async def create_body_async(
            self, request : FastApiRequest, req_upload : Optional[str] = None
            ) -> FastApiResponse:
        logging.debug('RestHandler.create_blob_async')
        cfut = self.executor.submit(
            partial(self._create_body, request, req_upload=req_upload), 0)
        if cfut is None:
            return self.response(request, code=500, msg='failed to schedule')
        fut = asyncio.wrap_future(cfut)
        await fut
        resp = fut.result()
        if resp is None or resp.status_code != 201:
           return fut.result()
        if req_upload is None:
            resp = await self._put_blob_async(request)
            if resp is not None and resp.status_code != 200:
                return resp

        blob_uri = make_blob_uri(self._tx_rest_id, tx_body=True)
        return self.response(request, code=201,
                             headers=[('location', blob_uri)])

    # populate self.blob or return http err
    def _get_blob_writer(self, request : HttpRequest,
                         blob_rest_id : Optional[str] = None,
                         tx_body : bool = False) -> Optional[HttpResponse]:
        if blob_rest_id is not None:
            self._blob_rest_id = blob_rest_id
        range = None

        range_err, range = self._get_range(request)
        if range_err:
            return range_err
        self.range = range

        blob = self.async_filter.get_blob_writer(
            create = False, blob_rest_id=self._blob_rest_id, tx_body=tx_body)

        if blob is None:
            return self.response(request, code=404, msg='unknown blob')

        self.blob = blob

        return None

    async def put_blob_async(
            self, request : FastApiRequest,
            tx_body : bool = False,
            blob_rest_id : Optional[str] = None) -> FastApiResponse:
        cfut = self.executor.submit(
            lambda: self._get_blob_writer(request, blob_rest_id, tx_body), 0)
        if cfut is None:
            return self.response(request, code=500, msg='failed to schedule')
        fut = asyncio.wrap_future(cfut)
        await fut
        if fut.result() is not None:
           return fut.result()
        return await self._put_blob_async(request)

    async def _put_blob_async(
            self, request : FastApiRequest) -> FastApiResponse:
        logging.debug('RestHandler._put_blob_async')
        b = bytes()
        self.bytes_read = 0
        async for chunk in request.stream():
            # copy from chunk to b until it contains chunk_size, then
            # send upstream
            while chunk:
                to_go = self.chunk_size - len(b)
                count = min(to_go, len(chunk))
                b += chunk[0:count]
                if len(b) < self.chunk_size:
                    assert count == len(chunk)
                    break
                resp = await self._put_blob_chunk_async(request, b)
                if resp is not None and resp.status_code != 200:
                    logging.debug(resp)
                    return resp
                b = bytes()
                chunk = chunk[count:]
        # send any leftover
        resp = await self._put_blob_chunk_async(request, b, last=True)
        if resp.status_code != 200:
            return resp

        if (err := self._maybe_schedule_ping(
                request, self.blob.session_uri(), self._tx_rest_id)):
            return err

        return resp

    async def _put_blob_chunk_async(
            self, request : FastApiRequest, b : bytes, last=False
    ) -> Optional[FastApiResponse]:
        cfut = self.executor.submit(
            lambda: self._put_blob_chunk(request, b, last), 0)
        if cfut is None:
            return self.response(request, code=500, msg='failed to schedule')
        fut = asyncio.wrap_future(cfut)
        await fut
        return fut.result()

    def _put_blob_chunk(self, request : HttpRequest, b : bytes,
                        last=False) -> HttpResponse:
        logging.debug('RestHandler._put_blob_chunk %s content-range: %s %d',
                      self._blob_rest_id, self.range, len(b))

        content_length = result_len = None

        start = 0
        if self.range is not None:
            start = self.range.start
            length = self.range.length
        elif last:
            length = self.bytes_read + len(b)
        else:
            length = None

        appended, result_len, content_length = self.blob.append_data(
            start + self.bytes_read, b, length)
        logging.debug(
            'RestHandler._put_blob_chunk %s %s %d %s',
            self._blob_rest_id, appended, result_len, content_length)

        headers = []
        if self.range is not None:
            headers.append(
                ('content-range',
                 ContentRange('bytes', 0, result_len, content_length)))

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
        tx = self.async_filter.get()
        if tx is None:
            return self.response(request)
        delta = TransactionMetadata(cancelled=True)
        assert tx.merge_from(delta) is not None
        assert tx.cancelled
        self.async_filter.update(tx, delta)
        # TODO this should probably return the tx?
        return self.response(request)


class EndpointFactory(ABC):
    # dict : endpoint yaml
    @abstractmethod
    def create(self, http_host : str) -> Optional[Tuple[AsyncFilter, dict]]:
        pass

    @abstractmethod
    def get(self, rest_id : str) -> Optional[AsyncFilter]:
        pass

class RestHandlerFactory(HandlerFactory):
    executor : Executor
    endpoint_factory : EndpointFactory
    session_uri : Optional[str] = None
    service_uri : Optional[str] = None
    rest_id_factory : Callable[[], str]

    def __init__(self, executor,
                 endpoint_factory,
                 rest_id_factory : Callable[[], str],
                 session_uri : Optional[str] = None,
                 service_uri : Optional[str] = None):
        self.executor = executor
        self.endpoint_factory = endpoint_factory
        self.rest_id_factory = rest_id_factory
        self.session_uri = session_uri
        self.service_uri = service_uri

    def create_tx(self, http_host) -> RestHandler:
        endpoint, yaml = self.endpoint_factory.create(http_host)
        return RestHandler(
            executor=self.executor,
            async_filter=endpoint,
            http_host=http_host,
            rest_id_factory=self.rest_id_factory,
            endpoint_yaml = yaml,
            session_uri = self.session_uri,
            service_uri = self.service_uri)

    def get_tx(self, tx_rest_id) -> RestHandler:
        filter = self.endpoint_factory.get(tx_rest_id)
        return RestHandler(
            executor=self.executor,
            async_filter=filter,
            tx_rest_id=tx_rest_id,
            rest_id_factory=self.rest_id_factory,
            session_uri = self.session_uri,
            service_uri = self.service_uri)
