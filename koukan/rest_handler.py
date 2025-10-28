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
    TransactionMetadata,
    WhichJson )
from koukan.executor import Executor

from koukan.rest_schema import (
    FINALIZE_BLOB_HEADER,
    BlobUri,
    make_blob_uri,
    make_tx_uri,
    parse_blob_uri )
from koukan.version_cache import IdVersion
from koukan.storage_schema import BlobSpec, VersionConflictException
from koukan.message_builder import MessageBuilderSpec


MAX_TIMEOUT=30


class RestHandler(Handler):
    chunk_size : int
    executor : Executor
    async_filter : Optional[AsyncFilter]
    _tx_rest_id : Optional[str]

    _blob_rest_id : Optional[str] = None
    rest_id_factory : Optional[Callable[[], str]]

    # _put_blob
    range : Optional[ContentRange] = None
    blob : Optional[WritableBlob] = None
    bytes_read : Optional[int] = None
    final_blob_length : Optional[int] = None

    endpoint_yaml : dict
    session_url : Optional[str] = None
    service_url : Optional[str] = None
    HTTP_CLIENT = Callable[[str], HttpxResponse]
    client : HTTP_CLIENT
    sender : Optional[str] = None

    def __init__(self,
                 executor : Executor,
                 async_filter : Optional[AsyncFilter] = None,
                 tx_rest_id : Optional[str] = None,
                 blob_rest_id : Optional[str] = None,
                 rest_id_factory : Optional[Callable[[], str]] = None,
                 chunk_size : int = 2**20,
                 endpoint_yaml : Optional[dict] = None,
                 session_url : Optional[str] = None,
                 service_url : Optional[str] = None,
                 client : Optional[HTTP_CLIENT] = None,
                 sender : Optional[str] = None):
        assert service_url is not None
        self.executor = executor
        self.async_filter = async_filter
        self._tx_rest_id = tx_rest_id
        self._blob_rest_id = blob_rest_id
        self.rest_id_factory = rest_id_factory
        self.chunk_size = chunk_size
        if endpoint_yaml:
            self.endpoint_yaml = endpoint_yaml
        else:
            self.endpoint_yaml = {}
        self.session_url = session_url
        self.service_url = service_url
        if client is not None:
            self.client = client
        self.sender = sender

    def blob_rest_id(self):
        return self._blob_rest_id

    def response(self,
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

    def _handle_async(self, request, fn):
        try:
            return fn()
        except Exception:
            logging.exception('_handle_async')
            return None

    async def handle_async(self, request : FastApiRequest, fn
                           ) -> FastApiResponse:
        logging.debug('RestHandler.handle_async req %s', request)
        timeout, err = self._get_timeout(request)
        if err is not None:
            return err
        deadline = Deadline(timeout)
        cfut = self.executor.submit(partial(self._handle_async, request, fn), 0)
        if cfut is None:
            return self.response(code=500, msg='failed to schedule')
        fut = asyncio.wrap_future(cfut)
        if not await asyncio.wait_for(fut, deadline.deadline_left()):
            cfut.cancel()
            return self.response(code=500, msg='timeout')
        resp = fut.result()
        logging.debug('RestHandler.handle_async resp %s', resp)
        return resp

    def _get_timeout(self, req : HttpRequest
                     ) -> Tuple[Optional[int], Optional[HttpResponse]]:
        # https://datatracker.ietf.org/doc/id/draft-thomson-hybi-http-timeout-00.html
        if 'if-none-match' not in req.headers:
            return None, None

        if not (timeout_header := req.headers.get('request-timeout', None)):
            return MAX_TIMEOUT, None
        timeout = None
        try:
            timeout = min(int(timeout_header), MAX_TIMEOUT)
        except ValueError:
            return None, self.response(
                code=400, msg='invalid request-timeout header')
        return timeout, None

    def _etag(self, version : int) -> str:
        return '%d' % version

    def _validate_incremental_tx(self, tx : TransactionMetadata
                                 ) -> Optional[HttpResponse]:
        if tx.retry is not None or tx.notification is not None:
            return self.response(
                code=400, msg='incremental endpoint does not '
                'accept retry/notification')
        return None

    def create_tx(self, request : HttpRequest, req_json : dict) -> HttpResponse:
        # TODO if request doesn't have remote_addr or is not from a
        # well-known/trusted peer (i.e. smtp gateway), set remote_addr to wsgi
        # environ REMOTE_ADDR or HTTP_X_FORWARDED_FOR
        # TODO only accept smtp_meta from trusted peer i.e. the
        # well-known address of the gateway
        logging.debug('RestHandler.create_tx %s %s', request, req_json)

        # no inflight waiting -> no timeout logic

        if self.async_filter is None:
            return self.response(
                code=500, msg='internal error creating transaction')
        tx = TransactionMetadata.from_json(
            req_json, WhichJson.REST_CREATE)
        if tx is None:
            return self.response(code=400, msg='invalid tx json')
        tx.sender = self.sender

        if not self.async_filter.incremental():
            if tx.mail_from is None or len(tx.rcpt_to) != 1:
                return self.response(
                    code=400, msg='transaction creation to '
                    'non-incremental endpoint must contain mail_from and '
                    'exactly 1 rcpt_to')
            # for smtp gw/exploder, we don't do this until they start
            # sending the data because
            # 'body is not None'
            # is used as a proxy for 'done with rcpts'
            if tx.body is None:
                tx.body = BlobSpec(create_tx_body=True)
        elif err := self._validate_incremental_tx(tx):
            return err

        upstream = self.async_filter.update(tx, tx.copy())
        cached = self.async_filter.check_cache()
        # the factory path up to router_service fails if the OH
        # couldn't be scheduled so if we got here, it should be leased
        assert cached is not None
        version, cached_tx, local, remote = cached
        assert cached_tx is not None
        if upstream is None or tx.rest_id is None:
            return self.response(code=400, msg='bad request')
        version = self.async_filter.version
        assert version is not None
        self._tx_rest_id = tx.rest_id

        # new tx url always initially scoped to creator session
        tx_path = make_tx_uri(tx.rest_id)
        tx_uri = urljoin(self.session_url, tx_path)
        self._update_body_blob_uri(cached_tx)
        resp = self.response(
            code=201,
            resp_json=cached_tx.to_json(WhichJson.REST_READ),
            headers=[('location', tx_uri)],
            etag=self._etag(version))
        logging.debug('RestHandler._create %s', resp)
        return resp

    def _update_blob_uri(self, blob):
        reuse_uri = blob.blob_uri().copy()
        if blob.finalized():
            reuse_uri.base_uri = self.service_url
        else:
            reuse_uri.base_uri = self.session_url
        out = BlobSpec(reuse_uri = reuse_uri,
                       finalized = blob.finalized())
        return out

    def _update_body_blob_uri(self, tx):
        # we should have redirected by now if it's leased somewhere else
        assert tx.session_uri is None
        if tx.body is None:
            # we don't actually create it for interactive/exploder
            # here (cf above) but need to return the uri in the json
            tx.body = BlobSpec(create_tx_body=True)
            tx.body.reuse_uri = BlobUri(
                self._tx_rest_id, tx_body=True, base_uri=self.service_url)
            logging.debug(tx.body.reuse_uri)
        elif isinstance(tx.body, Blob):
            tx.body = self._update_blob_uri(tx.body)
        elif isinstance(tx.body, MessageBuilderSpec):
            tx.body.blobs = {
                blob_id: self._update_blob_uri(blob)
                for blob_id,blob in tx.body.blobs.items()
            }
            # whereas rest receivers get the original message in
            # addition to the parsed, this is only used for
            # router <-> gw where it's either one or the other but
            # never both so this doesn't need to populate body_blob
            # uri here

    def _get_tx(self) -> Optional[TransactionMetadata]:
        try:
            logging.debug('_get_tx')
            assert self.async_filter is not None
            tx = self.async_filter.get()
            if tx is None:
                return None
            logging.debug('_get_tx %s', tx)
            return tx
        except Exception:
            logging.exception('_get_tx')
            return None

    def _get_tx_resp(self, request, tx, version):
        tx_out = tx.copy()
        self._update_body_blob_uri(tx_out)
        json_out = tx_out.to_json(WhichJson.REST_READ)
        return self.response(
            etag=self._etag(version) if tx else None,
            resp_json=json_out)

    def _check_etag(self, etag, cached_version) -> bool:
        etag = etag.strip('"')
        logging.debug(
            'RestHandler._check_etag %s etag %s cached_version %s',
            self._tx_rest_id, etag, cached_version)
        return self._etag(cached_version) == etag

    # -> version, leased here?, other session
    def _check_tx(self) -> Optional[AsyncFilter.CheckTxResult]:
        assert self.async_filter is not None
        return self.async_filter.check()

    async def _check_tx_async(
            self
    ) -> Tuple[Optional[FastApiResponse], Optional[AsyncFilter.CheckTxResult]]:
        logging.debug('_check_tx_async')
        cfut = self.executor.submit(self._check_tx)
        if cfut is None:
            return self.response(
                code=500, msg='_check_tx_async schedule read'), None
        afut = asyncio.wrap_future(cfut)
        try:
            # wait ~forever here, this is a point read
            # xxx fixed timeout? ignore deadline?
            await asyncio.wait_for(afut, None)
        except TimeoutError:
            # unexpected
            return self.response(
                code=500, msg='get tx async read'), None
        if not afut.done():
            return self.response(
                code=500, msg='get tx async read fut done'), None
        if afut.result() is None:
            return self.response(code=404, msg='unknown tx'), None
        logging.debug('_check_tx_async done')
        return None, afut.result()

    async def _get_tx_async(
            self
    ) -> Tuple[Optional[HttpResponse], Optional[TransactionMetadata]]:
        logging.debug('_get_tx_async')
        cfut = self.executor.submit(self._get_tx)
        if cfut is None:
            return self.response(
                code=500, msg='get tx async schedule read'), None
        afut = asyncio.wrap_future(cfut)
        try:
            # wait ~forever here, this is a point read
            # xxx fixed timeout? ignore deadline?
            await asyncio.wait_for(afut, None)
        except TimeoutError:
            # unexpected
            return self.response(
                code=500, msg='get tx async read'), None
        if not afut.done():
            return self.response(code=500,
                                 msg='get tx async read fut done'), None
        if afut.result() is None:
            return self.response(code=404, msg='unknown tx'), None
        logging.debug('_get_tx_async done')
        return None, afut.result()

    async def get_tx_async(self, request : HttpRequest) -> HttpResponse:
        assert self.async_filter is not None

        timeout, err = self._get_timeout(request)
        if err is not None:
            return err

        etag = request.headers.get('if-none-match', None)
        check_result = self.async_filter.check_cache()
        if check_result is None:
            err, check_result = await self._check_tx_async()
            if err:
                return err
            if check_result is None:
                return self.response(code=500, msg='check tx failed')
        version, tx, is_local, other = check_result
        if other is not None:
            return self.response(
                code=308, headers=[
                    ('location', urljoin(other, request.url.path))])

        fresh_etag = etag is not None and self._check_etag(etag, version)
        if timeout is None or not is_local or not fresh_etag:
            if fresh_etag:
                return self.response(code=304, msg='unchanged',
                                     headers=[('etag', self._etag(version))])
            # do a full read every time with no etag
            if tx is None or etag is None:
                err, tx = await self._get_tx_async()
                if err is not None:
                    return err
            return self._get_tx_resp(request, tx, version)

        assert is_local
        assert timeout is not None
        assert etag is not None
        assert fresh_etag

        deadline = Deadline(timeout)

        wait_result, tx = await self.async_filter.wait_async(
            version, deadline.deadline_left())

        if not wait_result:
            return self.response(code=304, msg='unchanged',
                                 headers=[('etag', etag)])

        if tx is None:
            err, tx = await self._get_tx_async()
            if err is not None:
                return err
        resp = self._get_tx_resp(request, tx, self.async_filter.version)
        assert resp.headers['etag'] != etag
        logging.debug('get_tx_async done')
        return resp

    def patch_tx(self, request : HttpRequest, req_json : Optional[dict]
                 ) -> HttpResponse:
        if self.async_filter is None:
            return self.response(code=404, msg='transaction not found')

        logging.debug('RestHandler.patch_tx %s %s',
                      self._tx_rest_id, req_json)
        if req_json is not None:
            downstream_delta = TransactionMetadata.from_json(
                req_json, WhichJson.REST_UPDATE)
        else:
             downstream_delta = TransactionMetadata()
        if downstream_delta is None:
            return self.response(code=400, msg='invalid request')
        body = downstream_delta.body
        req_etag = request.headers.get('if-match', None)
        if req_etag is None:
            return self.response(code=400, msg='etags required for update')
        req_etag = req_etag.strip('"')

        tx = self.async_filter.get()
        if tx is None:
            return self.response(
                code=500,
                msg='RestHandler.patch_tx timeout reading tx')
        if tx.session_uri:
            return self.response(
                code=308, headers=[
                    ('location', urljoin(tx.session_uri, request.url.path))])

        if req_json is None:  # heartbeat/ping
            pass
        elif not self.async_filter.incremental():
            return self.response(
                code=400,
                msg='endpoint does not accept incremental updates')
        elif err := self._validate_incremental_tx(tx):
            return err

        if tx.merge_from(downstream_delta) is None:
            return self.response(
                code=400, msg='RestHandler.patch_tx merge failed')

        # TODO should these 412s set the etag?
        version = self.async_filter.version
        assert version is not None
        if req_etag != self._etag(version):
            logging.debug('RestHandler.patch_tx conflict %s %s',
                          req_etag, self._etag(version))
            return self.response(code=412, msg='update conflict')
        try:
            upstream_delta = self.async_filter.update(tx, downstream_delta)
        except VersionConflictException:
            return self.response(code=412, msg='update conflict')
        if upstream_delta is None:
            return self.response(
                code=400, msg='RestHandler.patch_tx bad request')
        # checked this earlier, should have gotten a version conflict
        # if the tx changed
        assert tx.session_uri is None

        tx.body = body
        self._update_body_blob_uri(tx)
        version = self.async_filter.version
        assert version is not None
        return self.response(
            etag=self._etag(version),
            resp_json=tx.to_json(WhichJson.REST_READ))


    def _get_range(self, request : HttpRequest
                   ) -> Tuple[Optional[HttpResponse], Optional[ContentRange]]:
        if ((cl := request.headers.get('content-length', None)) is None or
            'content-range' not in request.headers):
            return None, None
        content_length = int(cl)
        range = werkzeug.http.parse_content_range_header(
            request.headers.get('content-range'))
        logging.info('put_blob content-range: %s', range)
        if (not range or range.units != 'bytes' or range.stop is None or
            range.start is None):
            return self.response(400, 'bad range'), None
        # no idea if underlying stack enforces this
        assert(range.stop - range.start == content_length)
        return None, range

    # populate self.blob or return http err
    def _get_blob_writer(self, request : HttpRequest,
                         blob_rest_id : Optional[str] = None,
                         tx_body : bool = False
                         ) -> Optional[HttpResponse]:
        assert self.async_filter is not None

        cached = self.async_filter.check_cache()
        if cached is None:
            tx = self.async_filter.get()
        else:
            tx = cached[1]
        if tx is None:
            return self.response(code=404, msg='unknown transaction')

        if tx.session_uri is not None:
            return self.response(
                code=308, headers=[
                    ('location',
                     urljoin(tx.session_uri, request.url.path))])

        if blob_rest_id is not None:
            self._blob_rest_id = blob_rest_id
        range = None

        range_err, range = self._get_range(request)
        if range_err:
            return range_err
        self.range = range
        # cf create_tx() blobs are created with in the initial post
        # for non-incremental/exploder but for incremental/exploder
        # not until the client actually starts sending the body
        create = tx_body and (range is None or range.start == 0)

        # this just returns the blob writer now if it already exists,
        # append will fail precondition/offset check downstream -> 416
        blob = self.async_filter.get_blob_writer(
            create = create, blob_rest_id=self._blob_rest_id, tx_body=tx_body)

        if blob is None:
            return self.response(code=404, msg='unknown blob')

        self.blob = blob

        return None

    async def put_blob_async(
            self, request : FastApiRequest,
            blob_rest_id : Optional[str] = None,
            tx_body : bool = False) -> FastApiResponse:
        if self.async_filter is None:
            return self.response(code=404, msg='transaction not found')

        cfut = self.executor.submit(
            partial(self._get_blob_writer, request, blob_rest_id, tx_body), 0)
        if cfut is None:
            return self.response(code=500, msg='failed to schedule')
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
        finalize_blob_header = request.headers.get(FINALIZE_BLOB_HEADER, None)
        if finalize_blob_header:
            finalize_blob_header.strip()
            try:
                final_blob_length = int(finalize_blob_header)
            except:
                return self.response(
                    code=400, msg='invalid ' + FINALIZE_BLOB_HEADER)
            self.final_blob_length = final_blob_length

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
        assert resp is not None
        if resp.status_code != 200:
            return resp
        assert self.blob is not None

        return resp

    async def _put_blob_chunk_async(
            self, request : FastApiRequest, b : bytes, last=False
    ) -> Optional[FastApiResponse]:
        cfut = self.executor.submit(
            lambda: self._put_blob_chunk(request, b, last), 0)
        if cfut is None:
            return self.response(code=500, msg='failed to schedule')
        fut = asyncio.wrap_future(cfut)
        await fut
        return fut.result()

    def _put_blob_chunk(self, request : HttpRequest, b : bytes,
                        last=False) -> HttpResponse:
        assert self.bytes_read is not None
        logging.debug('RestHandler._put_blob_chunk %s content-range: %s %d',
                      self._blob_rest_id, self.range, len(b))

        content_length = result_len = None

        start = 0
        length : Optional[int]
        if self.final_blob_length is not None:
            start = self.final_blob_length
            length = self.final_blob_length
            if len(b):
                return self.response(
                    code=400,
                    msg=FINALIZE_BLOB_HEADER + ' with non-empty PUT',)
        elif self.range is not None:
            assert self.range.start is not None
            start = self.range.start
            length = self.range.length
        elif last:
            length = self.bytes_read + len(b)
        else:
            length = None

        assert self.blob is not None
        appended, result_len, content_length = self.blob.append_data(
            start + self.bytes_read, b, length)
        logging.debug(
            'RestHandler._put_blob_chunk %s %s %d %s',
            self._blob_rest_id, appended, result_len, content_length)
        if not appended and (uri := self.blob.session_uri()) is not None:
            return self.response(
                code=308,
                headers=[('location', urljoin(uri, request.url.path))])


        headers : List[Tuple[str, Any]] = []
        headers.append(
            ('content-range',
             ContentRange('bytes', 0, result_len, content_length)))

        if not appended:
            return self.response(
                code = 416, msg = 'invalid range', headers=headers)

        self.bytes_read += len(b)

        return self.response(resp_json={}, headers=headers)

    def cancel_tx(self, request : HttpRequest) -> HttpResponse:
        if self.async_filter is None:
            return self.response(code=404, msg='transaction not found')

        logging.debug('RestHandler.cancel_tx %s', self._tx_rest_id)
        tx = self.async_filter.get()
        if tx is None:
            return self.response()
        delta = TransactionMetadata(cancelled=True)
        tx.merge_from(delta)
        assert tx.cancelled
        self.async_filter.update(tx, delta)
        # TODO this should probably return the tx?
        return self.response()


class EndpointFactory(ABC):
    # dict : endpoint yaml
    @abstractmethod
    def create(self, sender : str, tag : Optional[str]
               ) -> Optional[Tuple[AsyncFilter, dict]]:
        pass

    @abstractmethod
    def get(self, rest_id : str) -> Optional[AsyncFilter]:
        pass

class RestHandlerFactory(HandlerFactory):
    executor : Executor
    endpoint_factory : EndpointFactory
    session_url : Optional[str] = None
    service_url : Optional[str] = None
    rest_id_factory : Callable[[], str]
    chunk_size : Optional[int] = None
    client : Client

    def __init__(self, executor,
                 endpoint_factory,
                 rest_id_factory : Callable[[], str],
                 session_url : Optional[str] = None,
                 service_url : Optional[str] = None,
                 chunk_size : Optional[int] = None):
        self.executor = executor
        self.endpoint_factory = endpoint_factory
        self.rest_id_factory = rest_id_factory
        self.session_url = session_url
        self.service_url = service_url
        self.chunk_size = chunk_size
        self.client = Client(follow_redirects=True)

    def create_tx(self, sender, tag) -> RestHandler:
        res = self.endpoint_factory.create(sender, tag)
        # TODO possibly HandlerFactory should be able to return an
        # error response directly here?
        assert res is not None
        endpoint, yaml = res
        kwargs : Dict[str, Any] = {}
        if self.chunk_size:
            kwargs['chunk_size'] = self.chunk_size
        return RestHandler(
            executor=self.executor,
            async_filter=endpoint,
            rest_id_factory=self.rest_id_factory,
            endpoint_yaml = yaml,
            session_url = self.session_url,
            service_url = self.service_url,
            client = self.client.get,
            sender = sender,
            **kwargs)

    def get_tx(self, tx_rest_id) -> RestHandler:
        filter = self.endpoint_factory.get(tx_rest_id)
        kwargs : Dict[str, Any] = {}
        if self.chunk_size:
            kwargs['chunk_size'] = self.chunk_size
        return RestHandler(
            executor=self.executor,
            async_filter=filter,
            tx_rest_id=tx_rest_id,
            rest_id_factory=self.rest_id_factory,
            session_url = self.session_url,
            service_url = self.service_url,
            client = self.client.get,
            **kwargs)
