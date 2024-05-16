from typing import Any, Callable, Dict, List, Optional
from abc import ABC, abstractmethod
import logging
import time
from threading import Condition, Lock
import json

from flask import Response as FlaskResponse, Request as FlaskRequest, jsonify
from werkzeug.datastructures import ContentRange
import werkzeug.http

from response import Response as MailResponse
from blob import Blob, InlineBlob
from blobs import BlobStorage
from rest_service_handler import Handler, HandlerFactory
from filter import AsyncFilter, Filter, TransactionMetadata, WhichJson
from executor import Executor

# Wrapper for vanilla/sync Filter that provides async interface for REST
class SyncFilterAdapter(AsyncFilter):
    executor : Executor
    filter : Filter
    prev_tx : TransactionMetadata
    tx : TransactionMetadata
    mu : Lock
    cv : Condition
    inflight : bool = False
    rest_id : str
    _last_update : float
    _version = 0

    def __init__(self, executor : Executor, filter : Filter, rest_id : str):
        self.executor = executor
        self.mu = Lock()
        self.cv = Condition(self.mu)
        self.filter = filter
        self.rest_id = rest_id
        self._last_update = time.monotonic()
        self.prev_tx = TransactionMetadata()
        self.tx = TransactionMetadata()

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
            logging.debug('SyncFilterAdapter._update prev %s tx %s delta %s',
                          self.prev_tx, self.tx, delta)
            self.prev_tx = self.tx
            if not delta.req_inflight():
                self.inflight = False
                return False
        reqs = delta.copy()
        self.filter.on_update(delta)
        resps = reqs.delta(delta)  # responses only
        logging.debug('SyncFilterAdapter._update done pre %s out %s post %s',
                      reqs, delta, resps)
        with self.mu:
            txx = self.tx.merge(resps)
            assert txx is not None  # internal error
            logging.debug('SyncFilterAdapter._update done merged %s', txx)
            self.tx = txx
            self._version += 1
            self.cv.notify_all()
            self._last_update = time.monotonic()
        return True

    def _tx_done_locked(self, tx : TransactionMetadata):
        return not tx.req_inflight(self.tx)

    def _get_locked(self, tx, timeout) -> bool:
        return self.cv.wait_for(lambda: self._tx_done_locked(tx), timeout)

    def update(self, tx : TransactionMetadata,
               timeout : Optional[float] = None):
        with self.mu:
            txx = self.tx.merge(tx)
            logging.debug('SyncFilterAdapter.updated merged %s', txx)

            if txx is None:
                return FlaskResponse(status=400, response=['invalid tx delta'])
            self.tx = txx
            self._version += 1

            if not self.inflight:
                if self.executor.submit(lambda: self._update()) is None:
                    return FlaskResponse(status=500, response=['server busy'])
                self.inflight = True
            self._get_locked(self.tx, timeout)
            tx.replace_from(self.tx)

    def get(self, timeout : Optional[float] = None
            ) -> Optional[TransactionMetadata]:
        with self.mu:
            tx = self.tx.copy()
            self._get_locked(tx, timeout)
            return self.tx.copy()

class RestEndpointAdapter(Handler):
    async_filter : Optional[AsyncFilter]
    _tx_rest_id : str

    blob_storage : BlobStorage
    _blob_rest_id : str
    rest_id_factory : Optional[Callable[[], str]]
    http_host : Optional[str] = None

    def __init__(self, async_filter : Optional[AsyncFilter] = None,
                 tx_rest_id=None,
                 blob_storage=None,
                 blob_rest_id=None,
                 rest_id_factory : Optional[Callable[[], str]] = None,
                 http_host : Optional[str] = None):
        self.async_filter = async_filter
        self._tx_rest_id = tx_rest_id
        self.blob_storage = blob_storage
        self._blob_rest_id = blob_rest_id
        self.rest_id_factory = rest_id_factory
        self.http_host = http_host

    def tx_rest_id(self):
        return self._tx_rest_id

    def blob_rest_id(self):
        return self._blob_rest_id

    def _body(self, tx : TransactionMetadata):
        if tx.body:
            blob_id = tx.body.removeprefix('/blob/')
            # TODO this blob is not going to be reused and needs to be
            # retired ~immediately
            body_blob = self.blob_storage.get_finalized(blob_id)
            if body_blob is None:
                return FlaskResponse(status=404, response=['blob not found'])
            tx.body_blob = body_blob
            del tx.body

    def start(self, req_json,
              timeout : Optional[float] = None) -> Optional[FlaskResponse]:
        tx = TransactionMetadata.from_json(req_json)
        if tx is None:
            return FlaskResponse(status=400, response=['invalid request'])
        tx.host = self.http_host
        self._body(tx)
        self.async_filter.update(tx, timeout)
        self._tx_rest_id = self.async_filter.rest_id
        return jsonify(tx.to_json(WhichJson.REST_READ))

    def get(self, req_json : Dict[str, Any],
            timeout : Optional[float] = None) -> FlaskResponse:
        tx = self.async_filter.get(timeout)
        if tx is None:
            return FlaskResponse(status=500, response=['get tx'])
        return jsonify(tx.to_json(WhichJson.REST_READ))

    def patch(self, req_json : Dict[str, Any],
              timeout : Optional[float] = None) -> FlaskResponse:
        logging.debug('RestEndpointAdapter.patch %s %s',
                      self._tx_rest_id, req_json)
        tx = TransactionMetadata.from_json(req_json)
        if tx is None:
            return FlaskResponse(status=400, response=['invalid request'])
        self._body(tx)
        self.async_filter.update(tx, timeout)
        return jsonify(tx.to_json(WhichJson.REST_READ))

    def etag(self) -> str:
        return ('%d' % self.async_filter.version())

    def create_blob(self, request : FlaskRequest) -> FlaskResponse:
        self._blob_rest_id = self.rest_id_factory()
        if self.blob_storage.create(self._blob_rest_id) is None:
            return FlaskResponse(status=500)
        return FlaskResponse(status=201)

    def put_blob(self, request : FlaskRequest, content_range : ContentRange
                 ) -> FlaskResponse:
        logging.debug('RestEndpointAdapter.put_blob %s content-range: %s',
                      self._blob_rest_id, content_range)
        offset = 0
        last = True
        offset = content_range.start
        blob = self.blob_storage.get_for_append(self._blob_rest_id)
        if blob is None:
            return FlaskResponse(status=404, response=['unknown blob'])
        appended, result_len, content_length = blob.append_data(
            offset, request.data, content_range.length)
        logging.debug('RestEndpointAdapter.put_blob %s %d %s',
                      appended, result_len, content_length)
        resp = FlaskResponse()
        if not appended:
            resp.status = 416
            resp.response = ['invalid range']

        resp = jsonify({})
        # cf RestTransactionHandler.build_resp() regarding content-range
        resp.headers.set('content-range',
                         ContentRange('bytes', 0, result_len, content_length))
        return resp


class EndpointFactory(ABC):
    @abstractmethod
    def create(self, http_host : str) -> Optional[SyncFilterAdapter]:
        pass
    @abstractmethod
    def get(self, rest_id : str) -> Optional[SyncFilterAdapter]:
        pass

class RestEndpointAdapterFactory(HandlerFactory):
    endpoint_factory : EndpointFactory
    blob_storage : BlobStorage
    rest_id_factory : Callable[[], str]

    def __init__(self, endpoint_factory, blob_storage : BlobStorage,
                 rest_id_factory : Callable[[], str]):
        self.endpoint_factory = endpoint_factory
        self.blob_storage = blob_storage
        self.rest_id_factory = rest_id_factory

    def create_tx(self, http_host) -> Optional[RestEndpointAdapter]:
        endpoint = self.endpoint_factory.create(http_host)
        if endpoint is None:
            return None
        return RestEndpointAdapter(async_filter=endpoint,
                                   blob_storage=self.blob_storage,
                                   http_host=http_host)

    def get_tx(self, tx_rest_id) -> Optional[RestEndpointAdapter]:
        endpoint = self.endpoint_factory.get(tx_rest_id)
        if endpoint is None: return None
        return RestEndpointAdapter(async_filter=endpoint,
                                   tx_rest_id=tx_rest_id,
                                   blob_storage=self.blob_storage)

    def create_blob(self) -> Optional[Handler]:
        return RestEndpointAdapter(blob_storage=self.blob_storage,
                                   rest_id_factory=self.rest_id_factory)

    def get_blob(self, blob_rest_id) -> Optional[RestEndpointAdapter]:
        return RestEndpointAdapter(blob_storage=self.blob_storage,
                                   blob_rest_id=blob_rest_id)
