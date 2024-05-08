from typing import Any, Dict, Optional
from abc import ABC, abstractmethod
import logging
import time
from threading import Condition, Lock
import copy

from flask import Response as FlaskResponse, Request as FlaskRequest, jsonify
from werkzeug.datastructures import ContentRange
import werkzeug.http

from response import Response as MailResponse
from blob import Blob, InlineBlob
from blobs import BlobStorage
from rest_service_handler import Handler, HandlerFactory
from filter import TransactionMetadata, Filter, WhichJson
from executor import Executor

class FilterExecutor:
    executor : Executor
    filter : Filter
    tx : Optional[TransactionMetadata] = None
    mu : Lock
    cv : Condition
    inflight : bool = False
    rest_id : str
    _last_update : float

    def __init__(self, executor : Executor, filter : Filter, rest_id : str):
        self.executor = executor
        self.mu = Lock()
        self.cv = Condition(self.mu)
        self.filter = filter
        self.rest_id = rest_id
        self._last_update = time.monotonic()

    # for use in ttl/gc idle calcuation
    # returns now if there is an update inflight i.e. do not gc
    def last_update(self) -> float:
        if self.inflight:
            return time.monotonic()
        return self._last_update

    def _update(self, tx):
        logging.debug('FilterExecutor._update %s', tx)
        self.filter.on_update(tx)
        logging.debug('FilterExecutor._update done %s', tx)
        with self.mu:
            if self.tx is None:
                self.tx = copy.copy(tx)
            else:
                self.tx = self.tx.merge(tx)
            logging.debug('FilterExecutor._update done merged %s', self.tx)
            self.inflight = False
            self.cv.notify_all()
            self._last_update = time.monotonic()

    def _get_locked(self, timeout) -> FlaskResponse:
        self.cv.wait_for(
            lambda: self.tx is not None and not self.tx.req_inflight(), timeout)
        return jsonify(self.tx.to_json(WhichJson.REST_READ) if self.tx else {})

    def update(self, tx : TransactionMetadata,
               timeout : Optional[float] = None
               ) -> FlaskResponse:
        with self.mu:
            if self.inflight:
                return FlaskResponse(status=412, response=['write conflict'])
            self.inflight = True
            if self.executor.submit(lambda: self._update(tx)) is None:
                return FlaskResponse(status=500, response=['server busy'])
            return self._get_locked(timeout)

    def get(self, timeout : Optional[float] = None) -> FlaskResponse:
        with self.mu:
            return self._get_locked(timeout)


class RestEndpointAdapter(Handler):
    executor : Optional[FilterExecutor]
    _tx_rest_id : str

    blob_storage : BlobStorage
    _blob_rest_id : str

    def __init__(self, executor : Optional[FilterExecutor] = None,
                 tx_rest_id=None,
                 blob_storage=None, blob_rest_id=None):
        self.executor = executor
        self._tx_rest_id = tx_rest_id
        self.blob_storage = blob_storage
        self._blob_rest_id = blob_rest_id

    def tx_rest_id(self):
        return self._tx_rest_id

    def blob_rest_id(self):
        return self._blob_rest_id

    def start(self, req_json,
              timeout : Optional[float] = None) -> Optional[FlaskResponse]:
        tx = TransactionMetadata.from_json(req_json)
        return self.executor.update(tx)

    def get(self, req_json : Dict[str, Any],
            timeout : Optional[float] = None) -> FlaskResponse:
        return self.executor.get(timeout)

    def patch(self, req_json : Dict[str, Any],
              timeout : Optional[float] = None) -> FlaskResponse:
        logging.debug('RestEndpointAdapter.patch %s %s',
                      self._tx_rest_id, req_json)
        tx = TransactionMetadata.from_json(req_json)
        if tx is None:
            return FlaskResponse(status=400, response=['invalid request'])
        if tx.body:
            blob_id = tx.body.removeprefix('/blob/')
            body_blob = self.blob_storage.get(blob_id)
            if body_blob is None:
                return FlaskResponse(status=404, response=['blob not found'])
            tx.body_blob = body_blob
        return self.executor.update(tx)

    def etag(self) -> str:
        # TODO TBD exactly how to sort this out, possibly a subclass
        # of Filter that provides this?
        assert hasattr(self.executor.filter, 'version')
        # pytype: disable=attribute-error
        return ('%d' % self.executor.filter.version)
        # pytype: enable=attribute-error

    def create_blob(self, request : FlaskRequest) -> FlaskResponse:
        self._blob_rest_id = self.blob_storage.create()
        return FlaskResponse(status=201)

    def put_blob(self, request : FlaskRequest, content_range : ContentRange
                 ) -> FlaskResponse:
        logging.debug('RestEndpointAdapter.put_blob %s content-range: %s',
                      self._blob_rest_id, content_range)
        offset = 0
        last = True
        offset = content_range.start
        last = (content_range.length is not None and
                content_range.stop == content_range.length)
        result_len = self.blob_storage.append(
            self._blob_rest_id, offset, request.data, last)
        if result_len is None:
            return FlaskResponse(status=404, response=['unknown blob'])
        resp = FlaskResponse()
        if result_len != offset + len(request.data):
            resp.status = 416
            resp.response = ['invalid range']

        resp = jsonify({})
        # cf RestTransactionHandler.build_resp() regarding content-range
        resp.headers.set('content-range',
                         ContentRange('bytes', 0, result_len,
                                      result_len if last else None))
        return resp


class EndpointFactory(ABC):
    @abstractmethod
    def create(self, http_host : str) -> FilterExecutor:
        pass
    @abstractmethod
    def get(self, rest_id : str) -> FilterExecutor:
        pass

class RestEndpointAdapterFactory(HandlerFactory):
    endpoint_factory : EndpointFactory
    blob_storage : BlobStorage

    def __init__(self, endpoint_factory, blob_storage : BlobStorage):
        self.endpoint_factory = endpoint_factory
        self.blob_storage = blob_storage

    def create_tx(self, http_host) -> Optional[RestEndpointAdapter]:
        endpoint = self.endpoint_factory.create(http_host)
        if endpoint is None:
            return None
        return RestEndpointAdapter(executor=endpoint,
                                   tx_rest_id=endpoint.rest_id,
                                   blob_storage=self.blob_storage)

    def get_tx(self, tx_rest_id) -> Optional[RestEndpointAdapter]:
        endpoint = self.endpoint_factory.get(tx_rest_id)
        if endpoint is None: return None
        return RestEndpointAdapter(executor=endpoint,
                                   tx_rest_id=tx_rest_id,
                                   blob_storage=self.blob_storage)

    def create_blob(self) -> Optional[Handler]:
        return RestEndpointAdapter(blob_storage=self.blob_storage)

    def get_blob(self, blob_rest_id) -> Optional[RestEndpointAdapter]:
        return RestEndpointAdapter(blob_storage=self.blob_storage,
                                   blob_rest_id=blob_rest_id)
