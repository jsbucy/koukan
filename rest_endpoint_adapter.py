from typing import Any, Dict, Optional
from abc import ABC, abstractmethod
import logging
import time

from flask import Response as FlaskResponse, Request as FlaskRequest, jsonify
from werkzeug.datastructures import ContentRange
import werkzeug.http

from response import Response as MailResponse
from blob import Blob, InlineBlob
from blobs import BlobStorage
from rest_service_handler import Handler, HandlerFactory
from filter import TransactionMetadata, Filter

# XXX this expects the endpoint to look like SmtpEndpoint which is now
# completely different from the current Filter api
class RestEndpointAdapter(Handler):
    endpoint : Any
    _tx_rest_id : str

    blob_storage : BlobStorage
    blob_rest_id : str

    def __init__(self, endpoint=None,
                 tx_rest_id=None,
                 blob_storage=None, blob_rest_id=None):
        self.endpoint = endpoint
        self._tx_rest_id = tx_rest_id
        self.blob_storage = blob_storage
        self.blob_rest_id = blob_rest_id

    def tx_rest_id(self): return self._tx_rest_id

    def start(self, req_json,
              timeout : Optional[float] = None) -> Optional[FlaskResponse]:
        tx = TransactionMetadata.from_json(req_json)
        self.endpoint.start(tx)
        # xxx inflight response fields per tx req fields
        rest_resp = FlaskResponse(200)
        rest_resp.set_data('{}')
        rest_resp.content_type = 'application/json'
        return rest_resp

    def get(self, req_json : Dict[str, Any],
              timeout : Optional[float] = None) -> FlaskResponse:
        json_out = {}

        if self.endpoint.mail_resp:
            json_out['mail_response'] = self.endpoint.mail_resp.to_json()
        if self.endpoint.rcpt_resp:
            json_out['rcpt_response'] = [
                r.to_json() for r in self.endpoint.rcpt_resp]
        if self.endpoint.data_response:
            json_out['data_response'] = self.endpoint.data_response.to_json()

        return jsonify(json_out)

    def patch(self, req_json : Dict[str, Any],
              timeout : Optional[float] = None) -> FlaskResponse:
        logging.debug('RestEndpointAdapter.patch %s %s',
                      self._tx_rest_id, req_json)
        if req_json != {'body': ''}:
            raise NotImplementedError()
        blob_done_cb = lambda blob: self.append_blob_upstream(True, blob)
        blob_id = self.blob_storage.create(blob_done_cb)
        resp_json = { 'body': '/blob/' + str(blob_id) }
        return jsonify(resp_json)

    def etag(self):
        return None

    def append(self, req_json : Dict[str, Any]) -> FlaskResponse:
        last : bool = req_json['last']

        if self.endpoint.received_last and last:
            return FlaskResponse(status=400, response=['bad last after last'])

        # (short) data inline in request
        if 'd' in req_json:
            logging.info('rest service %s append_data inline %d',
                         self._tx_rest_id, len(req_json['d']))
            d : bytes = req_json['d'].encode('utf-8')
            self.append_blob_upstream(last, InlineBlob(d))
            resp_json = {}
            return jsonify(resp_json)

        # (long) data via separate PUT

        # at this point, we're either reusing an existing blob id (add waiter)
        # or creating a new one and return the url

        # we don't invoke the next-hop transaction append until the
        # client has sent the whole blob

        # the blob PUT response is just for the data tranfer, not the upstream

        # but if the upstream had some error, that needs to propagate
        # back to the transaction final_result and subsequent append
        # will be failed precondition

        blob_done_cb = lambda blob: self.append_blob_upstream(last, blob)

        if 'uri' in req_json and req_json['uri'] is not None:
            blob_id = req_json['uri'].removeprefix('/blob/')
            if self.blob_storage.add_waiter(
                    blob_id, self.endpoint, blob_done_cb):
                return jsonify({})
        blob_id = self.blob_storage.create(blob_done_cb)

        resp_json = { 'uri': '/blob/' + str(blob_id) }
        rest_resp = jsonify(resp_json)
        logging.info('rest service %s %s', self._tx_rest_id, resp_json)
        return rest_resp


    def append_blob_upstream(self, last : bool, blob : Blob):
        logging.info('%s rest service RestRequestHandler.append_data %s %d',
                     self._tx_rest_id, last, blob.len())
        # XXX put back a precondition check here similar to that at
        # the beginning of append_data() that the upstream transaction
        # hasn't already failed

        resp = None
        try:
            resp = self.endpoint.append_data(last, blob)
        except:
            resp = MailResponse.Internal(
                'rest transaction append_data exception')
        logging.info('%s rest service RestRequestHandler.append_data %s',
                     self._tx_rest_id, resp)

        if last:
            self.last = True

        return resp

    def put_blob(self, request : FlaskRequest, content_range : ContentRange,
                 range_in_headers : bool) -> FlaskResponse:
        logging.debug('RestEndpointAdapter.put_blob %s content-range: %s',
                      self.blob_rest_id, content_range)
        offset = 0
        last = True
        offset = content_range.start
        last = (content_range.length is not None and
                content_range.stop == content_range.length)
        result_len = self.blob_storage.append(
            self.blob_rest_id, offset, request.data, last)
        if result_len is None:
            return FlaskResponse(status=404, response=['unknown blob'])

        resp = jsonify({})
        resp.headers.set('content-range',
                         ContentRange('bytes', 0, result_len,
                                      result_len if last else None))
        return resp


class EndpointFactory(ABC):
    @abstractmethod
    def create(self, http_host : str) -> Optional[object]:  # Endpoint
        pass
    @abstractmethod
    def get(self, rest_id : str) -> Optional[object]:  # Endpoint
        pass

class RestEndpointAdapterFactory(HandlerFactory):
    blob_storage : BlobStorage
    def __init__(self, endpoint_factory, blob_storage : BlobStorage):
        self.endpoint_factory = endpoint_factory
        self.blob_storage = blob_storage

    def create_tx(self, http_host) -> Optional[RestEndpointAdapter]:
        endpoint = self.endpoint_factory.create(http_host)
        if endpoint is None: return None
        return RestEndpointAdapter(endpoint=endpoint,
                                   tx_rest_id=endpoint.rest_id)

    def get_tx(self, tx_rest_id) -> Optional[RestEndpointAdapter]:
        endpoint = self.endpoint_factory.get(tx_rest_id)
        if endpoint is None: return None
        return RestEndpointAdapter(endpoint=endpoint,
                                   tx_rest_id=tx_rest_id,
                                   blob_storage=self.blob_storage)

    def get_blob(self, blob_rest_id) -> Optional[RestEndpointAdapter]:
        return RestEndpointAdapter(blob_storage=self.blob_storage,
                                   blob_rest_id=blob_rest_id)
