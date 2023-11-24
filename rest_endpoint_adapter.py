from typing import Any, Dict, Optional
from abc import ABC, abstractmethod

from flask import Response as FlaskResponse, Request as FlaskRequest, jsonify
from werkzeug.datastructures import ContentRange
import werkzeug.http


from response import Response as MailResponse
from blob import Blob, InlineBlob
from blobs import BlobStorage

from rest_service_handler import Handler, HandlerFactory

import logging


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

    def start(self, req_json) -> Optional[FlaskResponse]:
        return self.endpoint.start(
            local_host = req_json.get('local_host', None),
            remote_host = req_json.get('remote_host', None),
            mail_from = req_json.get('mail_from', None),
            transaction_esmtp = None,  # XXX
            rcpt_to = req_json.get('rcpt_to', None),
            rcpt_esmtp = None)

    def get(self, req_json : Dict[str, Any]) -> FlaskResponse:
        # TODO: the upstream code currently waits for 1s if inflight
        # which isn't great in terms of busy-waiting but still pretty
        # short relative to a http timeout

        # mx always waits for start resp
        # msa waits for start resp (perm) err within short timeout and
        # then detaches

        json_out = {}
        # xxx this is the wrong way to surface this?
        start_resp = self.endpoint.get_start_result()
        if start_resp is not None:
            json_out['start_response'] = start_resp.to_json()

        final_status = self.endpoint.get_final_status(
            timeout = 1 if self.endpoint.received_last else 0)
        if final_status is not None:
            json_out['final_status'] = final_status.to_json()

        return jsonify(json_out)


    def append(self, req_json : Dict[str, Any]) -> FlaskResponse:
        assert(isinstance(req_json['chunk_id'], int))
        chunk_id : int = req_json['chunk_id']
        last : bool = req_json['last']
        mx_multi_rcpt = False
        if chunk_id == 0:
            mx_multi_rcpt = bool(req_json.get('mx_multi_rcpt', False))

        if self.endpoint.blobs_received == chunk_id:  # noop
            # XXX this needs to just return the previous response
            return FlaskResponse(status=400, response=['dupe appendData'])

        if chunk_id != self.endpoint.blobs_received + 1:
            return FlaskResponse(status=400,
                            response=['bad blob num %d %d',
                                      chunk_id, self.endpoint.blobs_received])

        if self.endpoint.received_last and last:
            return FlaskResponse(status=400, response=['bad last after last'])

        if mx_multi_rcpt:
            self.endpoint.set_mx_multi_rcpt()

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

        if resp is not None:
            self.final_status = resp

        return resp

    def put_blob(self, request : FlaskRequest) -> FlaskResponse:
        offset = 0
        last = True
        if 'content-range' in request.headers:
            range = werkzeug.http.parse_content_range_header(
                request.headers.get('content-range'))
            if not range or range.units != 'bytes':
                return FlaskResponse(status=400, response=['bad range'])
            offset = range.start
            last = range.length is not None and range.stop == range.length

        result_len = self.blob_storage.append(
            self.blob_rest_id, offset, request.data, last)
        if result_len is None:
            return FlaskResponse(status=404, response=['unknown blob'])

        resp = jsonify({})
        resp.headers.set('content-range',
                         ContentRange('bytes', 0, result_len,
                                      result_len if last else None))
        return resp


    def set_durable(self, req_json : Dict[str, Any]) -> FlaskResponse:
        if not self.endpoint.received_last:
            return FlaskResponse(
                status=400,
                response=['RouterTransaction.smtp_mode set_durable: have not '
                          'received last append_data'])

        resp = self.endpoint.set_durable()
        if resp.ok():
            return FlaskResponse()

        # xxx code
        rest_resp = FlaskResponse(
            status=500,
            response=['RouterTransaction.smtp_mode set_durable'])
        logging.info('rest service Transaction.set_durable %s', rest_resp)
        return rest_resp


class EndpointFactory(ABC):
    @abstractmethod
    def create(self, http_host : str) -> Optional[object]:  # Endpoint
        pass
    @abstractmethod
    def get(self, rest_id : str) -> Optional[object]:  # Endpoint
        pass

class RestEndpointAdapterFactory(HandlerFactory):
    blob_storages : BlobStorage
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
