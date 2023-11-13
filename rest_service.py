from flask import Flask
from flask import Response
from flask import jsonify
from flask import request

from werkzeug.datastructures import ContentRange
import werkzeug.http

from blobs import Blob, BlobStorage
from blob import InlineBlob
from tags import Tag

from threading import Lock, Condition

import logging

from typing import Any, Dict, Optional, List
from typing import Callable, Tuple

from response import Response as MailResponse

class RestRequestHandler:
    rest_id : str
    endpoint : Any

    def __init__(self, endpoint, blob_storage):
        self.endpoint = endpoint
        self.rest_id = endpoint.rest_id
        self.blob_storage = blob_storage

    def start(self, req_json) -> Optional[Response]:
        return self.endpoint.start(
            local_host = req_json.get('local_host', None),
            remote_host = req_json.get('remote_host', None),
            mail_from = req_json.get('mail_from', None),
            transaction_esmtp = None,  # XXX
            rcpt_to = req_json.get('rcpt_to', None),
            rcpt_esmtp = None)

    def get(self):
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


    def append_data(self, req_json):
        assert(isinstance(req_json['chunk_id'], int))
        chunk_id : int = req_json['chunk_id']
        last : bool = req_json['last']
        mx_multi_rcpt = False
        if chunk_id == 0:
            mx_multi_rcpt = bool(req_json.get('mx_multi_rcpt', False))

        if self.endpoint.blobs_received == chunk_id:  # noop
            # XXX this needs to just return the previous response
            return Response(status=400, response=['dupe appendData'])

        if chunk_id != self.endpoint.blobs_received + 1:
            return Response(status=400,
                            response=['bad blob num %d %d',
                                      chunk_id, self.endpoint.blobs_received])

        if self.endpoint.received_last and last:
            return Response(status=400, response=['bad last after last'])

        if mx_multi_rcpt:
            self.endpoint.set_mx_multi_rcpt()

        # (short) data inline in request
        if 'd' in req_json:
            logging.info('rest service %s append_data inline %d',
                         self.rest_id, len(req_json['d']))
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
        logging.info('rest service %s %s', self.rest_id, resp_json)
        return rest_resp


    def append_blob_upstream(self, last : bool, blob : Blob):
        logging.info('%s rest service RestRequestHandler.append_data %s %d',
                     self.rest_id, last, blob.len())
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
                     self.rest_id, resp)

        if last:
            self.last = True

        if resp is not None:
            self.final_status = resp

        return resp

    def set_durable(self):
        if not self.endpoint.received_last:
            return Response(
                status=400,
                response=['RouterTransaction.smtp_mode set_durable: have not '
                          'received last append_data'])

        resp = self.endpoint.set_durable()
        if resp.ok():
            return Response()

        # xxx code
        rest_resp = Response(
            status=500,
            response=['RouterTransaction.smtp_mode set_durable'])
        logging.info('rest service Transaction.set_durable %s', rest_resp)
        return rest_resp


#class EndpointFactory:
#    def create(self, host) -> Endpoint, bool:  # msa
#        pass
#    def get(self, rest_id) -> Endpoint:
#        pass


def create_app(
        endpoint_factory,
        blobs : BlobStorage):
    app = Flask(__name__)

    @app.route('/transactions', methods=['POST'])
    def start_transaction() -> Response:
        print(request, request.headers)
        endpoint, msa = endpoint_factory.create(request.headers['host'])
        if not request.is_json:
            return Response(status=400, response=['not json'])
        handler = RestRequestHandler(endpoint, blobs)
        resp = handler.start(request.get_json())

        json_out = {
            'url': '/transactions/' + handler.rest_id
        }
        # xxx this could be sync success or something like a syntax error
        if resp is not None:
            json_out['start_response'] = resp.to_json()

        return jsonify(json_out)

    @app.route('/transactions/<transaction_id>',
               methods=['GET'])
    def get_transaction(transaction_id) -> Response:
        t = endpoint_factory.get(transaction_id)
        if t is None:
            return Response(status=404, response=['transaction not found'])
        handler = RestRequestHandler(t, None)  # msa, blobs
        return handler.get()

    @app.route('/transactions/<transaction_id>/appendData',
               methods=['POST'])
    def append_data(transaction_id):
        if not request.is_json:
            return Response(status=400, response=['not json'])

        t = endpoint_factory.get(transaction_id)
        if t is None:
            return Response(status=404, response=['transaction not found'])
        handler = RestRequestHandler(t, blobs)

        logging.info("rest service append_data %s %s %s",
                     request, request.headers, request.get_json())
        rest_resp = handler.append_data(request.get_json())
        logging.info('rest service append_data %s', rest_resp)
        return rest_resp

    @app.route('/transactions/<transaction_id>/smtpMode',
               methods=['POST'])
    def smtp_mode(transaction_id):
        logging.info("rest service smtp_mode %s %s",
                     request, request.headers)
        # TODO this should take a parameter in the json whether or not
        # to emit a bounce. smtp gw wants this; first-class rest
        # clients that are willing to poll the lro may not.
        t = endpoint_factory.get(transaction_id)
        if t is None:
            return Response(status=404, response=['transaction not found'])
        handler = RestRequestHandler(t, blobs)
        rest_resp = handler.set_durable()
        logging.info("rest service smtp_mode %s", rest_resp)
        return rest_resp

    @app.route('/blob/<blob_id>', methods=['PUT'])
    def append_data_chunk(blob_id) -> Response:
        logging.info("rest service append_data_chunk %s %s",
                     request, request.headers)

        offset = 0
        last = True
        if 'content-range' in request.headers:
            range = werkzeug.http.parse_content_range_header(
                request.headers.get('content-range'))
            if not range or range.units != 'bytes':
                return Response(status=400, response=['bad range'])
            offset = range.start
            last = range.length is not None and range.stop == range.length

        result_len = blobs.append(blob_id, offset, request.data, last)
        if result_len is None:
            return Response(status=404, response=['unknown blob'])

        resp = jsonify({})
        resp.headers.set('content-range',
                         ContentRange('bytes', 0, result_len,
                                      result_len if last else None))
        return resp

    return app



