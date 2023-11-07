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

import response
MailResponse = response.Response

class Transaction:
    id : str
    msa : bool
    chunk_id : int = None
    last : bool = False
    endpoint : Any
    start_resp : MailResponse = None
    # response from the last append or the first one that yielded an error
    final_status : MailResponse = None
    start_inflight = False

    local_host = None
    remote_host = None
    mail_from = None
    transaction_esmtp = None
    rcpt_to = None
    rcpt_esmtp = None
    mx_multi_rcpt = False

    lock : Lock

    def __init__(self, endpoint, msa, blob_storage, rest_resources):
        self.endpoint = endpoint
        self.msa = msa
        self.blob_storage = blob_storage
        self.rest_resources = rest_resources

    def init(self, req_json) -> Optional[Response]:
        if not self.endpoint:
            return Response(status=404, response=['unknown host'])
        self.id = self.endpoint.rest_id

        self.local_host = req_json.get('local_host', None)
        self.remote_host = req_json.get('remote_host', None)
        self.mail_from = req_json.get('mail_from', None)
        self.rcpt_to = req_json.get('rcpt_to', None)

        return None

    def get(self):
        # TODO: this currently waits for 1s if inflight which isn't
        # great in terms of busy-waiting but still pretty short
        # relative to a http timeout

        # mx always waits for start resp
        # msa waits for start resp err within short timeout and then detaches

        json_out = {}
        # xxx this is the wrong way to surface this?
        if not self.start_resp:
            self.start_resp = self.endpoint.get_start_result()
        if self.start_resp:
            json_out['start_response'] = self.start_resp.to_json()

        if not self.final_status:
            self.final_status = self.endpoint.get_final_status(
                timeout = 1 if self.last else 0)
        if self.final_status:
            json_out['final_status'] = self.final_status.to_json()

        return jsonify(json_out)

    def start(self):
        self.start_resp = self.endpoint.start(
            self.local_host, self.remote_host,
            self.mail_from, self.transaction_esmtp,
            self.rcpt_to, self.rcpt_esmtp)

    def append_data(self, req_json):
        assert(isinstance(req_json['chunk_id'], int))
        chunk_id : int = req_json['chunk_id']
        last : bool = req_json['last']
        if chunk_id == 0:
            self.mx_multi_rcpt = bool(req_json.get('mx_multi_rcpt', False))

        # XXX do this here or in RouterTransaction? Note the router
        # transaction code propagates start errors to final and this
        # currently doesn't.
        if self.msa:
            if self.start_resp is not None and self.start_resp.perm():
                return Response(
                    status=400,
                    response=['failed precondition: msa start upstream perm'])
            if self.final_status is not None and self.final_status.perm():
                return Response(
                    status=400,
                    response=['failed precondition: msa append upstream perm'])
        else:  # mx
            if self.start_resp is None or self.start_resp.err():
                return Response(
                    status=400,
                    response=['failed precondition: mx start upstream err'])
            if not self.mx_multi_rcpt and self.final_status is not None:
                return jsonify({'final_status': self.final_status.to_json()})


        if self.chunk_id == chunk_id:  # noop
            # XXX this needs to just return the previous response
            return Response(status=400, response=['dupe appendData'])

        # XXX shouldn't this verify chunk_id == self.chunk_id + 1?

        if self.last and last:
            return Response(status=400, response=['bad last after last'])

        if self.mx_multi_rcpt:
            self.endpoint.set_mx_multi_rcpt()

        self.chunk_id = chunk_id
        self.last = last

        # (short) data inline in request
        if 'd' in req_json:
            logging.info('rest service %s append_data inline %d',
                         self.id, len(req_json['d']))
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
            if self.blob_storage.add_waiter(blob_id, blob_done_cb):
                return jsonify({})
        blob_id = self.blob_storage.create(blob_done_cb)

        resp_json = { 'uri': '/blob/' + str(blob_id) }
        rest_resp = jsonify(resp_json)
        logging.info('rest service %s %s', self.id, resp_json)
        return rest_resp


    def append_blob_upstream(self, last : bool, blob : Blob):
        logging.info('%s rest service Transaction.append_data %s %d',
                     self.id, last, blob.len())
        # XXX put back a precondition check here similar to that at
        # the beginning of append_data() that the upstream transaction
        # hasn't already failed

        resp = None
        try:
            resp = self.endpoint.append_data(last, blob)
        except:
            resp = MailResponse.Internal(
                'rest transaction append_data exception')
        logging.info('%s rest service Transaction.append_data %s',
                     self.id, resp)

        if last:
            self.last = True

        if resp is not None:
            self.final_status = resp

        return resp

    def set_durable(self):
        if not self.last:
            rest_resp = Response(
                status=400,
                response=['RouterTransaction.smtp_mode set_durable: have not '
                          'received last append_data'])

        # this is where we "detach" the operation so
        # subsequent rest calls go through to the router
        self.rest_resources.remove_transaction(self.id)

        if self.endpoint.set_durable() is None:
            rest_resp = Response(
                status=500,
                response=['RouterTransaction.smtp_mode set_durable'])
        rest_resp = Response()
        logging.info('rest service Transaction.set_durable %s', rest_resp)
        return rest_resp


# TODO need some integration with storage so these IDs can be stable
class RestResources:
    # current transaction id on each endpoint
    transaction : Dict[str, Transaction] = {}

    def add_transaction(self, t : Transaction):
        # XXX locking?
        assert(t.id not in self.transaction)
        self.transaction[t.id] = t

    def get_transaction(self, id : str) -> Optional[Transaction]:
        return self.transaction.get(id, None)

    def remove_transaction(self, id):
        if id in self.transaction:
            del self.transaction[id]

#class EndpointFactory:
#    def create(self, host) -> Endpoint, bool:  # msa
#        pass
#    def get(self, rest_id) -> Endpoint:
#        pass


def create_app(
        endpoint_factory,
        blobs : BlobStorage):
    app = Flask(__name__)

    resources = RestResources()

    @app.route('/transactions', methods=['POST'])
    def start_transaction() -> Response:
        print(request, request.headers)
        endpoint, msa = endpoint_factory.create(request.headers['host'])
        t = Transaction(endpoint, msa, blobs, resources)
        if not request.is_json:
            return Response(status=400, response=['not json'])
        resp = t.init(request.get_json())
        if resp is not None: return resp

        # local_host/remote_host ~
        # http x-forwarded-host/-for?

        resources.add_transaction(t)
        t.start()

        json_out = {
            'url': '/transactions/' + t.id
        }

        return jsonify(json_out)

    @app.route('/transactions/<transaction_id>',
               methods=['GET'])
    def get_transaction(transaction_id) -> Response:
        t = resources.get_transaction(transaction_id)
        if t is None:
            e = endpoint_factory.get(transaction_id)
            assert(e is not None)
            # oneshot
            t = Transaction(e, False, None, None)  # msa, blobs, resources xxx?
        if t is None:
            return Response(status=404, response=['transaction not found'])
        return t.get()

    @app.route('/transactions/<transaction_id>/appendData',
               methods=['POST'])
    def append_data(transaction_id):
        t = resources.get_transaction(transaction_id)
        if t is None:
            return Response(status=404, response=['transaction not found'])
        if not request.is_json:
            return Response(status=400, response=['not json'])
        logging.info("rest service append_data %s %s %s",
                     request, request.headers, request.get_json())
        rest_resp = t.append_data(request.get_json())
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
        t = resources.get_transaction(transaction_id)
        if t is None:
            return Response(status=404, response=['transaction not found'])
        rest_resp = t.set_durable()
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



