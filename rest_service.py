from flask import Flask
from flask import Response
from flask import jsonify
from flask import request

from werkzeug.datastructures import ContentRange
import werkzeug.http

from blobs import Blob, BlobStorage
from blob import InlineBlob
from tags import Tag

from executor import Executor

from threading import Lock, Condition

import logging

from typing import Any, Dict, Optional, List
from typing import Callable, Tuple

class Transaction:
    id : str
    chunk_id : int = None
    last : bool = False
    endpoint : Any
    start_resp : Response = None
    # response from the last append or the first one that yielded an error
    final_status : Response = None
    start_inflight = False
    final_inflight = False

    lock : Lock
    cv : Condition

    def __init__(self):
        self.lock = Lock()
        self.cv = Condition(self.lock)

    def start(self, local_host, remote_host,
              mail_from, transaction_esmtp,
              rcpt_to, rcpt_esmtp):
        with self.lock:
            self.start_inflight = True
        try:
            resp = self.endpoint.start(
                local_host, remote_host,
                mail_from, transaction_esmtp,
                rcpt_to, rcpt_esmtp)
        finally:
            with self.lock:
                self.start_inflight = False
        logging.info('%s rest service transaction start %s', self.id, resp)
        with self.lock:
            self.start_resp = resp
            # mx must wait for start resp but msa could have detached and
            # it failed by the time the client sent the data
            if (self.msa and resp.perm()) or (not self.msa and resp.err()):
                self.final_status = resp
            self.cv.notify()

    # precondition: self.final_status is None
    def append_data(self, last : bool, blob : Blob):
        logging.info('%s rest service Transaction.append_data %s %d',
                     self.id, last, blob.len())

        if self.final_status:
            logging.info('%s rest service Transaction.append_data '
                         'precondition: '
                         'already has final_status', self.id)
            return self.final_status  # XXX internal/failed_precondition?

        if last:
            with self.lock:
                self.final_inflight = True
        resp = None
        try:
            resp = self.endpoint.append_data(last, blob)
        except:
            resp = Response.Internal('rest transaction append_data exception')
        logging.info('%s rest service Transaction.append_data %s',
                     self.id, resp)
        with self.lock:
            if last:
                self.final_inflight = False
            if last or resp.err():
                self.final_status = resp
            self.cv.notify()
        return resp

# TODO need some integration with storage so these IDs can be stable
class RestResources:
    # real implementation uses secrets.token_urlsafe()
    next_id = 0

    # current transaction id on each endpoint
    transaction : Dict[str, Transaction] = {}

    def add_transaction(self, t : Transaction) -> str:
        id = "t%d" % self.next_id
        self.next_id += 1
        t.id = id
        self.transaction[id] = t

    def get_transaction(self, id : str) -> Transaction:
        return self.transaction[id] if id in self.transaction else None


def create_app(endpoints : Callable[[str], ["Endpoint", int]],
               executor : Executor,
               blobs : BlobStorage):
    app = Flask(__name__)

    resources = RestResources()


    @app.route('/transactions', methods=['POST'])
    def start_transaction() -> Response:
        print(request, request.headers)

        t = Transaction()
        t.endpoint, tag, t.msa = endpoints(request.headers['host'])

        req_json = request.get_json()
        local_host = req_json.get('local_host', None)
        remote_host = req_json.get('remote_host', None)
        mail_from = req_json.get('mail_from', None)
        rcpt_to = req_json.get('rcpt_to', None)

        # local_host/remote_host ~
        # http x-forwarded-host/-for?

        executor.enqueue(tag, lambda: t.start(
            local_host, remote_host,
            mail_from, transaction_esmtp=None,
            rcpt_to=rcpt_to, rcpt_esmtp=None))

        resources.add_transaction(t)
        json_out = {
            'url': '/transactions/' + t.id
        }

        return jsonify(json_out)

    @app.route('/transactions/<transaction_id>',
               methods=['GET'])
    def get_transaction(transaction_id) -> Response:
        t = resources.get_transaction(transaction_id)
        if not t:
            return Response(status_code=404, response=['transaction not found'])
        json_out = {}

        # TODO: this currently waits for 1s if inflight which isn't
        # great in terms of busy-waiting but still pretty short
        # relative to a http timeout

        with t.lock:
            if t.start_inflight or t.final_inflight:
                t.cv.wait_for(
                    lambda: not t.start_inflight and not t.final_inflight,
                    1)

        # mx always waits for start resp
        # msa waits for start resp err within short timeout and then detaches

        # xxx this is the wrong way to surface this?
        if t.start_resp:
            json_out['start_response'] = t.start_resp.to_json()

        if t.final_status:
            json_out['final_status'] = t.final_status.to_json()

        return jsonify(json_out)


    @app.route('/transactions/<transaction_id>/appendData',
               methods=['POST'])
    def append_data(transaction_id):
        req_json = request.get_json()
        logging.info("rest service append_data %s %s %s",
                     request, request.headers, req_json)

        assert(isinstance(req_json['chunk_id'], int))
        chunk_id : int = req_json['chunk_id']
        last : bool = req_json['last']

        t = resources.get_transaction(transaction_id)
        if t is None:
            return Response(status_code=404, response=['transaction not found'])
        if t.final_status:
            return jsonify({'final_status': t.final_status.to_json()})

        if t.chunk_id == chunk_id:  # noop
            # XXX this needs to just return the previous response
            return Response(status=400, response=['dupe appendData'])

        if t.last and last:
            return Response(status=400, response=['bad last after last'])

        t.chunk_id = chunk_id
        t.last = last

        # (short) data inline in request
        if 'd' in req_json:
            logging.info('rest service %s append_data inline %d',
                         t.id, len(req_json['d']))
            d : bytes = req_json['d'].encode('utf-8')
            resp = t.append_data(last, InlineBlob(d))
            resp_json = {}
            if resp.err():
                resp_json['final_status'] = resp.to_json()
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

        blob_done_cb = lambda blob: executor.enqueue(
            Tag.DATA, lambda: t.append_data(last, blob))

        if 'uri' in req_json and req_json['uri'] is not None:
            blob_id = req_json['uri'].removeprefix('/blob/')
            if blobs.add_waiter(blob_id, blob_done_cb):
                return jsonify({})
        blob_id = blobs.create(blob_done_cb)

        resp_json = { 'uri': '/blob/' + blob_id }
        rest_resp = jsonify(resp_json)
        logging.info('rest service %s %s', t.id, resp_json)
        return rest_resp

    @app.route('/transactions/<transaction_id>/smtpMode',
               methods=['POST'])
    def smtp_mode(transaction_id):
        t = resources.get_transaction(transaction_id)
        if t is None:
            return Response(status_code=404, response=['transaction not found'])
        if t.endpoint.set_durable() is None:
            return Response(status=500,
                            response='RouterTransaction.smtp_mode set_durable')
        return Response()


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

        resp = jsonify({})
        resp.headers.set('content-range',
                         ContentRange('bytes', 0, result_len,
                                      result_len if last else None))
        return resp

    return app



