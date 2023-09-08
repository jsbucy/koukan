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
        logging.info('rest service transaction start %s', resp)
        with self.lock:
            self.start_resp = resp
            self.cv.notify()

    def append_data(self, last : bool, blob : Blob):
        if last:
            with self.lock:
                self.final_inflight = True
        try:
            resp = self.endpoint.append_data(last, blob)
        finally:
            if last:
                with self.lock:
                    self.final_inflight = False
        if last: # XXX?
            with self.lock:
                self.final_status = resp
                self.cv.notify()

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
        t.endpoint, tag, msa = endpoints(request.headers['host'])

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
            r = Response()
            r.status_code = 404
            return r
        json_out = {}

        # TODO: this currently waits for 5s if inflight which isn't
        # great in terms of busy-waiting but still pretty short
        # relative to a http timeout

        with t.lock:
            if t.start_inflight or t.final_inflight:
                t.cv.wait_for(
                    lambda: not t.start_inflight and not t.final_inflight,
                    5)

        if t.start_resp:
            json_out['start_response'] = t.start_resp.to_json()

        if t.final_status:
            json_out['final_status'] = t.final_status.to_json()

        return jsonify(json_out)


    @app.route('/transactions/<transaction_id>/appendData',
               methods=['POST'])
    def append_data(transaction_id):
        req_json = request.get_json()
        print(request, request.headers, req_json)

        assert(isinstance(req_json['chunk_id'], int))
        chunk_id : int = req_json['chunk_id']
        last : bool = req_json['last']

        t = resources.get_transaction(transaction_id)
        if t.chunk_id == chunk_id:  # noop
            # XXX this needs to just return the previous response
            return Response(status=400, response=['dupe appendData'])

        if t.last and last:
            return Response(status=400, response=['bad last after last'])

        t.chunk_id = chunk_id
        t.last = last
        if 'd' in req_json:
            d : bytes = req_json['d'].encode('utf-8')
            executor.enqueue(Tag.DATA,
                             lambda: t.append_data(last, InlineBlob(d)))
            return jsonify({})

        blob_done_cb = lambda: executor.enqueue(
            Tag.DATA, lambda blob: t.endpoint.append_data(last, blob))

        if 'uri' in req_json and req_json['uri'] is not None:
            blob_id = req_json['uri'].removeprefix('/blob/')
            if blobs.add_waiter(blob_id, blob_done_cb):
                return jsonify({})
        print('append_data no uri')
        blob_id = blobs.create(blob_done_cb)
        print('append_data new blob id', blob_id)

        resp_json = { 'uri': '/blob/' + blob_id }
        return jsonify(resp_json)

    @app.route('/transactions/<transaction_id>/smtpMode',
               methods=['POST'])
    def smtp_mode(transaction_id):
        # XXX this should block until the transaction has received the
        # last data chunk
        t = resources.get_transaction(transaction_id)
        t.endpoint.set_durable()
        return Response()


    @app.route('/blob/<blob_id>', methods=['PUT'])
    def append_data_chunk(blob_id) -> Response:
        print(request, request.headers)

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



