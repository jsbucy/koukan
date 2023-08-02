from flask import Flask
from flask import Response
from flask import jsonify
from flask import request

from werkzeug.datastructures import ContentRange
import werkzeug.http

from blobs import Blob, BlobStorage

from typing import Any, Dict, Optional, List
from typing import Callable, Tuple

class Transaction:
    id : str
    chunk_id : int = None
    last : bool = False
    endpoint : Any
    final_status : Response = None

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


def create_app(endpoints : Callable[[str], "Endpoint"],
               blobs : BlobStorage):
    app = Flask(__name__)

    resources = RestResources()

    @app.route('/transactions', methods=['POST'])
    def start_transaction() -> Response:
        print(request, request.headers)

        t = Transaction()
        t.endpoint = endpoints(request.headers['host'])

        req_json = request.get_json()
        local_host = req_json.get('local_host', None)
        remote_host = req_json.get('remote_host', None)
        mail_from = req_json.get('mail_from', None)
        rcpt_to = req_json.get('rcpt_to', None)

        # local_host/remote_host ~
        # http x-forwarded-host/-for?

        resp = t.endpoint.start(
            local_host, remote_host,
            mail_from, transaction_esmtp=None,
            rcpt_to=rcpt_to, rcpt_esmtp=None)
        if resp.err():
            return {'final_status': resp.to_json()}

        resources.add_transaction(t)
        json_out = {
            'url': '/transactions/' + t.id
        }

        return jsonify(json_out)

    @app.route('/transactions/<transaction_id>',
               methods=['GET'])
    def get_transaction(transaction_id) -> Response:
        t = resources.get_transaction(transaction_id)
        if t.final_status is None:
            t.final_status = t.endpoint.get_status()

            # if 'wait_final_status' in request.args
            #   if not tx.final_status_event.wait(...)
            #     # 504 gateway timeout

        json_out = { 'final_status': t.final_status.to_json() }
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
        d : bytes = None
        if 'd' in req_json:
            d = req_json['d'].encode('utf-8')
            resp, blob_id = t.endpoint.append_data(d=d, blob_id=None, last=last)
            print(resp)
            return resp.to_json()

        if 'uri' in req_json and req_json['uri'] is not None:
            blob_id = req_json['uri'].removeprefix('/blob/')
            if blobs.add_waiter(
                    blob_id,
                    lambda blob_id, blob_len: t.endpoint.append_data(
                        d=None, blob_id=blob_id, last=last)):
                return jsonify({})
        print('append_data no uri')
        blob_id = blobs.create(
            lambda blob_id, blob_len: t.endpoint.append_data(
                d=None, blob_id=blob_id, last=last))
        print('append_data new blob id', blob_id)

        resp_json = { 'uri': '/blob/' + blob_id }
        return jsonify(resp_json)


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



