# gunicorn3 -b localhost:8002 --access-logfile - --log-level debug
#   'examples.receiver.receiver:create_app()'

from typing import Optional, Tuple
import logging
from tempfile import TemporaryFile
import json

from flask import (
    Flask,
    Request as FlaskRequest,
    Response as FlaskResponse,
    jsonify,
    make_response,
    request )

from werkzeug.datastructures import ContentRange

import secrets
import copy

class Transaction:
    CHUNK_SIZE=1048576

    tx_json : dict
    file : Optional[TemporaryFile] = None

    def __init__(self, tx_json):
        self.tx_json = tx_json
        self.tx_json['mail_response'] = {'code': 250, 'message': 'ok'}
        self.tx_json['rcpt_response'] = [{'code': 250, 'message': 'ok'}]

    def get_json(self):
        json_out = copy.copy(self.tx_json)
        if 'mail_from' in json_out:
            json_out['mail_from'] = {}
        if 'rcpt_to' in json_out:
            json_out['rcpt_to'] = [{} for x in self.tx_json['rcpt_to']]
        if self.file:
            json_out['body'] = {}
        return json_out

    def create_tx_body(self, upload_chunked : bool, stream):
        assert upload_chunked or stream
        self.file = TemporaryFile('w+b')
        if stream is None:
            return
        while b := stream.read(self.CHUNK_SIZE):
            self.file.write(b)
        self.tx_json['data_response'] = {'code': 250, 'message': 'ok'}
        self.log()

    def put_tx_body(self, range : ContentRange, stream
                    ) -> Tuple[int, ContentRange]:
        if range.start != self.file.tell():
            return 412, ContentRange('bytes', 0, self.file.tell())
        while b := stream.read(self.CHUNK_SIZE):
            self.file.write(b)
        if self.file.tell == range.length:
            self.tx_json['data_response'] = {'code': 250, 'message': 'ok'}
            self.log()
        return 200, ContentRange('bytes', 0, self.file.tell(), range.length)

    def log(self):
        logging.debug('received %s', self.tx_json)
        self.file.seek(0)
        logging.debug(self.file.read())

    def cancel_tx(self, tx_rest_id : str):
        pass


class Receiver:
    transactions : dict[str, Transaction]

    def __init__(self):
        self.transactions = {}

    def create_tx(self, tx_json) -> Tuple[str,dict]:
        tx_id = secrets.token_urlsafe()
        tx = Transaction(tx_json)
        self.transactions[tx_id] = tx
        return tx_id, tx.get_json()

    def get_tx(self, tx_rest_id : str):
        return self.transactions.get(tx_rest_id, None).get_json()

    def create_tx_body(self, tx_rest_id : str, upload_chunked : bool, stream):
        return self.transactions[tx_rest_id].create_tx_body(
            upload_chunked ,stream)

    def put_tx_body(self, tx_rest_id: str, range : ContentRange, stream):
        status, range = self.transactions[tx_rest_id].put_tx_body(range, stream)
        response = FlaskResponse(status=status)
        response.headers.set('content-range', range)
        return response

    def cancel_tx(self, tx_rest_id : str):
        return self.transactions[tx_rest_id].cancel()

def create_app():
    app = Flask(__name__)

    receiver = Receiver()

    @app.route('/transactions', methods=['POST'])
    def create_transaction() -> FlaskResponse:
        tx_id, tx_json = receiver.create_tx(request.json)
        resp = FlaskResponse(status=201)
        resp.headers.set('location', '/transactions/' + tx_id)
        resp.content_type = 'application/json'
        resp.set_data(json.dumps(tx_json))
        return resp

    @app.route('/transactions/<tx_rest_id>', methods=['GET'])
    def get_transaction(tx_rest_id) -> FlaskResponse:
        return jsonify(receiver.get_tx(tx_rest_id))

    @app.route('/transactions/<tx_rest_id>/body', methods=['POST'])
    def create_tx_body(tx_rest_id) -> FlaskResponse:
        receiver.create_tx_body(
            tx_rest_id,
            request.args.get('upload', None),
            request.stream)
        return FlaskResponse(status=201)

    @app.route('/transactions/<tx_rest_id>/body', methods=['PUT'])
    def put_tx_body(tx_rest_id) -> FlaskResponse:
        range = werkzeug.http.parse_content_range_header(
            request.headers.get('content-range'))
        return receiver.put_tx_body(tx_rest_id, range, request.stream)

    @app.route('/transactions/<tx_rest_id>/cancel', methods=['POST'])
    def cancel_tx(tx_rest_id) -> FlaskResponse:
        return handler.cancel_tx(tx_rest_id)

    return app
