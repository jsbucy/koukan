# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0

# gunicorn3 -b localhost:8002 --access-logfile - --log-level debug
#   'examples.receiver.receiver:create_app()'

from typing import Dict, List, Optional, Tuple
import logging
from tempfile import TemporaryFile
import json
from io import IOBase

from flask import (
    Flask,
    Request as FlaskRequest,
    Response as FlaskResponse,
    jsonify,
    make_response,
    request )

from werkzeug.datastructures import ContentRange
import werkzeug.http

import secrets
import copy

class Transaction:
    CHUNK_SIZE=1048576

    tx_json : dict
    file : Optional[IOBase] = None
    message_json : Optional[dict] = None

    blobs : Dict[str, IOBase]

    def __init__(self, tx_json):
        self.blobs = {}

        logging.debug('Tx.init %s', tx_json)
        self.tx_json = copy.copy(tx_json)
        if 'smtp_meta' in self.tx_json:
            del self.tx_json['smtp_meta']
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
        logging.debug('Tx.get %s', json_out)
        return json_out

    def _check_blobs(self, part_json) -> bool:
        logging.debug('check_blobs %s', part_json)
        if 'parts' not in part_json:
            if 'blob_rest_id' not in part_json:
                return
            blob_id = part_json['blob_rest_id']
            if blob_id.startswith('/'):
                return False  # this doesn't support reuse
            logging.debug('check_blobs %s', blob_id)
            if not self.create_tx_body(
                    upload_chunked=True,
                    tx_body=False,
                    blob_id=blob_id):
                return False
            return True

        for part_i in part_json['parts']:
            if not self._check_blobs(part_i):
                return False
        return True


    def update_message_builder(self, message_json):
        logging.debug('Tx.update_message_json %s', message_json)
        self.message_json = message_json
        if not self._check_blobs(self.message_json['parts']):
            return FlaskResponse(400, 'bad blob in message builder json')

    def create_tx_body(self, upload_chunked : bool,
                       tx_body : bool = False,
                       blob_id : Optional[str] = None,
                       stream = None
                       ) -> Optional[str]:  # blob-id if !tx_body
        assert upload_chunked or stream
        if tx_body:
            if self.file is not None:
                return None  # bad
            file = TemporaryFile('w+b')
            assert isinstance(file, IOBase)
            self.file = file
        else:
            logging.debug('create_tx_body %s %s', self.blobs, blob_id)
            if blob_id in self.blobs:
                return None  # bad
            file = TemporaryFile('w+b')
            self.blobs[blob_id] = file
        if stream is None:
            return
        if upload_chunked:
            return blob_id
        while b := stream.read(self.CHUNK_SIZE):
            file.write(b)
        if tx_body:
            self.tx_json['data_response'] = {'code': 250, 'message': 'ok'}
            self.log()
        file.seek(0)
        #logging.debug(file.read())
        return blob_id

    def _put_blob(self, range : ContentRange, stream,
                  file
                  ) -> Tuple[int, ContentRange]:
        if range.start != file.tell():
            return 412, ContentRange('bytes', 0, file.tell())
        while b := stream.read(self.CHUNK_SIZE):
            file.write(b)
        if file.tell() == range.length:
            file.seek(0)
            logging.debug(file.read())
        return 200, ContentRange('bytes', 0, file.tell(), range.length)

    def put_tx_body(self, range : ContentRange, stream
                    ) -> Tuple[int, ContentRange]:
        logging.debug('put_tx_body %s', range)
        resp = self._put_blob(range, stream, self.file)
        if range.length is not None and self.file.tell() == range.length:
            self.tx_json['data_response'] = {'code': 250, 'message': 'ok'}
            self.log()
        return resp

    def put_blob(self, blob_id, range : ContentRange, stream
                    ) -> Tuple[int, ContentRange]:
        logging.debug('put_blob %s %s', blob_id, range)
        #parsed_blob_id = self._parse_blob_id(blob_id)
        #assert parsed_blob_id < len(self.blobs)
        return self._put_blob(range, stream, self.blobs[blob_id])

    def log(self):
        logging.debug('received tx %s', self.tx_json)
        logging.debug('received parsed %s',
                      self.message_json if self.message_json else None)
        self.file.seek(0)
        logging.debug('received body %s', self.file.read())
        logging.debug('received blob %s', self.blobs.keys())
        for blob_id,file in self.blobs.items():
            file.seek(0)
            logging.debug('received blob %s %s', blob_id, file.read())

    def cancel(self):
        return FlaskResponse()


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
        tx = self.transactions.get(tx_rest_id, None)
        assert tx is not None
        return tx.get_json()

    def update_tx_message_builder(self, tx_rest_id : str, message_json):
        tx = self.transactions.get(tx_rest_id, None)
        assert tx is not None
        return tx.update_message_builder(message_json)

    def create_tx_body(self, tx_rest_id : str,
                       upload_chunked : bool,
                       tx_body : bool = False,
                       blob_id : Optional[str] = None,
                       stream = None):
        logging.debug('create_tx_body %s', blob_id)
        return self.transactions[tx_rest_id].create_tx_body(
            upload_chunked, tx_body, blob_id, stream)

    def put_tx_body(self, tx_rest_id: str, range : ContentRange, stream):
        status, range = self.transactions[tx_rest_id].put_tx_body(range, stream)
        response = FlaskResponse(status=status)
        response.headers.set('content-range', range)
        return response

    def put_blob(self, tx_rest_id: str, blob_id : str,
                 range : ContentRange, stream):
        status, range = self.transactions[tx_rest_id].put_blob(
            blob_id, range, stream)
        response = FlaskResponse(status=status)
        response.headers.set('content-range', range)
        return response


    def cancel_tx(self, tx_rest_id : str):
        return self.transactions[tx_rest_id].cancel()

def create_app(receiver = None):
    app = Flask(__name__)

    if receiver is None:
        receiver = Receiver()

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')

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

    @app.route('/transactions/<tx_rest_id>/message_builder', methods=['POST'])
    def update_message_builder(tx_rest_id) -> FlaskResponse:
        receiver.update_tx_message_builder(tx_rest_id, request.json)
        return FlaskResponse(status = 200)

    @app.route('/transactions/<tx_rest_id>/body', methods=['POST'])
    def create_tx_body(tx_rest_id) -> FlaskResponse:
        receiver.create_tx_body(
            tx_rest_id,
            request.args.get('upload', None),
            tx_body=True,
            stream=request.stream)
        return FlaskResponse(status=201)

    @app.route('/transactions/<tx_rest_id>/body', methods=['PUT'])
    def put_tx_body(tx_rest_id) -> FlaskResponse:
        range = werkzeug.http.parse_content_range_header(
            request.headers.get('content-range'))
        return receiver.put_tx_body(tx_rest_id, range, request.stream)

    @app.route('/transactions/<tx_rest_id>/blob/<blob_id>', methods=['PUT'])
    def put_blob(tx_rest_id, blob_id) -> FlaskResponse:
        range = werkzeug.http.parse_content_range_header(
            request.headers.get('content-range'))
        if range is None:
            content_length = int(request.headers.get('content-length'))
            range = ContentRange('bytes', 0, content_length, content_length)
        return receiver.put_blob(tx_rest_id, blob_id, range, request.stream)


    @app.route('/transactions/<tx_rest_id>/cancel', methods=['POST'])
    def cancel_tx(tx_rest_id) -> FlaskResponse:
        return receiver.cancel_tx(tx_rest_id)

    return app
