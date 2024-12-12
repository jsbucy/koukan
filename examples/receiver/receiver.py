# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0

# gunicorn3 -b localhost:8002 --access-logfile - --log-level debug
#   'examples.receiver.receiver:create_app()'

from typing import Dict, List, Optional, Tuple
import logging
from tempfile import TemporaryFile
import json
from io import IOBase
import os

from flask import (
    Flask,
    Request as FlaskRequest,
    Response as FlaskResponse,
    jsonify,
    make_response,
    request )

import secrets
import copy

class Transaction:
    CHUNK_SIZE=1048576

    tx_json : dict
    body_file : Optional[IOBase] = None
    message_json : Optional[dict] = None

    blobs : Dict[str, IOBase]
    close_files : bool

    def __init__(self, tx_json, close_files):
        self.blobs = {}

        logging.debug('Tx.init %s', tx_json)
        self.tx_json = copy.copy(tx_json)
        if 'smtp_meta' in self.tx_json:
            del self.tx_json['smtp_meta']
        self.tx_json['mail_response'] = {'code': 250, 'message': 'ok'}
        self.tx_json['rcpt_response'] = [{'code': 250, 'message': 'ok'}]
        self.close_files = close_files

    def get_json(self):
        json_out = copy.copy(self.tx_json)
        if 'mail_from' in json_out:
            json_out['mail_from'] = {}
        if 'rcpt_to' in json_out:
            json_out['rcpt_to'] = [{} for x in self.tx_json['rcpt_to']]
        if self.body_file:
            json_out['body'] = {}
        logging.debug('Tx.get %s', json_out)
        return json_out

    def _check_blobs(self, part_json) -> bool:
        logging.debug('check_blobs %s', part_json)
        if 'parts' not in part_json:
            if 'blob_rest_id' not in part_json:
                return True
            blob_id = part_json['blob_rest_id']
            if blob_id.startswith('/'):
                return False  # this doesn't support reuse
            logging.debug('check_blobs %s', blob_id)
            if blob_id in self.blobs:
                return None  # bad
            file = TemporaryFile('w+b')
            self.blobs[blob_id] = file
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

    def create_tx_body(self, stream : IOBase):
        if self.body_file is not None:
            return None  # bad
        file = TemporaryFile('w+b')
        assert isinstance(file, IOBase)
        self.body_file = file
        while b := stream.read(self.CHUNK_SIZE):
            file.write(b)
        self.tx_json['data_response'] = {'code': 250, 'message': 'ok'}
        self.log()

    def _put_blob(self, stream : IOBase, file) -> int:
        while b := stream.read(self.CHUNK_SIZE):
            file.write(b)
        return 200

    def put_blob(self, blob_id, stream : IOBase) -> int:
        logging.debug('put_blob %s', blob_id)
        return self._put_blob(stream, self.blobs[blob_id])

    def _file_size(self, file):
        file.seek(-1, os.SEEK_END)
        return file.tell() + 1

    def log(self):
        logging.debug('received tx %s', self.tx_json)
        logging.debug('received parsed %s',
                      self.message_json if self.message_json else None)
        logging.debug('received body %d bytes', self._file_size(self.body_file))

        logging.debug('received blobs %s', self.blobs.keys())
        for blob_id, blob_file in self.blobs.items():
            logging.debug('received blob %s %d bytes',
                          blob_id, self._file_size(blob_file))

        if self.close_files:
            for f in [ self.body_file ] + [f for f in self.blobs.values()]:
                f.close()

    def cancel(self):
        return FlaskResponse()


class Receiver:
    transactions : dict[str, Transaction]
    close_files : bool

    def __init__(self, close_files=False):
        self.transactions = {}
        self.close_files = close_files

    def create_tx(self, tx_json) -> Tuple[str,dict]:
        tx_id = secrets.token_urlsafe()
        tx = Transaction(tx_json, self.close_files)
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

    def create_tx_body(self,
                       tx_rest_id : str,
                       stream : IOBase):
        return self.transactions[tx_rest_id].create_tx_body(stream)

    def put_blob(self, tx_rest_id: str, blob_id : str, stream):
        status = self.transactions[tx_rest_id].put_blob(blob_id, stream)
        return FlaskResponse(status=status)

    def cancel_tx(self, tx_rest_id : str):
        return self.transactions[tx_rest_id].cancel()

def create_app(receiver = None):
    app = Flask(__name__)

    if receiver is None:
        receiver = Receiver()

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s [%(thread)d] '
                        '%(filename)s:%(lineno)d %(message)s')

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
        receiver.create_tx_body(tx_rest_id, request.stream)
        return FlaskResponse(status=201)

    @app.route('/transactions/<tx_rest_id>/blob/<blob_id>', methods=['PUT'])
    def put_blob(tx_rest_id, blob_id) -> FlaskResponse:
        return receiver.put_blob(tx_rest_id, blob_id, request.stream)

    @app.route('/transactions/<tx_rest_id>/cancel', methods=['POST'])
    def cancel_tx(tx_rest_id) -> FlaskResponse:
        return receiver.cancel_tx(tx_rest_id)

    return app
