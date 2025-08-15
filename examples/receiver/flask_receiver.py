# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0

# this keeps the inflight transaction state in-process so it's not
# compatible with multiple workers
# gunicorn3 -b localhost:8002 --access-logfile -w1 - --log-level debug
#   'examples.receiver.flask_receiver:create_app(path="/tmp/my_messages")'

import logging
import json

from flask import (
    Flask,
    Request as FlaskRequest,
    Response as FlaskResponse,
    jsonify,
    make_response,
    request )

from examples.receiver.receiver import Receiver

def create_app(receiver = None, path = None):
    app = Flask(__name__)

    if receiver is None:
        receiver = Receiver(path)

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s [%(thread)d] '
                        '%(filename)s:%(lineno)d %(message)s')

    @app.route('/transactions', methods=['POST'])
    def create_transaction() -> FlaskResponse:
        tx_id, tx_json, etag = receiver.create_tx(request.json)
        return make_response((tx_json, 201, [
            ('location', '/transactions/' + tx_id),
            ('etag', etag)]))

    @app.route('/transactions/<tx_rest_id>', methods=['GET'])
    def get_transaction(tx_rest_id) -> FlaskResponse:
        json, etag = receiver.get_tx(tx_rest_id)
        return make_response(json, 200, [('etag', etag)])

    @app.route('/transactions/<tx_rest_id>/message_builder', methods=['POST'])
    def update_message_builder(tx_rest_id) -> FlaskResponse:
        if err := receiver.update_tx_message_builder(tx_rest_id, request.json):
            code, msg = err
            return FlaskResponse(status=code, response=msg)
        return FlaskResponse()

    @app.route('/transactions/<tx_rest_id>/body', methods=['PUT'])
    def create_tx_body(tx_rest_id) -> FlaskResponse:
        receiver.put_blob(
            tx_rest_id, request.headers, request.stream, tx_body=True)
        return FlaskResponse(status=200)

    @app.route('/transactions/<tx_rest_id>/blob/<blob_id>', methods=['PUT'])
    def put_blob(tx_rest_id, blob_id) -> FlaskResponse:
        code, msg = receiver.put_blob(
            tx_rest_id, request.headers, request.stream, blob_id=blob_id)
        return FlaskResponse(status=code, response=msg)

    @app.route('/transactions/<tx_rest_id>/cancel', methods=['POST'])
    def cancel_tx(tx_rest_id) -> FlaskResponse:
        receiver.cancel_tx(tx_rest_id)
        return FlaskResponse()

    return app
