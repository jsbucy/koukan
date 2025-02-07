# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0

# this keeps the inflight transaction state in-process so it's not
# compatible with multiple workers
# gunicorn3 -b localhost:8002 --access-logfile -w1 - --log-level debug
#   'examples.receiver.flask_receiver:create_app(path='/tmp/my_messages')'

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
        if err := receiver.update_tx_message_builder(tx_rest_id, request.json):
            code, msg = err
            return FlaskResponse(status=code, response=msg)
        return FlaskResponse()

    @app.route('/transactions/<tx_rest_id>/body', methods=['POST'])
    def create_tx_body(tx_rest_id) -> FlaskResponse:
        receiver.create_tx_body(tx_rest_id, request.stream)
        return FlaskResponse(status=201)

    @app.route('/transactions/<tx_rest_id>/blob/<blob_id>', methods=['PUT'])
    def put_blob(tx_rest_id, blob_id) -> FlaskResponse:
        code = receiver.put_blob(tx_rest_id, blob_id, request.stream)
        return FlaskResponse(status=code)

    @app.route('/transactions/<tx_rest_id>/cancel', methods=['POST'])
    def cancel_tx(tx_rest_id) -> FlaskResponse:
        receiver.cancel_tx(tx_rest_id)
        return FlaskResponse()

    return app
