# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0

# this keeps the inflight transaction state in-process so it's not
# compatible with multiple workers
# gunicorn3 -b localhost:8002 --access-logfile - -w1 --log-level debug
#   'examples.receiver.flask_receiver:create_app(path='/tmp/my_messages')'

import logging

from flask import (
    Flask,
    Request as FlaskRequest,
    Response as FlaskResponse,
    make_response,
    request,
    url_for)

from examples.receiver.receiver import Receiver

def create_app(receiver = None, path = None):
    app = Flask(__name__)

    if receiver is None:
        kwargs = {}
        if path:
            kwargs['dir'] = path
        receiver = Receiver(**kwargs)

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s [%(thread)d] '
                        '%(filename)s:%(lineno)d %(message)s')

    @app.route('/transactions', methods=['POST'])
    def create_transaction():
        tx_url, tx_json, etag = receiver.create_tx(
            request.json,
            lambda tx_id: str(url_for(
                'get_transaction', tx_rest_id=tx_id, _external=True))
        )
        
        return tx_json, 201, [
            ('location', tx_url),
            ('etag', etag)]

    @app.route('/transactions/<tx_rest_id>', methods=['GET'])
    def get_transaction(tx_rest_id):
        tx_json, etag = receiver.get_tx(tx_rest_id)
        return tx_json, 200, [('etag', etag)]

    @app.route('/transactions/<tx_rest_id>/message_builder', methods=['POST'])
    def update_message_builder(tx_rest_id):
        err, res = receiver.update_tx_message_builder(tx_rest_id, request.json)
        if err:
            code, msg = err
            return FlaskResponse(status=code, response=msg)
        elif res is not None:
            tx_json, etag = res
            return tx_json, [('etag', etag)]
        else:
            assert False

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
