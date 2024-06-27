import logging

from flask import (
    Flask,
    Request as FlaskRequest,
    Response as FlaskResponse,
    request )

from rest_service_handler import (
    Handler,
    HandlerFactory )

MAX_TIMEOUT=30

def create_app(handler_factory : HandlerFactory):
    app = Flask(__name__)

    @app.route('/transactions', methods=['POST'])
    def create_transaction() -> FlaskResponse:
        handler = handler_factory.create_tx(request.headers['host'])
        return handler.create_tx(request)

    @app.route('/transactions/<tx_rest_id>', methods=['PATCH'])
    def update_transaction(tx_rest_id) -> FlaskResponse:
        handler = handler_factory.get_tx(tx_rest_id)
        return handler.patch_tx(request)

    @app.route('/transactions/<tx_rest_id>', methods=['GET'])
    def get_transaction(tx_rest_id) -> FlaskResponse:
        handler = handler_factory.get_tx(tx_rest_id)
        return handler.get_tx(request)

    @app.route('/transactions/<tx_rest_id>/body', methods=['POST'])
    def create_tx_body(tx_rest_id) -> FlaskResponse:
        logging.debug('rest_service.put_tx_body %s', request)
        handler = handler_factory.get_tx(tx_rest_id)
        return handler.create_blob(request, tx_body=True)

    @app.route('/transactions/<tx_rest_id>/body', methods=['PUT'])
    def put_tx_body(tx_rest_id) -> FlaskResponse:
        logging.debug('rest_service.put_tx_body %s', request)
        handler = handler_factory.get_tx(tx_rest_id)
        return handler.put_blob(request, tx_body=True)

    @app.route('/transactions/<tx_rest_id>/message_builder', methods=['POST'])
    def set_message_builder(tx_rest_id) -> FlaskResponse:
        logging.debug('rest_service.set_message_builder %s', request)
        handler = handler_factory.get_tx(tx_rest_id)
        return handler.patch_tx(request, message_builder=True)

    @app.route('/transactions/<tx_rest_id>/cancel', methods=['POST'])
    def cancel_tx(tx_rest_id) -> FlaskResponse:
        logging.debug('rest_service.set_message_builder %s', request)
        handler = handler_factory.get_tx(tx_rest_id)
        return handler.cancel_tx(request)

    @app.route('/transactions/<tx_rest_id>/blob', methods=['POST'])
    def create_tx_blob(tx_rest_id) -> FlaskResponse:
        logging.debug('rest_service.create_tx_blob %s', request)
        handler = handler_factory.get_tx(tx_rest_id)
        return handler.create_blob(request)

    @app.route('/transactions/<tx_rest_id>/blob/<blob_rest_id>',
               methods=['PUT'])
    def put_tx_blob(tx_rest_id, blob_rest_id) -> FlaskResponse:
        handler = handler_factory.get_tx(tx_rest_id)
        return handler.put_blob(request, blob_rest_id=blob_rest_id)
    return app
