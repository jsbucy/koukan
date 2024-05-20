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

    @app.route('/blob', methods=['POST'])
    def create_blob() -> FlaskResponse:
        handler = handler_factory.create_blob()
        return handler.create_blob(request)

    @app.route('/blob/<blob_rest_id>', methods=['PUT'])
    def append_data_chunk(blob_rest_id) -> FlaskResponse:
        handler = handler_factory.get_blob(blob_rest_id)
        return handler.put_blob(request)

    return app



