from flask import Flask
from flask import Response as FlaskResponse
from flask import jsonify
from flask import Request as FlaskRequest, request

from werkzeug.datastructures import ContentRange
import werkzeug.http

from blobs import Blob, BlobStorage
from blob import InlineBlob
from tags import Tag

from threading import Lock, Condition

import logging

from typing import Any, Dict, Optional, List
from typing import Callable, Tuple

from response import Response as MailResponse

from rest_service_handler import Handler, HandlerFactory


def create_app(handler_factory : HandlerFactory):
    app = Flask(__name__)

    @app.route('/transactions', methods=['POST'])
    def start_transaction() -> FlaskResponse:
        print(request, request.headers)
        if not request.is_json:
            return FlaskResponse(status=400, response=['not json'])
        handler = handler_factory.create_tx(request.headers['host'])
        if handler is None:
            return FlaskResponse(
                status=500,
                response=['internal error creating transaction'])
        #resp =
        handler.start(request.get_json())

        json_out = {
            'url': '/transactions/' + handler.tx_rest_id()
        }
        # xxx this could be sync success or something like a syntax error
        #if resp is not None:
        #    json_out['start_response'] = resp.to_json()

        return jsonify(json_out)

    @app.route('/transactions/<tx_rest_id>',
               methods=['GET'])
    def get_transaction(tx_rest_id) -> FlaskResponse:
        handler = handler_factory.get_tx(tx_rest_id)
        if handler is None:
            return FlaskResponse(status=404, response=['transaction not found'])
        return handler.get({})  # xxx request.get_json())

    @app.route('/transactions/<tx_rest_id>/appendData',
               methods=['POST'])
    def append_data(tx_rest_id):
        if not request.is_json:
            return FlaskResponse(status=400, response=['not json'])

        handler = handler_factory.get_tx(tx_rest_id)
        if handler is None:
            return FlaskResponse(status=404, response=['transaction not found'])

        logging.info("rest service append_data %s %s %s",
                     request, request.headers, request.get_json())
        rest_resp = handler.append(request.get_json())
        logging.info('rest service append_data %s', rest_resp)
        return rest_resp

    @app.route('/transactions/<tx_rest_id>/smtpMode',
               methods=['POST'])
    def smtp_mode(tx_rest_id):
        logging.info("rest service smtp_mode %s %s",
                     request, request.headers)
        # TODO this should take a parameter in the json whether or not
        # to emit a bounce. smtp gw wants this; first-class rest
        # clients that are willing to poll the lro may not.
        handler = handler_factory.get_tx(tx_rest_id)
        if handler is None:
            return FlaskResponse(status=404, response=['transaction not found'])
        rest_resp = handler.set_durable({})
        logging.info("rest service smtp_mode %s", rest_resp)
        return rest_resp

    @app.route('/blob/<blob_rest_id>', methods=['PUT'])
    def append_data_chunk(blob_rest_id) -> FlaskResponse:
        logging.info("rest service append_data_chunk %s %s",
                     request, request.headers)

        # no idea if underlying stack enforces this
        assert(len(request.data) == request.content_length)
        range = None
        range_in_headers = 'content-range' in request.headers
        if range_in_headers:
            range = werkzeug.http.parse_content_range_header(
                request.headers.get('content-range'))
            logging.info('put_blob content-range: %s', range)
            if not range or range.units != 'bytes':
                return FlaskResponse(400, 'bad range')
            # no idea if underlying stack enforces this
            assert(range.stop - range.start == request.content_length)
        else:
           range = ContentRange('bytes', 0, request.content_length,
                                request.content_length)

        handler = handler_factory.get_blob(blob_rest_id)
        if handler is None:
            return FlaskResponse(status=404, response=['blob not found'])
        return handler.put_blob(request, range, range_in_headers)

    return app



