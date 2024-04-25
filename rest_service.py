from typing import Any, Dict, Optional, List
from typing import Callable, Tuple

from threading import Lock, Condition
import logging
import json

from flask import Flask
from flask import Response as FlaskResponse
from flask import jsonify
from flask import Request as FlaskRequest, request

from werkzeug.datastructures import ContentRange
import werkzeug.http

from blobs import Blob, BlobStorage
from blob import InlineBlob

from response import Response as MailResponse

from rest_service_handler import Handler, HandlerFactory

MAX_TIMEOUT=30

def create_app(handler_factory : HandlerFactory):
    app = Flask(__name__)

    def set_etag(resp, handler):
        etag = handler.etag()
        if etag is None:
            return
        logging.debug('rest service set_etag %s', etag)
        resp.set_etag(etag)

    def validate_etag(request, handler) -> Optional[FlaskResponse]:
        req_etag = request.headers.get('if-match', None)
        if req_etag is None:
            return None
        req_etag = req_etag.strip('"')
        handler_etag = handler.etag()
        if req_etag != handler_etag:
            logging.debug('etag mismatch req %s db %s', req_etag, handler_etag)
            return FlaskResponse(status=412, response=['version mismatch'])
        return None

    def _get_timeout(req) -> Tuple[Optional[int], FlaskResponse]:
        # https://datatracker.ietf.org/doc/id/draft-thomson-hybi-http-timeout-00.html
        if not (timeout_header := request.headers.get('request-timeout', None)):
            return None, None
        timeout = None
        try:
            timeout = min(int(timeout_header), MAX_TIMEOUT)
        except ValueError:
            return None, FlaskResponse(status=400, response=[
                'invalid request-timeout header'])
        return timeout, None

    @app.route('/transactions', methods=['POST'])
    def start_transaction() -> FlaskResponse:
        logging.info('rest service start_transaction %s %s',
                     request, request.headers)
        if not request.is_json:
            return FlaskResponse(status=400, response=['not json'])
        # TODO if request doesn't have remote_addr or is not from a
        # well-known/trusted peer (i.e. smtp gateway), set remote_addr to wsgi
        # environ REMOTE_ADDR or HTTP_X_FORWARDED_FOR
        handler = handler_factory.create_tx(request.headers['host'])
        if handler is None:
            return FlaskResponse(
                status=500,
                response=['internal error creating transaction'])
        timeout, err = _get_timeout(request)
        if err:
            return err
        rest_resp : Optional[FlaskResponse] = handler.start(
            request.get_json(), timeout)
        if rest_resp is None:
            return FlaskResponse(status=500)
        if rest_resp.status_code > 299:
            return rest_resp
        tx_url = '/transactions/' + handler.tx_rest_id()
        rest_resp.status_code = 201
        rest_resp.headers.set('location', tx_url)
        set_etag(rest_resp, handler)

        return rest_resp

    @app.route('/transactions/<tx_rest_id>',
               methods=['PATCH'])
    def update_transaction(tx_rest_id) -> FlaskResponse:
        logging.info('rest service update_transaction %s %s',
                     request, request.headers)
        handler = handler_factory.get_tx(tx_rest_id)
        if handler is None:
            return FlaskResponse(status=404, response=['transaction not found'])
        if (err := validate_etag(request, handler)) is not None:
            return err
        timeout, err = _get_timeout(request)
        if err:
            return err
        rest_resp = handler.patch(request.get_json(), timeout)
        set_etag(rest_resp, handler)
        return rest_resp


    @app.route('/transactions/<tx_rest_id>',
               methods=['GET'])
    def get_transaction(tx_rest_id) -> FlaskResponse:
        handler = handler_factory.get_tx(tx_rest_id)
        if handler is None:
            return FlaskResponse(status=404, response=['transaction not found'])
        timeout, err = _get_timeout(request)
        if err:
            return err

        rest_resp = handler.get({}, timeout=timeout)
        set_etag(rest_resp, handler)
        return rest_resp

    def _get_range(request) -> Tuple[Optional[FlaskResponse],
                                     Optional[ContentRange]]:
        if range_header := request.headers.get('content-range', None):
            range = werkzeug.http.parse_content_range_header(
                request.headers.get('content-range'))
            logging.info('put_blob content-range: %s', range)
            if not range or range.units != 'bytes':
                return FlaskResponse(400, 'bad range'), None
            # no idea if underlying stack enforces this
            assert(range.stop - range.start == request.content_length)
            return None, range
        else:
            return None, ContentRange(
                'bytes', 0, request.content_length, request.content_length)

    @app.route('/blob', methods=['POST'])
    def create_blob() -> FlaskResponse:
        logging.info("rest service create_blob %s %s",
                     request, request.headers)
        handler = handler_factory.create_blob()
        if handler is None:
            return FlaskResponse(status=500,
                                 response=['could not create blob handler'])
        rest_resp = handler.create_blob(request)
        if rest_resp.status_code != 201:
            return rest_resp

        if not request.args:
            range_err, range = _get_range(request)
            if range_err:
                return range_err
            rest_resp = handler.put_blob(request, range)
        elif request.args.keys() != {'upload'} or (
                request.args['upload'] != 'chunked'):
            return FlaskResponse(status=400, response=['bad query'])
        elif request.data:
            return FlaskResponse(
                status=400, response=['unimplemented metadata upload'])

        blob_url = '/blob/' + handler.blob_rest_id()
        rest_resp.status_code = 201
        rest_resp.headers.set('location', blob_url)

        return rest_resp

    @app.route('/blob/<blob_rest_id>', methods=['PUT'])
    def append_data_chunk(blob_rest_id) -> FlaskResponse:
        logging.info("rest service append_data_chunk %s %s",
                     request, request.headers)

        handler = handler_factory.get_blob(blob_rest_id)
        if handler is None:
            return FlaskResponse(status=404, response=['blob not found'])

        # no idea if underlying stack enforces this
        assert(len(request.data) == request.content_length)
        range_err, range = _get_range(request)
        if range_err:
            return range_err

        return handler.put_blob(request, range)

    return app



