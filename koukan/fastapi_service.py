# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional, Union
import logging
from functools import partial

from fastapi import (
    FastAPI,
    Request as FastApiRequest,
    Response as FastApiResponse )

from koukan.rest_service_handler import (
    Handler,
    HandlerFactory )

MAX_TIMEOUT=30

def create_app(handler_factory : HandlerFactory):
    app = FastAPI()

    @app.post('/senders/{sender}/transactions')
    async def create_transaction(
            sender : str,
            request : FastApiRequest) -> FastApiResponse:
        try:
            req_json = await request.json()
            handler = handler_factory.create_tx(
                request.headers['host'], sender, req_json.get('tag', None))
            return await handler.handle_async(
                request, partial(handler.create_tx, request, req_json=req_json))
        except Exception as e:
            logging.exception('create_transaction')
            return FastApiResponse(status_code=500)


    @app.patch('/transactions/{tx_rest_id}')
    async def update_transaction(tx_rest_id : str,
                                 request : FastApiRequest) -> FastApiResponse:
        if request.headers.get('content-type', '') == 'application/json':
            req_json = await request.json()
        else:
            body = await request.body()
            if bool(body):
                return FastApiResponse(status_code=400)
            req_json = None
        handler = handler_factory.get_tx(tx_rest_id)
        return await handler.handle_async(
            request, partial(handler.patch_tx, request, req_json=req_json))

    @app.get('/transactions/{tx_rest_id}')
    async def get_transaction(tx_rest_id : str,
                              request : FastApiRequest) -> FastApiResponse:
        handler = handler_factory.get_tx(tx_rest_id)
        try:
            return await handler.get_tx_async(request)
        except Exception:
            logging.exception('get_transaction')
            return FastApiResponse(500)

    # body stream
    @app.put('/transactions/{tx_rest_id}/body')
    async def put_tx_body(tx_rest_id : str,
                          request : FastApiRequest) -> FastApiResponse:
        logging.debug('fastapi_service.put_tx_body %s', request)
        handler = handler_factory.get_tx(tx_rest_id)

        return await handler.put_blob_async(request, tx_body=True)

    @app.post('/transactions/{tx_rest_id}/cancel')
    async def cancel_tx(tx_rest_id : str, request : FastApiRequest
                        ) -> FastApiResponse:
        logging.debug('rest_service.cancel_tx %s', request)
        handler = handler_factory.get_tx(tx_rest_id)
        return await handler.handle_async(
            request, partial(handler.cancel_tx, request))

    # body stream
    @app.put('/transactions/{tx_rest_id}/blob/{blob_rest_id}')
    async def put_tx_blob(tx_rest_id : str, blob_rest_id : str,
                          request : FastApiRequest) -> FastApiResponse:
        logging.debug('fastapi_service.put_tx_blob %s', request)
        handler = handler_factory.get_tx(tx_rest_id)
        return await handler.put_blob_async(request, blob_rest_id=blob_rest_id)

    return app
