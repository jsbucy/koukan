from typing import Optional, Union
import logging

from fastapi import (
    FastAPI,
    Request as FastApiRequest,
    Response as FastApiResponse )

from rest_service_handler import (
    Handler,
    HandlerFactory )

MAX_TIMEOUT=30

def create_app(handler_factory : HandlerFactory):
    app = FastAPI()

    @app.post('/transactions')
    async def create_transaction(request : FastApiRequest) -> FastApiResponse:
        req_json = await request.json()
        handler = handler_factory.create_tx(request.headers['host'])
        return await handler.handle_async(
            request, lambda: handler.create_tx(request, req_json=req_json))


    @app.patch('/transactions/{tx_rest_id}')
    async def update_transaction(tx_rest_id : str,
                                 request : FastApiRequest) -> FastApiResponse:
        req_json = await request.json()
        handler = handler_factory.get_tx(tx_rest_id)
        return await handler.handle_async(
            request, lambda: handler.patch_tx(request, req_json=req_json))

    @app.get('/transactions/{tx_rest_id}')
    async def get_transaction(tx_rest_id : str,
                              request : FastApiRequest) -> FastApiResponse:
        handler = handler_factory.get_tx(tx_rest_id)
        return await handler.get_tx_async(request)

    # ?upload=chunked
    # then body is json metadata (unimplemented)
    # else body stream
    @app.post('/transactions/{tx_rest_id}/body')
    async def create_tx_body(tx_rest_id : str,
                             request : FastApiRequest,
                             upload : Union[str, None] = None
                             ) -> FastApiResponse:
        logging.debug('fastapi_service.create_tx_body %s', request)
        chunked = (upload == "chunked")
        handler = handler_factory.get_tx(tx_rest_id)
        return await handler.create_blob_async(request, tx_body=True,
                                               req_upload=upload)

    # body stream
    @app.put('/transactions/{tx_rest_id}/body')
    async def put_tx_body(tx_rest_id : str,
                          request : FastApiRequest) -> FastApiResponse:
        logging.debug('fastapi_service.put_tx_body %s', request)
        handler = handler_factory.get_tx(tx_rest_id)

        return await handler.put_blob_async(request, tx_body=True)


    @app.post('/transactions/{tx_rest_id}/message_builder')
    async def set_message_builder(tx_rest_id : str,
                                  request : FastApiRequest) -> FastApiResponse:
        req_json = await request.json()
        logging.debug('rest_service.set_message_builder %s', request)
        handler = handler_factory.get_tx(tx_rest_id)
        return await handler.handle_async(
            request, lambda: handler.patch_tx(request, req_json=req_json,
                                              message_builder=True))

    @app.post('/transactions/{tx_rest_id}/cancel')
    async def cancel_tx(tx_rest_id : str, request : FastApiRequest
                        ) -> FastApiResponse:
        logging.debug('rest_service.cancel_tx %s', request)
        handler = handler_factory.get_tx(tx_rest_id)
        return await handler.handle_async(
            request, lambda: handler.cancel_tx(request))

    # ?upload=chunked
    # then body is json metadata (unimplemented)
    # else body stream
    @app.post('/transactions/{tx_rest_id}/blob')
    async def create_tx_blob(tx_rest_id : str,
                             request : FastApiRequest,
                             upload : Union[str, None] = None
                             ) -> FastApiResponse:
        logging.debug('fastapi_service.create_tx_blob %s', request)
        chunked = (upload == "chunked")
        handler = handler_factory.get_tx(tx_rest_id)
        resp = await handler.create_blob_async(
            request, tx_body=False, req_upload=upload)
        logging.debug('fastapi_service.create_tx_blob %s', resp)
        return resp

    # body stream
    @app.put('/transactions/{tx_rest_id}/blob/{blob_rest_id}')
    async def put_tx_blob(tx_rest_id : str, blob_rest_id : str,
                          request : FastApiRequest) -> FastApiResponse:
        logging.debug('fastapi_service.put_tx_blob %s', request)
        handler = handler_factory.get_tx(tx_rest_id)
        return await handler.put_blob_async(request, blob_rest_id=blob_rest_id)

    return app
