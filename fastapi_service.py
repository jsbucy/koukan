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
        return handler.create_tx(request, req_json)

    @app.patch('/transactions/{tx_rest_id}')
    async def update_transaction(tx_rest_id : str,
                                 request : FastApiRequest) -> FastApiResponse:
        req_json = await request.json()
        handler = handler_factory.get_tx(tx_rest_id)
        return handler.patch_tx(request, req_json)

    @app.get('/transactions/{tx_rest_id}')
    async def get_transaction(tx_rest_id : str,
                              request : FastApiRequest) -> FastApiResponse:
        handler = handler_factory.get_tx(tx_rest_id)
        return handler.get_tx(request)

    # ?upload=chunked
    # then body is json metadata (unimplemented)
    # else body stream
    @app.post('/transactions/{tx_rest_id}/body')
    async def create_tx_body(tx_rest_id : str,
                             request : FastApiRequest) -> FastApiResponse:
        logging.debug('rest_service.put_tx_body %s', request)
        handler = handler_factory.get_tx(tx_rest_id)
        return handler.create_blob(request, tx_body=True)

    # body stream
    @app.put('/transactions/{tx_rest_id}/body')
    async def put_tx_body(tx_rest_id : str,
                          request : FastApiRequest) -> FastApiResponse:
        logging.debug('rest_service.put_tx_body %s', request)
        handler = handler_factory.get_tx(tx_rest_id)

        # # start handler on executor

        # b = bytes()
        # async for chunk in request.stream():
        #     while chunk:
        #         to_go = CHUNK_SIZE - len(b)
        #         count = min(to_go, len(chunk))
        #         b += chunk[0:count]
        #         if len(b) < CHUNK_SIZE:
        #             continue
        #         # send to handler and wait until done
        #         b = bytes()
        #         chunk = chunk[count:]
        # if b:
        #     # send
        #     pass

        # # get final response from handler

        return handler.put_blob(request, tx_body=True)



    @app.post('/transactions/{tx_rest_id}/message_builder')
    async def set_message_builder(tx_rest_id : str,
                                  request : FastApiRequest) -> FastApiResponse:
        req_json = await request.json()
        logging.debug('rest_service.set_message_builder %s', request)
        handler = handler_factory.get_tx(tx_rest_id)
        return handler.patch_tx(request, message_builder=True,
                                req_json=req_json)

    @app.post('/transactions/{tx_rest_id}/cancel')
    async def cancel_tx(tx_rest_id : str, request : FastApiRequest
                        ) -> FastApiResponse:
        logging.debug('rest_service.cancel_tx %s', request)
        handler = handler_factory.get_tx(tx_rest_id)
        return handler.cancel_tx(request)

    # ?upload=chunked
    # then body is json metadata (unimplemented)
    # else body stream
    @app.post('/transactions/{tx_rest_id}/blob')
    async def create_tx_blob(tx_rest_id : str, request : FastApiRequest
                             ) -> FastApiResponse:
        logging.debug('rest_service.create_tx_blob %s', request)
        handler = handler_factory.get_tx(tx_rest_id)
        return handler.create_blob(request)

    # body stream
    @app.put('/transactions/{tx_rest_id}/blob/{blob_rest_id}')
    async def put_tx_blob(tx_rest_id : str, blob_rest_id : str,
                          request : FastApiRequest) -> FastApiResponse:
        handler = handler_factory.get_tx(tx_rest_id)
        return handler.put_blob(request, blob_rest_id=blob_rest_id)

    return app
