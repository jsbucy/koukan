# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0

# this keeps the inflight transaction state in-process so it's not
# compatible with multiple workers
# hypercorn -b localhost:8002 -w0 \
#   'examples.receiver.fastapi_receiver:create_app(path="/tmp/my_messages")'

from typing import Union
import logging

from fastapi import (
    FastAPI,
    Request as FastApiRequest,
    Response as FastApiResponse )
from fastapi.responses import JSONResponse as FastApiJsonResponse

from examples.receiver.receiver import Receiver

def create_app(receiver = None, path = None):
    app = FastAPI()

    if receiver is None:
        receiver = Receiver(path)

    @app.post('/transactions')
    async def create_transaction(request : FastApiRequest) -> FastApiResponse:
        req_json = await request.json()
        tx_id, tx_json = receiver.create_tx(req_json)
        return FastApiJsonResponse(
            status_code=201,
            headers={'location': '/transactions/' + tx_id},
            content=tx_json)

    @app.get('/transactions/{tx_rest_id}')
    async def get_transaction(tx_rest_id : str,
                              request : FastApiRequest) -> FastApiResponse:
        tx_json = receiver.get_tx(tx_rest_id)
        return FastApiJsonResponse(status_code=200, content=tx_json)

    @app.post('/transactions/{tx_rest_id}/message_builder')
    async def update_message_builder(tx_rest_id : str, request : FastApiRequest,
                                     ) -> FastApiResponse:
        builder_json = await request.json()
        receiver.update_tx_message_builder(tx_rest_id, builder_json)
        return FastApiResponse()

    @app.post('/transactions/{tx_rest_id}/body')
    async def create_tx_body(tx_rest_id : str,
                             request : FastApiRequest,
                             upload : Union[str, None] = None
                             ) -> FastApiResponse:
        await receiver.create_tx_body_async(tx_rest_id, request.stream())
        return FastApiResponse(status_code=201)

    @app.put('/transactions/{tx_rest_id}/blob/{blob_id}')
    async def put_blob(tx_rest_id : str, blob_id : str,
                       request : FastApiRequest) -> FastApiResponse:
        code = await receiver.put_blob_async(
            tx_rest_id, blob_id, request.stream())
        return FastApiResponse(status_code=code)

    @app.post('/transactions/{tx_rest_id}/cancel')
    async def cancel_tx(tx_rest_id) -> FastApiResponse:
        receiver.cancel_tx(tx_rest_id)
        return FastApiResponse()


    return app
