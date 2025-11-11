# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0

# this keeps the inflight transaction state in-process so it's not
# compatible with multiple workers
# uvicorn --log-level debug --host localhost --port 8002 --factory
#   'examples.receiver.fastapi_receiver:create_app'

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
        kwargs = {}
        if path:
            kwargs['dir'] = path
        receiver = Receiver(**kwargs)

    @app.post('/senders/{sender}/transactions')
    async def create_transaction(
            sender : str,
            request : FastApiRequest) -> FastApiResponse:
        try:
            req_json = await request.json()
            tx_url, tx_json, etag = receiver.create_tx(
                sender,
                req_json,
                lambda tx_id: str(request.url_for(
                    'get_transaction', tx_rest_id=tx_id)))
            return FastApiJsonResponse(
                status_code=201,
                headers={'location': tx_url, 'etag': etag},
                content=tx_json)
        except Exception as e:
            logging.debug(e)
            return FastApiResponse(status_code=500)

    @app.get('/transactions/{tx_rest_id}')
    async def get_transaction(tx_rest_id : str,
                              request : FastApiRequest) -> FastApiResponse:
        try:
            tx_json, etag = receiver.get_tx(tx_rest_id)
        except Exception as e:
            logging.exception('fastapi_receiver.get_transaction')
            return FastApiResponse(status_code=500)
        return FastApiJsonResponse(status_code=200, content=tx_json,
                                   headers={'etag': etag})

    @app.post('/transactions/{tx_rest_id}/message_builder')
    async def update_message_builder(tx_rest_id : str, request : FastApiRequest,
                                     ) -> FastApiResponse:
        builder_json = await request.json()
        
        err, res = receiver.update_tx_message_builder(tx_rest_id, builder_json)

        if err is not None:
            code, msg = err
            return FastApiResponse(status_code=code, content=msg)
        elif res is not None:
            tx_json, etag = res
            return FastApiJsonResponse(status_code=200, content=tx_json,
                                       headers={'etag': etag})
        else:
            assert False

    @app.put('/transactions/{tx_rest_id}/body')
    async def create_tx_body(tx_rest_id : str,
                             request : FastApiRequest) -> FastApiResponse:
        await receiver.put_blob_async(
            tx_rest_id, request.headers, request.stream(), tx_body=True)
        return FastApiResponse(status_code=200)

    @app.put('/transactions/{tx_rest_id}/blob/{blob_id}')
    async def put_blob(tx_rest_id : str, blob_id : str,
                       request : FastApiRequest) -> FastApiResponse:
        code, msg = await receiver.put_blob_async(
            tx_rest_id, request.headers, request.stream(), blob_id=blob_id)
        return FastApiResponse(status_code=code, content=msg)

    @app.post('/transactions/{tx_rest_id}/cancel')
    async def cancel_tx(tx_rest_id) -> FastApiResponse:
        receiver.cancel_tx(tx_rest_id)
        return FastApiResponse()


    return app
