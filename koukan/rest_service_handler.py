# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Callable, Dict, Optional, Union

from abc import ABC, abstractmethod

from fastapi import (
    Request as FastApiRequest,
    Response as FastApiResponse )

HttpRequest = FastApiRequest
HttpResponse = FastApiResponse

class Handler(ABC):
    @abstractmethod
    def create_tx(self, request : HttpRequest, req_json: dict
                  ) -> HttpResponse:
        pass

    @abstractmethod
    async def get_tx_async(self, request : HttpRequest) -> HttpResponse:
        pass

    @abstractmethod
    def patch_tx(self, request : HttpRequest,
                 req_json : dict,
                 message_builder : bool = False) -> HttpResponse:
        pass

    @abstractmethod
    async def create_blob_async(
            self, request : HttpRequest,
            tx_body : bool = False,
            req_upload : Optional[str] = None
    ) -> HttpResponse:
        pass

    @abstractmethod
    async def put_blob_async(
            self, request : HttpRequest,
            blob_rest_id : Optional[str] = None,
            tx_body : bool = False) -> HttpResponse:
        pass

    @abstractmethod
    def cancel_tx(self, request : HttpRequest) -> HttpResponse:
        pass

    @abstractmethod
    async def handle_async(self, request : HttpRequest, fn : Callable
                           ) -> HttpResponse:
        pass

# These are called early in the request lifecycle i.e. at the
# beginning of the wsgi/asgi entry point and therefore should not do
# db reads etc, just create the objects. As such, this should not fail
# to create the handler but rather return a handler that returns an
# error, etc.
class HandlerFactory(ABC):
    @abstractmethod
    def create_tx(self, http_host : str) -> Handler:
        pass

    @abstractmethod
    def get_tx(self, tx_rest_id : str) -> Handler:
        pass
