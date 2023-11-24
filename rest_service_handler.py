from typing import Optional, Dict, Any

from abc import ABC, abstractmethod

from flask import Response as FlaskResponse, Request as FlaskRequest

class Handler(ABC):
    @abstractmethod
    def start(self, req_json) -> Optional[FlaskResponse]:
        pass

    @abstractmethod
    def get(self, req_json : Dict[str, Any]) -> FlaskResponse:
        pass

    @abstractmethod
    def append(self, req_json : Dict[str, Any]) -> FlaskResponse:
        pass

    @abstractmethod
    def put_blob(self, request : FlaskRequest) -> FlaskResponse:
        pass

    @abstractmethod
    def set_durable(self, req_json : Dict[str, Any]) -> FlaskResponse:
        pass

    @abstractmethod
    def tx_rest_id(self) -> str:
        pass

class HandlerFactory(ABC):
    @abstractmethod
    def create_tx(self, http_host : str) -> Optional[Handler]:
        pass

    @abstractmethod
    def get_tx(self, tx_rest_id : str) -> Optional[Handler]:
        pass

    @abstractmethod
    def get_blob(self, blob_rest_id : str) -> Optional[Handler]:
        pass