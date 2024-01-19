from typing import Optional, Dict, Any

from abc import ABC, abstractmethod

from flask import Response as FlaskResponse, Request as FlaskRequest
from werkzeug.datastructures import ContentRange

class Handler(ABC):
    @abstractmethod
    def start(self, req_json) -> Optional[FlaskResponse]:
        pass

    @abstractmethod
    def get(self, req_json : Dict[str, Any]) -> FlaskResponse:
        pass

    @abstractmethod
    def patch(self, req_json : Dict[str, Any]) -> FlaskResponse:
        pass

    @abstractmethod
    def append(self, req_json : Dict[str, Any]) -> FlaskResponse:
        pass

    @abstractmethod
    def put_blob(self, request : FlaskRequest,
                 content_range : ContentRange,
                 range_in_headers : bool) -> FlaskResponse:
        pass

    @abstractmethod
    def set_durable(self, req_json : Dict[str, Any]) -> FlaskResponse:
        pass

    @abstractmethod
    def tx_rest_id(self) -> str:
        pass

    # This should be valid before and after handling a request. The
    # first-tier request handler validates if-match: and returns etag:
    # headers.
    def etag(self) -> Optional[str]:
        return None

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
