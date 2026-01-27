from typing import Optional
from abc import ABC, abstractmethod

from koukan.rest_schema import WhichJson

class FilterOutput(ABC):
    def to_json(self, which_js : WhichJson) -> Optional[dict]:
        return None
