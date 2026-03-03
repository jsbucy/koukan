from typing import Optional
from abc import ABC, abstractmethod

from koukan.rest_schema import WhichJson
from koukan.matcher_result import MatcherResult

class FilterOutput(ABC):
    def to_json(self, which_js : WhichJson) -> Optional[dict]:
        return None

    @abstractmethod
    def match(self, yaml : dict, rcpt_num : Optional[int]) -> MatcherResult:
        raise NotImplementedError()
