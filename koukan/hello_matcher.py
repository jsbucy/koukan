from typing import Optional

from koukan.filter import TransactionMetadata
from koukan.matcher_result import MatcherResult

def match(yaml : dict, tx : TransactionMetadata, rcpt_num : Optional[int]
          ) -> MatcherResult:
    return MatcherResult.MATCH
