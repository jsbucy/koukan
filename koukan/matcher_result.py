# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0

from enum import IntEnum

class MatcherResult(IntEnum):
    PRECONDITION_UNMET = 0
    MATCH = 1
    NO_MATCH = 2

    @classmethod
    def from_bool(cls, b):
        return MatcherResult.MATCH if b else MatcherResult.NO_MATCH
