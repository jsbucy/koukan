# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import List, Optional, Union

from dns.resolver import Answer

from koukan.dns_wrapper import Resolver

class FakeResolver(Resolver):
    answers : List[Union[Answer, Exception]]

    def __init__(self,
                 answers : Optional[List[Union[Answer, Exception]]] = None):
        self.answers = answers if answers else []
    def resolve_address(self, addr):
        next = self.answers.pop(0)
        if isinstance(next, Exception):
            raise next
        return next
    def resolve(self, host, rrtype):
        next = self.answers.pop(0)
        if isinstance(next, Exception):
            raise next
        return next
