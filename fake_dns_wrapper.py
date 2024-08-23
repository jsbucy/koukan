from typing import List, Union

from dns.resolver import Answer

from dns_wrapper import Resolver

class FakeResolver(Resolver):
    answers : List[Union[Answer, Exception]]

    def __init__(self):
        self.answers = []
    def resolve_address(self, addr):
        if isinstance(self.addr_ans, Exception):
            raise self.addr_ans
        return self.addr_ans
    def resolve(self, host, rrtype):
        next = self.answers.pop(0)
        if isinstance(next, Exception):
            raise next
        return next
