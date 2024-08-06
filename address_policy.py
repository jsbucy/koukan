from typing import Any,List,Optional,Tuple

from email import _header_value_parser

from address import domain_from_address
from response import Response
from router import RoutingPolicy

class PrefixAddr:
    def __init__(self, base, delimiter, endpoint, remote_host):
        self.base = base.lower()
        self.delimiter = delimiter
        self.endpoint = endpoint
        self.remote_host = remote_host

    def match(self, addr):
        spec = _header_value_parser.get_addr_spec(addr)
        if len(spec) != 2 or spec[0].defects:
            return False
        local = spec[0].local_part.lower()
        base_len = len(self.base)
        return local == self.base or (
            local.startswith(self.base) and len(local) > base_len and
            local[base_len] == self.delimiter)

    def endpoint(self, addr):
        return self.endpoint_factory(addr)


class AddressPolicy(RoutingPolicy):
    patterns : List[PrefixAddr] = None

    def __init__(self, patterns : List[PrefixAddr]):
        self.patterns = patterns

    # called on the first recipient in the transaction
    # -> (Endpoint, host, resp)
    def endpoint_for_rcpt(self, rcpt):
        p = self.match_rcpt(rcpt)
        if p is None:
            return None, None, Response(550, 'AddressPolicy unknown address')
        return p.endpoint, p.remote_host, None

    def match_rcpt(self, rcpt):
        for p in self.patterns:
            if p.match(rcpt):
                return p
        return None
