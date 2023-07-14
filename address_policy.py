
from address import domain_from_address

from email import _header_value_parser

class PlusAddr:
    def __init__(self, base, endpoint_factory):
        self.base = base.lower()
        self.endpoint_factory = endpoint_factory

    def match(self, addr):
        spec = _header_value_parser.get_addr_spec(addr)
        if len(spec) != 2 or spec[0].defects:
            return False
        local = spec[0].local_part.lower()
        base_len = len(self.base)
        return local == self.base or (
            local.startswith(self.base) and len(local) > base_len and
            local[base_len] == '+')

    def endpoint(self, addr):
        return self.endpoint_factory(addr)


class AddressPolicy:
    patterns = []

    def __init__(self, patterns):
        self.patterns = patterns

    # called on the first recipient in the transaction
    def endpoint_for_rcpt(self, rcpt) -> "Endpoint":
        p = self.match_rcpt(rcpt)
        if p:
            return p.endpoint(rcpt)
        return None

    def check_rcpt(self, rcpt0, rcpt) -> bool:
        p = self.match_rcpt(rcpt)
        return p and p == self.match_rcpt(rcpt0)

    def match_rcpt(self, rcpt):
        for p in self.patterns:
            if p.match(rcpt):
                return p
        return None
