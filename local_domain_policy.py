
from address import domain_from_address

class LocalDomainPolicy:
    def __init__(self, local_domains, endpoint_factory):
        self.local_domains = local_domains
        self.endpoint_factory = endpoint_factory

    # called on the first recipient in the transaction
    def endpoint_for_rcpt(self, rcpt):
        d = domain_from_address(rcpt)
        if d is None:
            return None
        if not self.match_domain(d):
            return None
        return self.endpoint_factory(d)

    def check_rcpt(self, rcpt0, rcpt):
        d = domain_from_address(rcpt)
        return self.match_domain(d)

    def match_domain(self, domain):
        for ld in self.local_domains:
            if ld == domain:  # XXX case
                return True
        return False
