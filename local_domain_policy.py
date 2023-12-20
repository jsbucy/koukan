from typing import Dict

from address import domain_from_address
from response import Response
from router import RoutingPolicy

class LocalDomainPolicy(RoutingPolicy):
    # domain -> rest endpoint url
    def __init__(self, local_domains : Dict[str,str]):
        # dict from domain to host
        self.local_domains = local_domains

    def endpoint_for_rcpt(self, rcpt):
        d = domain_from_address(rcpt)
        if d is None:
            return None, None, Response(550, 'LocalDomainPolicy bad address')
        if d not in self.local_domains:
            return None, None, Response(550, 'LocalDomainPolicy unknown domain')
        return self.local_domains[d], d, None
