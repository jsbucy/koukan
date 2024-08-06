from typing import Dict, Optional, Tuple

from filter import HostPort

from address import domain_from_address
from response import Response
from recipient_router_filter import Destination, RoutingPolicy

class LocalDomainPolicy(RoutingPolicy):
    local_domains : Dict[str, Destination]

    def __init__(self, local_domains : Dict[str,Destination]):
        self.local_domains = local_domains

    def endpoint_for_rcpt(self, rcpt) -> Tuple[
            Optional[Destination], Optional[Response]]:
        domain = domain_from_address(rcpt)
        if domain is None:
            return None, Response(550, 'LocalDomainPolicy bad address')
        if domain not in self.local_domains:
            return None, Response(550, 'LocalDomainPolicy unknown domain')
        return self.local_domains[domain], None
