from typing import Optional, Tuple

from address import domain_from_address
from response import Response
from recipient_router_filter import Destination, RoutingPolicy
from filter import HostPort

class DestDomainPolicy(RoutingPolicy):
    def __init__(self, dest_port=25):
        self.dest_port = dest_port

    # called on the first recipient in the transaction
    def endpoint_for_rcpt(self, rcpt) -> Tuple[
            Optional[Destination], Optional[Response]]:
        domain = domain_from_address(rcpt)
        if domain is None:
            return None, Response(550, 'DestDomainPolicy bad address')
        return Destination(remote_host=HostPort(domain, self.dest_port)), None
