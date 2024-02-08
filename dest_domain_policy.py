
from address import domain_from_address
from response import Response
from recipient_router_filter import RoutingPolicy
from filter import HostPort

class DestDomainPolicy(RoutingPolicy):
    def __init__(self, dest_port=25):
        self.dest_port = dest_port

    # called on the first recipient in the transaction
    def endpoint_for_rcpt(self, rcpt):
        d = domain_from_address(rcpt)
        if d is None:
            return None, None, Response(550, 'DestDomainPolicy bad address')
        return None, HostPort(d, self.dest_port), None
