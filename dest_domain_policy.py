
from address import domain_from_address

from response import Response

# really Mx policy, construct an endpoint for the dest domain with the
# provided factory
class DestDomainPolicy:
    def __init__(self, endpoint_factory, dest_port=25):
        self.endpoint_factory = endpoint_factory
        self.dest_port = dest_port

    # called on the first recipient in the transaction
    def endpoint_for_rcpt(self, rcpt):
        d = domain_from_address(rcpt)
        if d is None:
            return None, None, Response(550, 'DestDomainPolicy bad address')
        endpoint = self.endpoint_factory()
        return endpoint, (d, self.dest_port), None
