
from address import domain_from_address

from response import Response

# really Mx policy, construct an endpoint for the dest domain with the
# provided factory
class DestDomainPolicy:
    def __init__(self, endpoint_factory):
        self.endpoint_factory = endpoint_factory

    # called on the first recipient in the transaction
    def endpoint_for_rcpt(self, rcpt):
        d = domain_from_address(rcpt)
        if d is None:
            return None, Response(550, 'DestDomainPolicy bad address')
        return self.endpoint_factory(d), Response()

    def check_rcpt(self, rcpt0, rcpt):
        if domain_from_address(rcpt0) != domain_from_address(rcpt):
            return Response(452, 'too many recipients --'
                            'DestDomainPolicy.check_rcpt')
        return Response()
