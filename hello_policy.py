import logging

from recipient_router_filter import Destination, RoutingPolicy

class HelloPolicy(RoutingPolicy):
    def endpoint_for_rcpt(self, rcpt):
        logging.debug('HelloPolicy.endpoint_for_rcpt %s', rcpt)
        return None, None

def factory(yaml):
    return HelloPolicy()
