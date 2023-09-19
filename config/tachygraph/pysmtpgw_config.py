from typing import Dict, Tuple, Any

from local_domain_policy import LocalDomainPolicy
from dest_domain_policy import DestDomainPolicy
from router import Router
from rest_endpoint import RestEndpoint
from dkim_endpoint import DkimEndpoint
from mx_resolution_endpoint import MxResolutionEndpoint


import logging

class Config:
    def __init__(self, config):
        self.config = config

    def inbound(self):
        aspmx = lambda _: RestEndpoint(
            gw_base_url, http_host='outbound',
            static_remote_host=('aspmx.l.google.com', 25))

        local_addrs = AddressPolicy([
            PrefixAddr('bucy', delimiter='+', endpoint_factory=aspmx),
        ])
        local_addr_router = lambda: Router(local_addrs)
        local_domains = LocalDomainPolicy({
            'sandbox.gloop.org': local_addr_router})
        return Router(local_domains), False


    def outbound(self):
        outbound_mx = lambda: RestEndpoint(
            gw_base_url, http_host='outbound')
        mx_resolution = lambda: MxResolutionEndpoint(outbound_mx)
        next = mx_resolution

        if dkim_key:
            print('enabled dkim signing', dkim_key)
            next = lambda: DkimEndpoint(dkim_domain, dkim_selector, dkim_key,
                                        mx_resolution())

        dest_domain_policy = DestDomainPolicy(next)
        return Router(dest_domain_policy), True


    def get_endpoint(self, host) -> Tuple[Any, bool]:
        if host == 'inbound-gw':
            return self.inbound()
        elif host == 'outbound-gw':
            return self.outbound()
        else:
            return None, None
