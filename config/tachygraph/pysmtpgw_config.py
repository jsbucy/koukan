from typing import Dict, Tuple, Any

from address_policy import AddressPolicy, PrefixAddr
from local_domain_policy import LocalDomainPolicy
from dest_domain_policy import DestDomainPolicy
from router import Router
from rest_endpoint import RestEndpoint
from dkim_endpoint import DkimEndpoint
from mx_resolution_endpoint import MxResolutionEndpoint


import logging

TIMEOUT_START=30
TIMEOUT_DATA=60

class Config:
    def __init__(self, config):
        self.config = config

    def inbound(self):
        aspmx = lambda _: RestEndpoint(
            self.config.get_str('gw_base_url'), http_host='outbound',
            static_remote_host=('aspmx.l.google.com', 25),
            timeout_start=TIMEOUT_START, timeout_data=TIMEOUT_DATA)

        sink = lambda: RestEndpoint(
            self.config.get_str('gw_base_url'), http_host='outbound',
            static_remote_host=('sink.gloop.org', 25),
            timeout_start=TIMEOUT_START, timeout_data=TIMEOUT_DATA)

        sandbox_addrs = AddressPolicy([
            PrefixAddr('bucy', delimiter='+', endpoint_factory=aspmx),
        ])
        sandbox_addr_router = lambda: Router(sandbox_addrs)

        local_domains = LocalDomainPolicy({
            'sandbox.gloop.org': sandbox_addr_router,
            'sink.gloop.org': sink })
        return Router(local_domains), False


    def outbound(self):
        outbound_mx = lambda: RestEndpoint(
            self.config.get_str('gw_base_url'), http_host='outbound',
            timeout_start=TIMEOUT_START, timeout_data=TIMEOUT_DATA)
        mx_resolution = lambda: MxResolutionEndpoint(outbound_mx)
        next = mx_resolution

        dkim_key = self.config.get_str('dkim_key')
        if dkim_key:
            print('enabled dkim signing', dkim_key)
            next = lambda: DkimEndpoint(
                self.config.get_str('dkim_domain').encode('ascii'),
                self.config.get_str('dkim_selector').encode('ascii'),
                dkim_key,
                mx_resolution())

        dest_domain_policy = DestDomainPolicy(next)
        return Router(dest_domain_policy), True


    def get_endpoint(self, host) -> Tuple[Any, bool]:
        logging.info('tachygraph Config.get_endpoint %s', host)
        if host == 'inbound-gw':
            return self.inbound()
        elif host == 'outbound-gw':
            return self.outbound()
        else:
            return None, None
