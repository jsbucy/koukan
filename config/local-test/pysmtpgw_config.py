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
        # AddressPolicy passes the rcpt to the endpoint factory

        outbound_host = lambda: RestEndpoint(
            self.config.get_str('gw_base_url'), http_host='outbound',
            static_remote_host=('127.0.0.1', 3025))  #'aspmx.l.google.com')

        local_domains = LocalDomainPolicy({'d': outbound_host})
        return Router(local_domains), False

    def outbound(self):
        ### outbound
        outbound_mx = lambda: RestEndpoint(
            self.config.get_str('gw_base_url'), http_host='outbound',
            static_remote_host=('127.0.0.1', 3025))
        mx_resolution = lambda: MxResolutionEndpoint(outbound_mx)
        next = mx_resolution

        dkim_key = self.config.get_str('dkim_key')
        if dkim_key:
            logging.info('test-config enabled dkim signing %s', dkim_key)
            next = lambda: DkimEndpoint(
                self.config.get_str('dkim_domain'),
                self.config.get_str('dkim_selector'),
                dkim_key,
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
