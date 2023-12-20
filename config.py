from typing import Any, Callable
import logging
import sys

from yaml import load, CLoader as Loader
from local_domain_policy import LocalDomainPolicy
from dest_domain_policy import DestDomainPolicy
from router import Router
from rest_endpoint import RestEndpoint
from dkim_endpoint import DkimEndpoint
from mx_resolution_endpoint import MxResolutionEndpoint

class Config:
    rest_blob_id_map = None
    def __init__(self, rest_blob_id_map = None):
       self.router_policies = {
           'dest_domain': self.router_policy_dest_domain,
           'local_domain': self.router_policy_local_domain}
       self.filters = {
           'rest_output': self.rest_output,
           'router': self.router,
           'dkim': self.dkim,
           'mx_resolution': self.mx
       }
       self.rest_blob_id_map = rest_blob_id_map

    def inject_filter(self, name : str, fac : Callable[[Any, Any], Any]):
        self.filters[name] = fac

    def inject_yaml(self, root_yaml):
        self.root_yaml = root_yaml
        self.endpoint_yaml = {}
        for endpoint_yaml in self.root_yaml.get('endpoint', []):
            self.endpoint_yaml[endpoint_yaml['name']] = endpoint_yaml

    def load_yaml(self, filename):
        root_yaml = load(open(filename, 'r'), Loader=Loader)
        self.inject_yaml(root_yaml)

    def rest_output(self, yaml, next):
        assert next is None
        static_remote_host = yaml['static_remote_host']
        logging.info('Factory.rest_output %s', static_remote_host)
        return lambda: RestEndpoint(
            yaml['endpoint'],
            http_host = yaml['http_host'],
            static_remote_host = (static_remote_host['host'],
                                  static_remote_host['port']),
            blob_id_map=self.rest_blob_id_map)

    def router_policy_dest_domain(self, policy_yaml, next):
        return DestDomainPolicy(next)

    def router_policy_local_domain(self, policy_yaml, next):
        d = {}
        for domain in policy_yaml['domains']:
            d[domain['name']] = next
        logging.info('router_policy_local_domain %s', d)
        return LocalDomainPolicy(d)

    def router(self, yaml, next):
        policy_yaml = yaml['policy']
        policy_name = policy_yaml['name']
        policy = self.router_policies[policy_name](policy_yaml, next)
        return Router(policy)

    def dkim(self, yaml, next):
        if 'key' not in yaml:
            return None
        return lambda: DkimEndpoint(
            yaml['domain'], yaml['selector'], yaml['key'], next)

    def mx(self, yaml, next):
        return lambda: MxResolutionEndpoint(next)

    def get_endpoint(self, host):
        endpoint_yaml = self.endpoint_yaml[host]
        next = None
        for filter_yaml in reversed(endpoint_yaml['chain']):
            filter_name = filter_yaml['filter']
            endpoint = self.filters[filter_name](filter_yaml, next)
            next = endpoint

        return next, endpoint_yaml['msa']

