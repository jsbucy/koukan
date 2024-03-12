from typing import Any, Callable, Optional, Tuple
import logging
import sys

from yaml import load, CLoader as Loader
from local_domain_policy import LocalDomainPolicy
from dest_domain_policy import DestDomainPolicy
from recipient_router_filter import RecipientRouterFilter
from rest_endpoint import RestEndpoint, constant_resolution
from dkim_endpoint import DkimEndpoint
from mx_resolution import resolve as resolve_mx
from message_builder_filter import MessageBuilderFilter
from filter import Filter, HostPort
from storage_writer_filter import StorageWriterFilter
from exploder import Exploder
from storage import Storage

class Config:
    storage : Optional[Storage] = None
    endpoint_yaml : Optional[dict] = None

    def __init__(self):
        self.router_policies = {
            'dest_domain': self.router_policy_dest_domain,
            'local_domain': self.router_policy_local_domain}
        self.filters = {
            'rest_output': self.rest_output,
            'router': self.router,
            'dkim': self.dkim,
            'exploder': self.exploder,
            'message_builder': self.message_builder
       }

    def set_storage(self, storage : Storage):
        self.storage = storage

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

    def exploder(self, yaml, next):
        assert next is None
        msa = msa=yaml.get('msa', False)
        rcpt_timeout = 30
        data_timeout = 300
        max_attempts = 100
        if msa:
            rcpt_timeout = 5
            data_timeout = 30
        return Exploder(yaml['output_chain'],
                        lambda: StorageWriterFilter(self.storage),
                        msa=msa,
                        rcpt_timeout=yaml.get('rcpt_timeout', rcpt_timeout),
                        data_timeout=yaml.get('data_timeout', data_timeout),
                        max_attempts=yaml.get('max_attempts', max_attempts))


    def rest_output(self, yaml, next):
        logging.debug('Config.rest_output %s', yaml)
        assert next is None
        static_remote_host_yaml = yaml.get('static_remote_host', None)
        static_remote_host = (HostPort.from_yaml(static_remote_host_yaml)
                              if static_remote_host_yaml else None)
        logging.info('Factory.rest_output %s', static_remote_host)
        remote_host_disco = None
        if static_remote_host is not None:
            remote_host_disco = constant_resolution(static_remote_host)
        elif yaml.get('remote_host_discovery', '') == 'mx':
            remote_host_disco = resolve_mx
        rcpt_timeout = 30
        data_timeout = 300
        return RestEndpoint(
            static_base_url = yaml['static_endpoint'],
            http_host = yaml['http_host'],
            remote_host_resolution = remote_host_disco,
            timeout_start=yaml.get('rcpt_timeout', rcpt_timeout),
            timeout_data=yaml.get('data_timeout', data_timeout))

    def router_policy_dest_domain(self, policy_yaml):
        return DestDomainPolicy()

    def router_policy_local_domain(self, policy_yaml):
        d = {}
        for domain in policy_yaml['domains']:
            d[domain['name']] = domain.get('endpoint', None)
        logging.info('router_policy_local_domain %s', d)
        return LocalDomainPolicy(d)

    def router(self, yaml, next):
        policy_yaml = yaml['policy']
        policy_name = policy_yaml['name']
        policy = self.router_policies[policy_name](policy_yaml)
        return RecipientRouterFilter(policy, next)

    def dkim(self, yaml, next):
        if 'key' not in yaml:
            return None
        return DkimEndpoint(
            yaml['domain'], yaml['selector'], yaml['key'], next)

    def message_builder(self, yaml, next):
        return MessageBuilderFilter(self.storage, next)

    def get_endpoint(self, host) -> Tuple[Filter, bool]:
        endpoint_yaml = self.endpoint_yaml[host]
        next : Optional[Filter] = None
        for filter_yaml in reversed(endpoint_yaml['chain']):
            filter_name = filter_yaml['filter']
            endpoint = self.filters[filter_name](filter_yaml, next)
            next = endpoint
        assert next is not None
        return next, endpoint_yaml
