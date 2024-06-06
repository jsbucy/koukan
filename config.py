from typing import Any, Callable, Optional, Tuple, Union
import logging
import sys
import secrets

from yaml import load, CLoader as Loader
from local_domain_policy import LocalDomainPolicy
from dest_domain_policy import DestDomainPolicy
from recipient_router_filter import RecipientRouterFilter
from rest_endpoint import RestEndpoint, constant_resolution
from dkim_endpoint import DkimEndpoint
from mx_resolution import resolve as resolve_mx
from message_builder_filter import MessageBuilderFilter
from filter import Filter, HostPort, SyncFilter
from filter_adapters import DeltaToFullAdapter, FullToDeltaAdapter
from storage_writer_filter import StorageWriterFilter
from exploder import Exploder
from storage import Storage
from remote_host_filter import RemoteHostFilter
from received_header_filter import ReceivedHeaderFilter
from relay_auth_filter import RelayAuthFilter

class FilterSpec:
    def __init__(self, builder, t):
        self.builder = builder
        self.t = t

class Config:
    storage : Optional[Storage] = None
    endpoint_yaml : Optional[dict] = None

    def __init__(self):
        self.router_policies = {
            'dest_domain': self.router_policy_dest_domain,
            'local_domain': self.router_policy_local_domain}
        self.filters = {
            'rest_output': FilterSpec(self.rest_output, Filter),
            'router': FilterSpec(self.router, SyncFilter),
            'dkim': FilterSpec(self.dkim, SyncFilter),
            'exploder': FilterSpec(self.exploder, SyncFilter),
            'message_builder': FilterSpec(self.message_builder, SyncFilter),
            'remote_host': FilterSpec(self.remote_host, Filter),
            'received_header': FilterSpec(self.received_header, SyncFilter),
            'relay_auth': FilterSpec(self.relay_auth, Filter)
       }

    def set_storage(self, storage : Storage):
        self.storage = storage

    def inject_filter(self, name : str, fac : Callable[[Any, Any], Any], t):
        self.filters[name] = FilterSpec(fac, t)

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
        if msa:
            rcpt_timeout = 5
            data_timeout = 30
        return Exploder(
            yaml['output_chain'],
            lambda: StorageWriterFilter(
                self.storage, rest_id_factory=self.rest_id_factory()),
            msa=msa,
            rcpt_timeout=yaml.get('rcpt_timeout', rcpt_timeout),
            data_timeout=yaml.get('data_timeout', data_timeout),
            default_notification=yaml.get('default_notification', None))

    def rest_id_factory(self):
        entropy = self.root_yaml.get('global', {}).get('rest_id_entropy', 16)
        return lambda: secrets.token_urlsafe(entropy)

    def notification_endpoint(self):
        return StorageWriterFilter(self.storage,
                                   rest_id_factory=self.rest_id_factory())

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
            timeout_data=yaml.get('data_timeout', data_timeout),
            verify=yaml.get('verify', True))

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
        return RecipientRouterFilter(
            policy, next)

    def dkim(self, yaml, next):
        if 'key' not in yaml:
            return None
        return DkimEndpoint(
            yaml['domain'], yaml['selector'], yaml['key'], next)

    def message_builder(self, yaml, next):
        return MessageBuilderFilter(self.storage, next)

    def remote_host(self, yaml, next):
        return RemoteHostFilter(next)

    def received_header(self, yaml, next):
        return ReceivedHeaderFilter(next, yaml.get('received_hostname', None))

    def relay_auth(self, yaml, next):
        return RelayAuthFilter(next, smtp_auth = yaml.get('smtp_auth', False))

    def get_endpoint(self, host) -> Tuple[Filter, bool]:
        endpoint_yaml = self.endpoint_yaml[host]
        next : Optional[Union[Filter,SyncFilter]] = None
        for filter_yaml in reversed(endpoint_yaml['chain']):
            filter_name = filter_yaml['filter']
            logging.debug('config.get_endpoint %s', filter_name)
            spec = self.filters[filter_name]
            if next is None:
                pass
            elif isinstance(next, Filter) and spec.t == SyncFilter:
                next = FullToDeltaAdapter(next)
            elif isinstance(next, SyncFilter) and spec.t == Filter:
                next = DeltaToFullAdapter(next)
            else:
                assert (isinstance(next, Filter) and spec.t == Filter) or (
                    isinstance(next, SyncFilter) and spec.t == SyncFilter)
            endpoint = spec.builder(filter_yaml, next)
            assert isinstance(endpoint, spec.t)
            next = endpoint
        assert next is not None
        if isinstance(next, SyncFilter):
            next = DeltaToFullAdapter(next)
        return next, endpoint_yaml
