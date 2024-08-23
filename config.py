from typing import Any, Callable, Dict, Optional, Tuple, Union
import logging
import sys
import secrets
import importlib

from yaml import load, CLoader as Loader
from address_list_policy import AddressListPolicy
from dest_domain_policy import DestDomainPolicy
from recipient_router_filter import (
    Destination,
    RecipientRouterFilter )
from rest_endpoint import RestEndpoint
from dkim_endpoint import DkimEndpoint
from mx_resolution import DnsResolutionFilter, StaticResolutionFilter
from message_builder_filter import MessageBuilderFilter
from message_parser_filter import MessageParserFilter
from filter import (
    AsyncFilter,
    HostPort,
    Resolution,
    SyncFilter )
from storage_writer_filter import StorageWriterFilter
from exploder import Exploder
from storage import Storage
from remote_host_filter import RemoteHostFilter
from received_header_filter import ReceivedHeaderFilter
from relay_auth_filter import RelayAuthFilter
from executor import Executor

class FilterSpec:
    def __init__(self, builder, t):
        self.builder = builder
        self.t = t

StorageWriterFactory = Callable[[],Optional[StorageWriterFilter]]

class Config:
    storage : Optional[Storage] = None
    endpoint_yaml : Optional[dict] = None
    storage_writer_factory : Optional[StorageWriterFactory] = None
    executor : Optional[Executor] = None

    def __init__(
            self,
            executor : Optional[Executor] = None,
            storage_writer_factory : Optional[StorageWriterFactory] = None,
    ):
        self.executor = executor
        self.storage_writer_factory = storage_writer_factory
        self.router_policies = {
            'dest_domain': self.router_policy_dest_domain,
            'address_list': self.router_policy_address_list }
        self.filters = {
            'rest_output': FilterSpec(self.rest_output, SyncFilter),
            'router': FilterSpec(self.router, SyncFilter),
            'dkim': FilterSpec(self.dkim, SyncFilter),
            'exploder': FilterSpec(self.exploder, SyncFilter),
            'message_builder': FilterSpec(self.message_builder, SyncFilter),
            'message_parser': FilterSpec(self.message_parser, SyncFilter),
            'remote_host': FilterSpec(self.remote_host, SyncFilter),
            'received_header': FilterSpec(self.received_header, SyncFilter),
            'relay_auth': FilterSpec(self.relay_auth, SyncFilter),
            'static_resolution': FilterSpec(self.static_resolution, SyncFilter),
            'dns_resolution': FilterSpec(self.dns_resolution, SyncFilter),
        }

    def _load_user_module(self, name, mod, add_factory):
        dot = mod.find('.')
        if dot > 0:
            mod_name = mod[0:dot]
            fn_name = mod[dot+1:]
        else:
            mod_name = mod
            fn_name = 'factory'
        logging.debug('%s %s', mod_name, fn_name)
        modd = importlib.import_module(mod_name)
        fn = getattr(modd, fn_name)
        add_factory(name, fn)

    def add_filter(self, name, fn):
        assert name not in self.filters
        self.filters[name] = FilterSpec(fn, SyncFilter)

    def add_router_policy(self, name, fn):
        assert name not in self.router_policies
        self.router_policies[name] = fn

    def load_user_modules(self, yaml):
        for modtype,add_factory in [
                ('recipient_router_policy', self.add_router_policy),
                ('sync_filter', self.add_filter)]:
            if (modtype_yaml := yaml.get(modtype, None)) is None:
                continue
            for name,mod in modtype_yaml.items():
                logging.debug('%s %s', name, mod)
                self._load_user_module(name, mod, add_factory)

    def set_storage(self, storage : Storage):
        self.storage = storage

    def inject_filter(self, name : str, fac : Callable[[Any, Any], Any], t):
        self.filters[name] = FilterSpec(fac, t)

    def inject_yaml(self, root_yaml):
        self.root_yaml = root_yaml
        self.endpoint_yaml = {}
        for endpoint_yaml in self.root_yaml.get('endpoint', []):
            self.endpoint_yaml[endpoint_yaml['name']] = endpoint_yaml
        if (modules_yaml := self.root_yaml.get('modules', None)) is not None:
            self.load_user_modules(modules_yaml)

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
            self.storage_writer_factory,
            self.executor,
            msa=msa,
            rcpt_timeout=yaml.get('rcpt_timeout', rcpt_timeout),
            data_timeout=yaml.get('data_timeout', data_timeout),
            default_notification=yaml.get('default_notification', None))

    def rest_id_factory(self):
        entropy = self.root_yaml.get('global', {}).get('rest_id_entropy', 16)
        return lambda: secrets.token_urlsafe(entropy)

    def notification_endpoint(self) -> AsyncFilter:
        return StorageWriterFilter(
            self.storage,
            rest_id_factory=self.rest_id_factory(),
            create_leased=False)

    def rest_output(self, yaml, next):
        logging.debug('Config.rest_output %s', yaml)
        assert next is None
        static_remote_host_yaml = yaml.get('static_remote_host', None)
        static_remote_host = (HostPort.from_yaml(static_remote_host_yaml)
                              if static_remote_host_yaml else None)
        logging.info('Factory.rest_output %s', static_remote_host)
        rcpt_timeout = 30
        data_timeout = 300
        return RestEndpoint(
            static_base_url = yaml.get('static_endpoint', None),
            http_host = yaml['http_host'],
            timeout_start=yaml.get('rcpt_timeout', rcpt_timeout),
            timeout_data=yaml.get('data_timeout', data_timeout),
            verify=yaml.get('verify', True))

    def router_policy_dest_domain(self, policy_yaml):
        return DestDomainPolicy(policy_yaml['endpoint'])

    def _route_destination(self, yaml):
        dest = yaml.get('destination', None)
        if dest is None:
            return None
        hosts = None
        if 'host_list' in dest:
            hosts = [HostPort.from_yaml(h) for h in dest['host_list']]
        return Destination(
            rest_endpoint = dest.get('endpoint', None),
            options = dest.get('options', None),
            remote_host = hosts)

    def router_policy_address_list(self, policy_yaml):
        return AddressListPolicy(
            policy_yaml.get('domains', []),
            policy_yaml.get('delimiter', None),
            policy_yaml.get('prefixes', []),
            self._route_destination(policy_yaml))

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

    def message_parser(self, yaml, next):
        return MessageParserFilter(next)

    def remote_host(self, yaml, next):
        return RemoteHostFilter(next)

    def received_header(self, yaml, next):
        return ReceivedHeaderFilter(next, yaml.get('received_hostname', None))

    def relay_auth(self, yaml, next):
        return RelayAuthFilter(next, smtp_auth = yaml.get('smtp_auth', False))

    def dns_resolution(self, yaml, next):
        return DnsResolutionFilter(next)

    def static_resolution(self, yaml, next):
        return StaticResolutionFilter(
            Resolution([HostPort.from_yaml(yaml)]),
            next,
            suffix=yaml.get('suffix', None),
            literal=yaml.get('literal', None),
            overwrite=yaml.get('overwrite', False))

    def get_endpoint(self, host) -> Tuple[SyncFilter, bool]:
        endpoint_yaml = self.endpoint_yaml[host]
        next : Optional[SyncFilter] = None
        for filter_yaml in reversed(endpoint_yaml['chain']):
            filter_name = filter_yaml['filter']
            logging.debug('config.get_endpoint %s', filter_name)
            spec = self.filters[filter_name]
            endpoint = spec.builder(filter_yaml, next)
            assert isinstance(endpoint, spec.t)
            next = endpoint
        assert next is not None
        return next, endpoint_yaml
