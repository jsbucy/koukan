# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Callable, Dict, Optional, Tuple
import logging
import importlib
from functools import partial
import inspect

from koukan.address_list_policy import AddressListPolicy
from koukan.dest_domain_policy import DestDomainPolicy
from koukan.recipient_router_filter import (
    Destination,
    RecipientRouterFilter,
    RoutingPolicy )
from koukan.rest_endpoint import RestEndpoint
from koukan.dkim_endpoint import DkimEndpoint
from koukan.mx_resolution import DnsResolutionFilter
from koukan.message_parser_filter import MessageParserFilter
from koukan.filter import (
    AsyncFilter,
    HostPort,
    Resolution,
    SyncFilter )
from koukan.exploder import Exploder
from koukan.remote_host_filter import RemoteHostFilter
from koukan.received_header_filter import ReceivedHeaderFilter
from koukan.relay_auth_filter import RelayAuthFilter
from koukan.async_filter_wrapper import AsyncFilterWrapper
from koukan.add_route_filter import AddRouteFilter

class FilterSpec:
    builder : Callable[[Any, SyncFilter], SyncFilter]
    def __init__(self, builder):
        self.builder = builder

StorageWriterFactory = Callable[[str],Optional[AsyncFilter]]

class FilterChainFactory:
    endpoint_yaml : Optional[dict] = None
    exploder_output_factory : Optional[StorageWriterFactory] = None
    filters : Dict[str, FilterSpec]
    root_yaml : dict
    router_policies : Dict[str, RoutingPolicy]

    def __init__(
            self,
            root_yaml : dict,
            exploder_output_factory : Optional[StorageWriterFactory] = None):
        self.exploder_output_factory = exploder_output_factory
        self.router_policies = {}
        self.add_router_policy('dest_domain', self.router_policy_dest_domain)
        self.add_router_policy('address_list', self.router_policy_address_list)
        self.filters = {}
        self.add_filter('rest_output', self.rest_output)
        self.add_filter('router', self.router)
        self.add_filter('dkim', self.dkim)
        # router handle_new_tx()
        self.add_filter('exploder', self.exploder)
        self.add_filter('message_parser', self.message_parser)
        self.add_filter('remote_host', self.remote_host)
        self.add_filter('received_header', self.received_header)
        self.add_filter('relay_auth', self.relay_auth)
        self.add_filter('dns_resolution', self.dns_resolution)
        # router handle_new_tx()
        self.add_filter('add_route', self.add_route)

        self._inject_yaml(root_yaml)


    def _load_user_module(self, name, mod):
        colon = mod.find(':')
        if colon > 0:
            mod_name = mod[0:colon]
            fn_name = mod[colon+1:]
        else:
            mod_name = mod
            fn_name = 'factory'
        logging.debug('%s %s', mod_name, fn_name)
        modd = importlib.import_module(mod_name)
        fn = getattr(modd, fn_name)
        return fn

    def _load_filter(self, name, mod):
        fn = self._load_user_module(name, mod)
        sig = inspect.signature(fn)
        param = list(sig.parameters)
        assert len(param) == 2
        # assert sig.parameters[param[0]].annotation == dict  # yaml
        assert sig.parameters[param[1]].annotation == SyncFilter
        assert sig.return_annotation == SyncFilter
        self.inject_filter(name, fn)

    def _load_router_policy(self, name, mod):
        fn = self._load_user_module(name, mod)
        self.add_router_policy(name, fn)

    def add_filter(self, name, fn):
        assert name not in self.filters
        self.filters[name] = FilterSpec(fn)

    def add_router_policy(self, name, fn):
        assert name not in self.router_policies
        self.router_policies[name] = fn

    def load_user_modules(self, yaml):
        for modtype,load in [
                ('recipient_router_policy', self._load_router_policy),
                ('sync_filter', self._load_filter)]:
            if (modtype_yaml := yaml.get(modtype, None)) is None:
                continue
            for name,mod in modtype_yaml.items():
                logging.debug('%s %s', name, mod)
                load(name, mod)

    def inject_filter(self, name : str,
                      fac : Callable[[Any, SyncFilter], SyncFilter]):
        self.filters[name] = FilterSpec(fac)

    def _inject_yaml(self, root_yaml):
        self.root_yaml = root_yaml
        self.endpoint_yaml = {}
        for endpoint_yaml in self.root_yaml.get('endpoint', []):
            self.endpoint_yaml[endpoint_yaml['name']] = endpoint_yaml

        if (modules_yaml := self.root_yaml.get('modules', None)) is not None:
            self.load_user_modules(modules_yaml)

    def exploder_upstream(self, http_host : str,
                          rcpt_timeout : float,
                          data_timeout : float,
                          store_and_forward : bool,
                          notification : Optional[dict],
                          retry : Optional[dict]):
        upstream : Optional[AsyncFilter] = self.exploder_output_factory(
            http_host)
        # TODO this is expected if the router can't start the upstream OH
        #assert upstream is not None
        return AsyncFilterWrapper(
            upstream,
            rcpt_timeout,
            store_and_forward=store_and_forward,
            default_notification=notification, retry_params=retry)

    def exploder(self, yaml, next):
        assert next is None
        msa = msa=yaml.get('msa', False)
        rcpt_timeout = 30
        data_timeout = 300
        if msa:
            rcpt_timeout = 5
            data_timeout = 30
        notification = yaml.get('default_notification', None)
        return Exploder(
            yaml['output_chain'],
            partial(self.exploder_upstream, yaml['output_chain'],
                    rcpt_timeout, data_timeout, msa, notification,
                    retry={}),
            rcpt_timeout=yaml.get('rcpt_timeout', rcpt_timeout),
            data_timeout=yaml.get('data_timeout', data_timeout),
            default_notification=notification)

    def add_route(self, yaml, next):
        if yaml.get('store_and_forward', None):
            add_route = self.exploder_upstream(
                yaml['output_chain'],
                0, 0,  # 0 upstream timeout ~ effectively swallow errors
                store_and_forward=True,
                notification=yaml.get('notification', None),
                retry=yaml.get('retry_params', None))
        else:
            output = self.get_endpoint(yaml['output_chain'])
            if output is None:
                return None
            add_route, output_yaml = output
        return AddRouteFilter(add_route, yaml['output_chain'], next)

    def rest_output(self, yaml, next):
        logging.debug('Config.rest_output %s', yaml)
        assert next is None
        chunk_size = yaml.get('chunk_size', None)
        static_remote_host_yaml = yaml.get('static_remote_host', None)
        static_remote_host = (HostPort.from_yaml(static_remote_host_yaml)
                              if static_remote_host_yaml else None)
        logging.info('Factory.rest_output %s', static_remote_host)
        rcpt_timeout = 30
        data_timeout = 300
        return RestEndpoint(
            static_base_url = yaml.get('static_endpoint', None),
            static_http_host = yaml.get('http_host', None),
            timeout_start=yaml.get('rcpt_timeout', rcpt_timeout),
            timeout_data=yaml.get('data_timeout', data_timeout),
            verify=yaml.get('verify', True),
            chunk_size=chunk_size)

    def router_policy_dest_domain(self, policy_yaml):
        return DestDomainPolicy(self._route_destination(policy_yaml),
                                policy_yaml.get('dest_port', 25))

    def _route_destination(self, yaml):
        dest = yaml.get('destination', None)
        if dest is None:
            return None
        hosts = None
        if 'host_list' in dest:
            hosts = [HostPort.from_yaml(h) for h in dest['host_list']]
        return Destination(
            rest_endpoint = dest.get('endpoint', None),
            http_host = dest.get('http_host', None),
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

    def message_parser(self, yaml, next):
        return MessageParserFilter(next)

    def remote_host(self, yaml, next):
        return RemoteHostFilter(next)

    def received_header(self, yaml, next):
        return ReceivedHeaderFilter(next, yaml.get('received_hostname', None))

    def relay_auth(self, yaml, next):
        return RelayAuthFilter(next, smtp_auth = yaml.get('smtp_auth', False))

    def dns_resolution(self, yaml, next):
        host_list = yaml.get('static_hosts', None)
        static_resolution = None
        if host_list:
            static_resolution = Resolution(
                [HostPort.from_yaml(h) for h in host_list])
        # TODO add option for mx resolution (vs just A)
        return DnsResolutionFilter(
            next,
            static_resolution=static_resolution,
            suffix=yaml.get('suffix', None),
            literal=yaml.get('literal', None))

    def _get_filter(self, filter_yaml, next):
        filter_name = filter_yaml['filter']
        logging.debug('config.get_endpoint %s', filter_name)
        spec = self.filters[filter_name]
        filter = spec.builder(filter_yaml, next)
        assert isinstance(filter, SyncFilter)
        return filter

    def get_endpoint(self, host) -> Optional[Tuple[SyncFilter, dict]]:
        if (endpoint_yaml := self.endpoint_yaml.get(host, None)) is None:
            return None
        next : Optional[SyncFilter] = None
        chain = list(reversed(endpoint_yaml['chain']))
        for filter_yaml in chain:
            next = self._get_filter(filter_yaml, next)
        assert next is not None
        return next, endpoint_yaml
