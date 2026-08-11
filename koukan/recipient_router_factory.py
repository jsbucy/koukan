# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Callable, Dict, Optional, Tuple
import logging
import importlib
import inspect

from koukan.address_list_policy import AddressListPolicy
from koukan.dest_domain_policy import DestDomainPolicy
from koukan.recipient_router_filter import (
    Destination,
    RecipientRouterFilter,
    RoutingPolicy )
from koukan.filter import (
    HostPort,
    Resolution )
from koukan.filter_chain import ProxyFilter
from koukan.sender import Sender

PolicyFactory = Callable[[dict], RoutingPolicy]

class RecipientRouterFactory:
    router_policies : Dict[str, PolicyFactory]
    rest_endpoints : Dict[str, Dict[str, Any]]
    sender_factory : Callable[[Sender], Optional[Sender]]

    def __init__(self, rest_endpoints, sender_factory) -> None:
        self.router_policies = {}
        self.rest_endpoints = rest_endpoints
        self.sender_factory = sender_factory
        self.add_router_policy('dest_domain', self._policy_dest_domain)
        self.add_router_policy('address_list', self._policy_address_list)

    def _load_user_module(self, name, mod) -> PolicyFactory:
        colon = mod.find(':')
        if colon > 0:
            mod_name = mod[0:colon]
            fn_name = mod[colon+1:]
        else:
            mod_name = mod
            fn_name = 'factory'
        logging.debug('%s %s', mod_name, fn_name)
        modd = importlib.import_module(mod_name)
        return getattr(modd, fn_name)

    def _load_router_policy(self, name, mod) -> None:
        fn = self._load_user_module(name, mod)
        sig = inspect.signature(fn)
        param = list(sig.parameters)
        assert len(param) == 1
        # assert sig.parameters[param[0]].annotation == dict  # yaml
        assert sig.return_annotation == RoutingPolicy

        self.add_router_policy(name, fn)

    def add_router_policy(self, name, fn) -> None:
        assert name not in self.router_policies
        self.router_policies[name] = fn

    def load_policies(self, yaml) -> None:
        if (policy_yaml := yaml.get('modules', {})
            .get('recipient_router_policy', None)) is None:
            return

        for name,mod in policy_yaml.items():
            logging.debug('%s %s', name, mod)
            self._load_router_policy(name, mod)

    def _policy_dest_domain(self, policy_yaml) -> DestDomainPolicy:
        dest = self._route_destination(policy_yaml)
        assert dest is not None
        return DestDomainPolicy(dest, policy_yaml.get('dest_port', 25))

    def _route_destination(self, yaml) -> Optional[Destination]:
        if 'destination' not in yaml:
            return None
        dest = yaml['destination']
        # placeholder non-None dest for dry_run routes
        if not dest:
            return Destination()
        rest_endpoint_yaml = None
        if (endpoint := dest.get('endpoint', None)) is None:
            return None
        if (rest_endpoint_yaml := self.rest_endpoints.get(endpoint, None)
            ) is None:
            return None

        hosts = None
        if 'host_list' in dest:
            hosts = [HostPort.from_yaml(h) for h in dest['host_list']]

        if (sender_name := rest_endpoint_yaml.get('sender', None)) is None:
            return None
        rest_upstream_sender = Sender(
            sender_name, rest_endpoint_yaml.get('tag', None))
        # NOTE router -> gw uses sender 'router' which isn't defined in
        # the router yaml
        if (s := self.sender_factory(rest_upstream_sender)) is not None:
            rest_upstream_sender = s
        options = rest_endpoint_yaml.get('options', {})
        dest_options = dest.get('options', {})
        options.update(dest_options)
        return Destination(
            rest_endpoint = rest_endpoint_yaml.get('endpoint', None),
            rest_upstream_sender = rest_upstream_sender,
            options = options if options else None,
            remote_host = hosts)

    def _policy_address_list(self, policy_yaml) -> AddressListPolicy:
        dest = self._route_destination(policy_yaml)
        return AddressListPolicy(
            policy_yaml.get('domains', []),
            policy_yaml.get('delimiter', None),
            policy_yaml.get('prefixes', []),
            dest)

    def build_router(self, yaml : dict, sender : Sender) -> ProxyFilter:
        if policy_yaml := yaml.get('policy', None):
            policy_name = policy_yaml['name']
            policy = self.router_policies[policy_name](policy_yaml)
            dest = None
        else:
            policy = None
            dest = self._route_destination(yaml)
        return RecipientRouterFilter(policy, yaml.get('dry_run', False), dest)
