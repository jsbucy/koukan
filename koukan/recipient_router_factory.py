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
from koukan.filter_chain import Filter

class RecipientRouterFactory:
    router_policies : Dict[str, RoutingPolicy]

    def __init__(self):
        self.router_policies = {}
        self.add_router_policy('dest_domain', self.router_policy_dest_domain)
        self.add_router_policy('address_list', self.router_policy_address_list)

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
        return getattr(modd, fn_name)

    def _load_router_policy(self, name, mod):
        fn = self._load_user_module(name, mod)
        sig = inspect.signature(fn)
        param = list(sig.parameters)
        assert len(param) == 1
        # assert sig.parameters[param[0]].annotation == dict  # yaml
        assert sig.return_annotation == RoutingPolicy

        self.add_router_policy(name, fn)

    def add_router_policy(self, name, fn):
        assert name not in self.router_policies
        self.router_policies[name] = fn

    def load_policies(self, yaml):
        if (policy_yaml := yaml.get('modules', {})
            .get('recipient_router_policy', None)) is None:
            return

        for name,mod in policy_yaml.items():
            logging.debug('%s %s', name, mod)
            self._load_router_policy(name, mod)

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

    def build_router(self, yaml : dict) -> Filter:
        policy_yaml = yaml['policy']
        policy_name = policy_yaml['name']
        policy = self.router_policies[policy_name](policy_yaml)
        return RecipientRouterFilter(policy)
