# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Callable, Dict, Optional, Tuple
import logging
import importlib
from functools import partial
import inspect

from koukan.filter import SyncFilter
from koukan.filter_chain import FilterChain

class FilterSpec:
    builder : Callable[[Any, SyncFilter], SyncFilter]
    def __init__(self, builder):
        self.builder = builder

class FilterChainFactory:
    endpoint_yaml : Optional[dict] = None
    filters : Dict[str, FilterSpec]
    root_yaml : dict

    def __init__(self, root_yaml : dict):
        self.router_policies = {}
        self.filters = {}
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

    def add_filter(self, name, fn):
        assert name not in self.filters
        self.filters[name] = FilterSpec(fn)

    def load_user_modules(self, yaml):
        if (filter_yaml := yaml.get('sync_filter', None)) is None:
            return
        for name,mod in filter_yaml.items():
            logging.debug('%s %s', name, mod)
            self._load_filter(name, mod)

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

    def _get_filter(self, filter_yaml, next):
        filter_name = filter_yaml['filter']
        spec = self.filters[filter_name]
        filter = spec.builder(filter_yaml, next)
        # assert isinstance(filter, SyncFilter)
        return filter

    def build_filter_chain(self, host) -> Optional[Tuple[FilterChain, dict]]:
        if (endpoint_yaml := self.endpoint_yaml.get(host, None)) is None:
            return None
        next : Optional[SyncFilter] = None
        filters = [self._get_filter(filter_yaml, None)
                   for filter_yaml in endpoint_yaml['chain']]
        return FilterChain(filters), endpoint_yaml
