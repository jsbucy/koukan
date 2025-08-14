# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Callable, Dict, Optional, Tuple
import logging
import importlib
from functools import partial
import inspect
import asyncio

from koukan.filter_chain import BaseFilter, Filter
from koukan.filter_chain import FilterChain

class FilterSpec:
    builder : Callable[[Any], Filter]
    def __init__(self, builder):
        self.builder = builder

_log_disabled_filter = {}

class FilterChainFactory:
    endpoint_yaml : Optional[dict] = None
    filters : Dict[str, FilterSpec]
    root_yaml : dict
    loop : asyncio.AbstractEventLoop

    def __init__(self, root_yaml : dict):
        self.router_policies = {}
        self.filters = {}
        self._inject_yaml(root_yaml)
        self.loop = asyncio.new_event_loop()

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
        assert len(param) == 1
        # assert sig.parameters[param[0]].annotation == dict  # yaml
        logging.debug(sig.return_annotation)
        assert issubclass(sig.return_annotation, BaseFilter)
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
                      fac : Callable[[Any], Filter]):
        self.filters[name] = FilterSpec(fac)

    def _inject_yaml(self, root_yaml):
        self.root_yaml = root_yaml
        self.endpoint_yaml = {}
        for endpoint_yaml in self.root_yaml.get('endpoint', []):
            self.endpoint_yaml[endpoint_yaml['name']] = endpoint_yaml

        if (modules_yaml := self.root_yaml.get('modules', None)) is not None:
            self.load_user_modules(modules_yaml)

    def _get_filter(self, filter_yaml):
        filter_name = filter_yaml['filter']
        spec = self.filters[filter_name]
        filter = spec.builder(filter_yaml)
        # assert isinstance(filter, Filter)
        return filter

    def build_filter_chain(self, host, endpoint_yaml : Optional[dict] = None
                           ) -> Optional[Tuple[FilterChain, dict]]:
        if endpoint_yaml is None:
            endpoint_yaml = self.endpoint_yaml.get(host, None)
            if endpoint_yaml is None:
                return None
        filters = []
        for filter_yaml in endpoint_yaml['chain']:
            f = self._get_filter(filter_yaml)
            if f is not None:
                filters.append(f)
            elif host not in _log_disabled_filter:
                # It can be convenient to leave disabled filters in
                # the yaml e.g. in the examples, dkim is disabled by
                # default (no key) but left as a placeholder for the
                # end2end test setup. Log this the first time only.
                logging.warning('filter disabled chain=%s filter=%s %s',
                                host, filter_yaml['filter'], filter_yaml)
        _log_disabled_filter[host] = True
        return FilterChain(filters, self.loop), endpoint_yaml
