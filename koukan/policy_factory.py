# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Callable, Dict, Optional, Tuple
import logging
import importlib
import inspect

from koukan.filter_chain import (
    ProxyFilter,
    TransactionMetadata )
from koukan.sender import Sender

from koukan.policy_action_filter import (
    PolicyActionFilter,
    TransactionMatcher )

from koukan.matcher_result import MatcherResult

from koukan.transaction_matchers import (
    match_invalid_mail_from,
    match_invalid_rcpt_to,
    match_network_address,
    match_num_rcpts,
    match_smtp_auth,
    match_smtp_tls )
from koukan.address_list_policy import match_address_list

class PolicyFactory:
    matchers : Dict[str, TransactionMatcher]

    def __init__(self) -> None:
        self.matchers = {}
        self.add_matcher('address_list', match_address_list)
        self.add_matcher('invalid_mail_from', match_invalid_mail_from)
        self.add_matcher('invalid_rcpt_to', match_invalid_rcpt_to)
        self.add_matcher('network_address', match_network_address)
        self.add_matcher('num_rcpts', match_num_rcpts)
        self.add_matcher('smtp_tls', match_smtp_tls)
        self.add_matcher('smtp_auth', match_smtp_auth)

    # TODO dedupe with recipient_router_factory?
    def _load_user_module(self, name, mod):
        colon = mod.find(':')
        if colon > 0:
            mod_name = mod[0:colon]
            fn_name = mod[colon+1:]
        else:
            mod_name = mod
            fn_name = 'match'
        logging.debug('%s %s', mod_name, fn_name)
        modd = importlib.import_module(mod_name)
        return getattr(modd, fn_name)

    def _load_matcher(self, name, mod):
        fn = self._load_user_module(name, mod)
        sig = inspect.signature(fn)
        param = list(sig.parameters)
        assert len(param) == 3
        assert sig.parameters[param[0]].annotation == dict  # yaml
        assert sig.parameters[param[1]].annotation == TransactionMetadata
        assert sig.parameters[param[2]].annotation == Optional[int]
        assert sig.return_annotation == MatcherResult

        self.add_matcher(name, fn)

    def add_matcher(self, name, m : TransactionMatcher):
        assert name not in self.matchers
        self.matchers[name] = m

    def load_matchers(self, yaml):
        if (policy_yaml := yaml.get('modules', {})
            .get('transaction_matcher', None)) is None:
            return

        for name,mod in policy_yaml.items():
            logging.debug('%s %s', name, mod)
            self._load_matcher(name, mod)

    def build_policy_action(self, yaml : dict, sender : Sender) -> ProxyFilter:
        return PolicyActionFilter(yaml, self.matchers)
