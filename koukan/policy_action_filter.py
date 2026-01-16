# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional, Set
import logging
from enum import IntEnum

from koukan.filter import TransactionMetadata
from koukan.filter_chain import Filter, FilterResult
from koukan.response import Response

class PolicyActionFilterOutput:
    matched_tags : Set[str]
    def __init__(self):
        self.matched_tags = set()

class PolicyActionFilter(Filter):
    yaml : dict

    def __init__(self, yaml : dict):
        self.yaml = yaml

    def _match(self, tx) -> bool:
        for match_yaml in self.yaml['match']:
            filter_output = tx.get_filter_output(match_yaml['filter_name'])
            if filter_output is None:
                logging.debug('missing %s', match_yaml['filter_name'])
                # XXX fail open for now
                return False
            if not filter_output.match(match_yaml):
                return False
        return True

    def _apply_action(self, tx, tag):
        action = self.yaml.get('action', 'ACCEPT')
        if action == 'REJECT':
            tx.fill_inflight_responses(
                Response(550, '5.6.0 message rejected ' + tag))

    def on_update(self, tx_delta : TransactionMetadata):
        tx = self.downstream_tx
        assert tx is not None
        out = tx.get_filter_output(self.fullname())
        if out is None:
            out = PolicyActionFilterOutput()
        tag = self.yaml['tag']

        if tag in out.matched_tags:
            return FilterResult()

        if not self._match(tx):
            return FilterResult()
        self._apply_action(tx, tag)
        out.matched_tags.add(tag)
        tx.add_filter_output(self.fullname(), out)
        return FilterResult()
