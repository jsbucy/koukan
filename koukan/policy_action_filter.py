# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Callable, Dict, Optional, Set
import logging

from koukan.filter import TransactionMetadata
from koukan.filter_chain import Filter, FilterResult
from koukan.response import Response
from koukan.matcher_result import MatcherResult
from koukan.filter_output import FilterOutput
from koukan.rest_schema import WhichJson

class PolicyActionFilterOutput(FilterOutput):
    matched_tags : Set[str]
    def __init__(self):
        self.matched_tags = set()

    def to_json(self, w : WhichJson):
        if w != WhichJson.DB_ATTEMPT:
            return None
        return {'matched_tags': list(self.matched_tags)}


class _Output:
    unmet_precondition_tags : Set[str]
    def __init__(self):
        self.unmet_precondition_tags = set()


TransactionMatcher = Callable[[dict, TransactionMetadata], MatcherResult]


class PolicyActionFilter(Filter):
    yaml : dict
    matchers : Dict[str, TransactionMatcher]

    def __init__(self, yaml : dict, matchers : Dict[str, TransactionMatcher]):
        self.yaml = yaml
        self.matchers = matchers

    def _add_missing(self, tx, tag):
        if (out := tx.get_ephemeral_filter_output(self.fullname())) is None:
            out = tx.add_ephemeral_filter_output(self.fullname(), _Output())
        out.unmet_precondition_tags.add(tag)

    def _match_one(self, tx, yaml):
        matcher_name = yaml['matcher']
        if matcher := self.matchers.get(matcher_name, None):
            return matcher(yaml, tx)

        filter_output = tx.get_filter_output(matcher_name)
        if filter_output is None:
            return MatcherResult.PRECONDITION_UNMET
        return filter_output.match(yaml)

    def _match_rec(self, tx, yaml):
        if 'matcher' in yaml:
            return self._match_one(tx, yaml)

        assert len(yaml) == 1
        op = [k for k in yaml.keys()][0]
        assert op in {'any', 'all', 'not', 'matcher'}
        arg = yaml[op]

        if op == 'not':
            assert isinstance(arg, dict)
            r = self._match_rec(tx, arg)
            if r == MatcherResult.PRECONDITION_UNMET:
                return MatcherResult.PRECONDITION_UNMET
            elif r == MatcherResult.MATCH:
                return MatcherResult.NO_MATCH
            else:
                return MatcherResult.MATCH
        elif op == 'any':
            assert isinstance(arg, list)
            for i in arg:
                assert isinstance(i, dict)
                if (r := self._match_rec(tx, i)) != MatcherResult.NO_MATCH:
                    return r
            return MatcherResult.NO_MATCH
        elif op == 'all':
            assert isinstance(arg, list)
            for i in arg:
                if (r := self._match_rec(tx, i)) != MatcherResult.MATCH:
                    return r
            return MatcherResult.MATCH


    def _match(self, tx) -> bool:
        tag = self.yaml['tag']
        # empty match specification matches everything for
        # fallthrough/catchall at the end of a group
        match = self.yaml.get('match', None)
        if match is None:
            return True
        r = self._match_rec(tx, match)
        if r == MatcherResult.PRECONDITION_UNMET:
            # TODO something like if yaml['required'] and
            # tx.body.finalized, raise
            # ie it's expected to always match by the end/if
            # it's missing at the end, it's a bug
            self._add_missing(tx, tag)
            return False

        return r == MatcherResult.MATCH

    def _apply_action(self, tx, tag):
        action = self.yaml.get('action', 'ACCEPT')
        if action == 'REJECT':
            tx.fill_inflight_responses(
                Response(550, '5.6.0 message rejected ' + tag))

    def on_update(self, tx_delta : TransactionMetadata):
        tx = self.downstream_tx
        assert tx is not None
        if tx.cancelled:
            return FilterResult()

        out = tx.get_filter_output(self.fullname())
        if out is None:
            out = PolicyActionFilterOutput()
        tag = self.yaml['tag']

        if (((eout := tx.get_ephemeral_filter_output(self.fullname()))
             is not None) and
            (tag in eout.unmet_precondition_tags)):
            return FilterResult()

        if tag in out.matched_tags:
            return FilterResult()

        if not self._match(tx):
            return FilterResult()
        self._apply_action(tx, tag)
        out.matched_tags.add(tag)
        tx.add_filter_output(self.fullname(), out)
        return FilterResult()
