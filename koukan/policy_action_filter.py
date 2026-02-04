# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Callable, Dict, List, Optional, Set
import logging
import random
from functools import reduce

from koukan.filter import TransactionMetadata
from koukan.filter_chain import Filter, FilterResult
from koukan.response import Response
from koukan.matcher_result import MatcherResult
from koukan.filter_output import FilterOutput
from koukan.rest_schema import WhichJson

class PolicyActionFilterOutput(FilterOutput):
    matched_tags_set : Set[str]
    matched_tags : List[str]

    matched_rules_set : Set[str]
    matched_rules : List[str]

    def __init__(self):
        self.matched_tags_set = set()
        self.matched_tags = []
        self.matched_rules_set = set()
        self.matched_rules = []

    def to_json(self, w : WhichJson):
        if w != WhichJson.DB_ATTEMPT:
            return None
        return {'matched_tags': self.matched_tags,
                'matched_rules': self.matched_rules}

    def match(self, yaml):
        raise NotImplementedError()

    def _add_tag(self, tag):
        assert tag not in self.matched_tags_set
        self.matched_tags_set.add(tag)
        self.matched_tags.append(tag)

    def _add_rule(self, rule):
        assert rule not in self.matched_rules_set
        self.matched_rules_set.add(rule)
        self.matched_rules.append(rule)



class _Output:
    unmet_precondition_tags : Set[str]
    def __init__(self):
        self.unmet_precondition_tags = set()


TransactionMatcher = Callable[[dict, TransactionMetadata], MatcherResult]


class PolicyActionFilter(Filter):
    yaml : dict
    matchers : Dict[str, TransactionMatcher]

    group_name : str
    rule_name : str

    def __init__(self, yaml : dict, matchers : Dict[str, TransactionMatcher]):
        self.yaml = yaml
        self.matchers = matchers

        self.group_name = yaml['tag']
        n = yaml.get('name', None)
        self.rule_name = n if n else self.group_name

    def _add_missing(self, tx, tag):
        if (out := tx.get_ephemeral_filter_output(self.fullname())) is None:
            out = tx.add_ephemeral_filter_output(self.fullname(), _Output())
        out.unmet_precondition_tags.add(tag)

    def _match_one(self, tx, yaml):
        matcher_name = yaml['matcher']
        matcher_yaml = dict(yaml)
        del matcher_yaml['matcher']
        if matcher := self.matchers.get(matcher_name, None):
            return matcher(matcher_yaml, tx)

        filter_output = tx.get_filter_output(matcher_name)
        if filter_output is None:
            return MatcherResult.PRECONDITION_UNMET
        return filter_output.match(matcher_yaml)

    def _match_rec(self, tx, yaml):
        if 'matcher' in yaml:
            return self._match_one(tx, yaml)
        assert len(yaml) == 1
        op = [k for k in yaml.keys()][0]
        assert op in {'any', 'all', 'not'}
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
        # empty match specification matches everything for
        # fallthrough/catchall at the end of a group
        match = self.yaml.get('match', None)
        if match is None:
            return True
        r = self._match_rec(tx, match)
        if r == MatcherResult.PRECONDITION_UNMET:
            self._add_missing(tx, self.group_name)
            return False

        return r == MatcherResult.MATCH

    def _sample_action(self, action):
        if not isinstance(action, list):
            return action
        denom = reduce(lambda x,y: x + y[0], action, 0)
        n = random.uniform(0, denom)
        for i in action:
            rate, act = i
            if n < rate:
                return act
            n -= rate
        assert False, 'bug'

    def _apply_action(self, tx, out):
        action = self._sample_action(self.yaml.get('action', 'MATCH'))

        if action == 'REJECT':
            out._add_rule(self.rule_name)
            out._add_tag(self.group_name)
            tx.fill_inflight_responses(
                Response(550, '5.6.0 message rejected ' + self.group_name))
        elif action == 'LOG':
            out._add_rule(self.rule_name)
        elif action == 'MATCH':
            out._add_rule(self.rule_name)
            out._add_tag(self.group_name)
        else:
            raise ValueError()

    def on_update(self, tx_delta : TransactionMetadata):
        tx = self.downstream_tx
        assert tx is not None
        if tx.cancelled:
            return FilterResult()

        out = tx.get_filter_output(self.fullname())
        if out is None:
            out = PolicyActionFilterOutput()

        if (((eout := tx.get_ephemeral_filter_output(self.fullname()))
             is not None) and
            (self.group_name in eout.unmet_precondition_tags)):
            return FilterResult()

        if (self.group_name in out.matched_tags_set or
            self.rule_name in out.matched_rules):
            return FilterResult()

        if not self._match(tx):
            return FilterResult()
        self._apply_action(tx, out)
        tx.add_filter_output(self.fullname(), out)
        return FilterResult()
