# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Callable, Dict, List, Optional, Set
import logging
import random
from functools import reduce

from koukan.filter import TransactionMetadata
from koukan.filter_chain import ProxyFilter, FilterResult
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

    def copy(self):
        out = PolicyActionFilterOutput()
        out.matched_tags_set = set(self.matched_tags_set)
        out.matched_tags = list(self.matched_tags)
        out.matched_rules_set = set(self.matched_rules_set)
        out.matched_rules = list(self.matched_rules)
        return out

    def to_json(self, w : WhichJson):
        if w not in [WhichJson.DB_ATTEMPT,
                     WhichJson.REST_CREATE,
                     WhichJson.REST_UPDATE]:
            return None
        return {'matched_tags': self.matched_tags,
                'matched_rules': self.matched_rules}

    def match(self, yaml, rcpt_num : Optional[int]):
        if (tag := yaml.get('tag', None)) and (tag in self.matched_tags):
            return MatcherResult.MATCH
        if (rule := yaml.get('rule', None)) and (rule in self.matched_rules):
            return MatcherResult.MATCH
        return MatcherResult.NO_MATCH

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

    def copy(self):
        out = _Output()
        out.unmet_precondition_tags = set(self.unmet_precondition_tags)
        return out

TransactionMatcher = Callable[
    [dict, TransactionMetadata, Optional[int]], MatcherResult]


class PolicyActionFilter(ProxyFilter):
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
        eout = tx.get_ephemeral_filter_output(self.fullname())
        if eout is not None:
            eout = eout.copy()
        else:
            eout = _Output()
        tx.add_ephemeral_filter_output(self.fullname(), eout)
        eout.unmet_precondition_tags.add(tag)

    def _match_one(self, tx, yaml, rcpt_num : Optional[int]):
        matcher_name = yaml['matcher']
        matcher_yaml = dict(yaml)
        del matcher_yaml['matcher']
        if matcher := self.matchers.get(matcher_name, None):
            return matcher(matcher_yaml, tx, rcpt_num)

        filter_output = tx.get_filter_output(matcher_name)
        if filter_output is None:
            # per-rcpt matchers should not return this if used in
            # conjunction with a tx matcher, it must have a result by
            # the time this is invoked
            assert rcpt_num is None
            return MatcherResult.PRECONDITION_UNMET
        return filter_output.match(matcher_yaml, rcpt_num)

    def _match_rec(self, tx, yaml, rcpt_num : Optional[int]):
        if 'matcher' in yaml:
            return self._match_one(tx, yaml, rcpt_num)
        assert len(yaml) == 1
        op = [k for k in yaml.keys()][0]
        assert op in {'any', 'all', 'not'}
        arg = yaml[op]

        if op == 'not':
            assert isinstance(arg, dict)
            r = self._match_rec(tx, arg, rcpt_num)
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
                if (r := self._match_rec(tx, i, rcpt_num)) != MatcherResult.NO_MATCH:
                    return r
            return MatcherResult.NO_MATCH
        elif op == 'all':
            assert isinstance(arg, list)
            for i in arg:
                if (r := self._match_rec(tx, i, rcpt_num)) != MatcherResult.MATCH:
                    return r
            return MatcherResult.MATCH


    def _match(self, tx, rcpt_num : Optional[int]) -> bool:
        # empty match specification matches everything for
        # fallthrough/catchall at the end of a group
        match = self.yaml.get('match', None)
        if match is None:
            return True
        r = self._match_rec(tx, match, rcpt_num)
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

    def _apply_action(self, tx, out, rcpt_num : Optional[int]):
        action = self._sample_action(self.yaml.get('action', 'MATCH'))

        if action == 'REJECT':
            code = self.yaml.get('code', 550)
            err = self.yaml.get(
                'message', '5.6.0 message rejected ' + self.rule_name)
            out._add_rule(self.rule_name)
            out._add_tag(self.group_name)
            if rcpt_num is not None:
                tx.rcpt_response.extend(
                    [None] * (len(tx.rcpt_to) - len(tx.rcpt_response) - 1))
                assert len(tx.rcpt_response) == rcpt_num
                tx.rcpt_response.append(Response(code, err))
            else:
                tx.fill_inflight_responses(Response(code, err))
        elif action == 'LOG':
            out._add_rule(self.rule_name)
        elif action == 'MATCH':
            out._add_rule(self.rule_name)
            out._add_tag(self.group_name)
        else:
            assert False, 'invalid action ' + action

    def on_update(self, tx_delta : TransactionMetadata):
        tx = self.downstream_tx
        assert tx is not None
        if tx.cancelled:
            return FilterResult()

        out = tx.get_filter_output(self.fullname())
        if out is None:
            out = PolicyActionFilterOutput()
        else:
            out = out.copy()

        assert self.upstream_tx is not None
        if self.yaml.get('mode', None) == 'PER_RCPT':
            delta = tx_delta.copy()
            delta.rcpt_to = []
            self.upstream_tx.merge_from(delta)

            if not tx_delta.rcpt_to:
                return FilterResult()
            off = tx_delta.rcpt_to_list_offset
            assert off is not None
            for i in range(off, off + len(tx_delta.rcpt_to)):
                if self._match(tx, i):
                    self._apply_action(tx, out, i)
        else:
            self.upstream_tx.merge_from(tx_delta)

            if ((eout := tx.get_ephemeral_filter_output(self.fullname())) is not None) and (
                    self.group_name in eout.unmet_precondition_tags):
                return FilterResult()

            if (self.group_name in out.matched_tags_set or
                self.rule_name in out.matched_rules):
                return FilterResult()

            if not self._match(tx, rcpt_num=None):
                return FilterResult()
            self._apply_action(tx, out, rcpt_num=None)

        tx.add_filter_output(self.fullname(), out)
        return FilterResult()
