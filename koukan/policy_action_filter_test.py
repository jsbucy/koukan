# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0

import logging
import unittest
import random

from koukan.filter import Mailbox, TransactionMetadata
from koukan.policy_action_filter import (
    PolicyActionFilter,
    PolicyActionFilterOutput,
    TransactionMatcher )
from koukan.blob import InlineBlob
from koukan.sender import Sender

from koukan.matcher_result import MatcherResult
from koukan.rest_schema import WhichJson

class FilterOutput:
    expect_call = True
    match_result = MatcherResult.NO_MATCH
    def __init__(self):
        pass
    def match(self, yaml : dict) -> MatcherResult:
        assert self.expect_call
        return self.match_result

class PolicyActionFilterTest(unittest.TestCase):
    def test_smoke(self):
        filter = PolicyActionFilter(
            {'tag': 'test_smoke',
             'match': {'matcher': 'my_filter'}},
            matchers={})
        filter.wire_downstream(TransactionMetadata())
        tx = filter.downstream_tx
        tx.sender = Sender('ingress', 'smtp-mx')
        tx.filter_output = {}
        prev = tx.copy()
        tx.mail_from = Mailbox('alice')

        filter_output = FilterOutput()
        tx.add_filter_output('my_filter', filter_output)
        filter.on_update(prev.delta(tx))
        self.assertIsNone(out := tx.get_filter_output(filter.fullname()))

        filter_output.match_result = MatcherResult.MATCH
        filter.on_update(prev.delta(tx))
        self.assertIsNotNone(out := tx.get_filter_output(filter.fullname()))
        self.assertIn('test_smoke', out.matched_tags)
        self.assertEqual({'matched_tags': ['test_smoke'],
                          'matched_rules': ['test_smoke']},
                         out.to_json(WhichJson.DB_ATTEMPT))

        filter_output.expect_call = False
        filter.on_update(prev.delta(tx))

    def test_trivial(self):
        filter = PolicyActionFilter(
            {'tag': 'test_smoke'},
            # no match: specification matches everything
            matchers={})
        filter.wire_downstream(TransactionMetadata())
        tx = filter.downstream_tx
        tx.sender = Sender('ingress', 'smtp-mx')
        tx.filter_output = {}
        prev = tx.copy()
        tx.mail_from = Mailbox('alice')

        filter_output = FilterOutput()
        tx.add_filter_output('my_filter', filter_output)
        filter.on_update(prev.delta(tx))
        self.assertIsNotNone(out := tx.get_filter_output(filter.fullname()))
        self.assertIn('test_smoke', out.matched_tags)

    def test_missing_input(self):
        filter = PolicyActionFilter(
            {'tag': 'test_smoke',
             'match': {'matcher': 'my_filter'}},
            matchers={})
        filter.wire_downstream(TransactionMetadata())
        tx = filter.downstream_tx
        tx.sender = Sender('ingress', 'smtp-mx')
        tx.filter_output = {}
        prev = tx.copy()
        tx.mail_from = Mailbox('alice')

        filter.on_update(prev.delta(tx))
        self.assertIsNone(out := tx.get_filter_output(filter.fullname()))

    def test_reject(self):
        filter = PolicyActionFilter(
            {'tag': 'test_smoke',
             'match': {'matcher': 'my_filter'},
             'action': 'REJECT'},
            matchers={})
        filter.wire_downstream(TransactionMetadata())
        tx = filter.downstream_tx
        tx.sender = Sender('ingress', 'smtp-mx')
        tx.filter_output = {}
        prev = tx.copy()
        tx.mail_from = Mailbox('alice')

        filter_output = FilterOutput()
        filter_output.match_result = MatcherResult.MATCH
        tx.add_filter_output('my_filter', filter_output)
        filter.on_update(prev.delta(tx))
        self.assertIsNotNone(out := tx.get_filter_output(filter.fullname()))
        self.assertIn('test_smoke', out.matched_tags)
        self.assertEqual(550, tx.mail_response.code)

    def test_matcher(self):
        class TestMatcher:
            m = True
            def match(self, yaml, tx : TransactionMetadata) -> bool:
                return self.m

        matcher = TestMatcher()
        filter = PolicyActionFilter(
            {'tag': 'test_smoke',
             'match': {'matcher': 'test_matcher'},
             'action': 'REJECT'},
            matchers={'test_matcher': matcher.match})

        filter.wire_downstream(TransactionMetadata())
        tx = filter.downstream_tx
        tx.sender = Sender('ingress', 'smtp-mx')
        tx.filter_output = {}
        prev = tx.copy()
        tx.mail_from = Mailbox('alice')

        filter_output = FilterOutput()
        tx.add_filter_output('test_matcher', filter_output)
        filter.on_update(prev.delta(tx))
        self.assertIsNotNone(out := tx.get_filter_output(filter.fullname()))
        self.assertIn('test_smoke', out.matched_tags)
        self.assertEqual(550, tx.mail_response.code)

    def test_preconditions(self):
        def fail(yaml, tx):
            assert False
        matchers = {
            'unmet': lambda yaml,tx: MatcherResult.PRECONDITION_UNMET,
            'assert': lambda yaml,tx: fail
        }

        f1 = PolicyActionFilter(
            {'tag': 'my_tag',
             'match': {'matcher': 'unmet'}},
            matchers=matchers)
        f2 = PolicyActionFilter(
            {'tag': 'my_tag',
             'match': {'matcher': 'assert'}},
            matchers=matchers)

        tx = TransactionMetadata()
        f1.wire_downstream(tx)
        f2.wire_downstream(tx)

        prev = tx.copy()
        tx.sender = Sender('ingress', 'smtp-mx')
        tx.filter_output = {}
        prev = tx.copy()
        tx.mail_from = Mailbox('alice')

        delta = prev.delta(tx)
        f1.on_update(delta)
        self.assertIsNotNone(
            eout := tx.get_ephemeral_filter_output(f1.fullname()))
        self.assertIsNone(
            out := tx.get_filter_output(f1.fullname()))
        self.assertIn('my_tag', eout.unmet_precondition_tags)
        f2.on_update(delta)
        self.assertIsNone(
            out := tx.get_filter_output(f1.fullname()))

    def test_expr(self):
        matchers = {
            'match': lambda tx, yaml: MatcherResult.MATCH,
            'nomatch': lambda tx, yaml: MatcherResult.NO_MATCH,
            'precond': lambda tx, yaml: MatcherResult.PRECONDITION_UNMET }

        yaml = {'all': [
            {'matcher': 'match'},
            {'matcher': 'match'}
        ]}
        f = PolicyActionFilter(
            yaml = yaml,
            matchers = matchers)
        self.assertEqual(MatcherResult.MATCH, f._match_rec(None, yaml))

        yaml = {'all': [
            {'matcher': 'match'},
            {'matcher': 'nomatch'}
            ]}
        f = PolicyActionFilter(
            yaml = yaml,
            matchers = matchers)
        self.assertEqual(MatcherResult.NO_MATCH, f._match_rec(None, yaml))

        yaml = {'any': [
            {'matcher': 'match'},
            {'matcher': 'nomatch'}
            ]}
        f = PolicyActionFilter(
            yaml = yaml,
            matchers = matchers)
        self.assertEqual(MatcherResult.MATCH, f._match_rec(None, yaml))

        yaml = {'any': [
            {'matcher': 'nomatch'},
            {'matcher': 'nomatch'}
            ]}
        f = PolicyActionFilter(
            yaml = yaml,
            matchers = matchers)
        self.assertEqual(MatcherResult.NO_MATCH, f._match_rec(None, yaml))

        yaml = {'not': {'matcher': 'match'}}
        f = PolicyActionFilter(
            yaml = yaml,
            matchers = matchers)
        self.assertEqual(MatcherResult.NO_MATCH, f._match_rec(None, yaml))

        yaml = {'not': {'matcher': 'nomatch'}}
        f = PolicyActionFilter(
            yaml = yaml,
            matchers = matchers)
        self.assertEqual(MatcherResult.MATCH, f._match_rec(None, yaml))

        yaml = {'not': {'matcher': 'precond'}}
        f = PolicyActionFilter(
            yaml = yaml,
            matchers = matchers)
        self.assertEqual(MatcherResult.PRECONDITION_UNMET,
                         f._match_rec(None, yaml))

    def test_percent(self):
        random.seed(1)

        filter = PolicyActionFilter(
            {'tag': 'test_smoke'},
            # no match: specification matches everything
            matchers={})

        actions = [
            [0.5, 'LOG'],
            [0.5, 'REJECT']
        ]

        self.assertEqual(
            'LRRLLLRRLL',
            ''.join([filter._sample_action(actions)[0] for i in range(0,10)]))


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d %(message)s')

    unittest.main()
