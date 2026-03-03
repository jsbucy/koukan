# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0

from typing import Optional
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
    def match(self, yaml : dict, rcpt_num : Optional[int]) -> MatcherResult:
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

    # TODO mode: PER_RCPT

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
            def match(self, yaml, tx : TransactionMetadata,
                      rcpt_num : Optional[int]) -> bool:
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
            'unmet': lambda yaml, tx, rcpt: MatcherResult.PRECONDITION_UNMET,
            'assert': lambda yaml, tx, rcpt: fail
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

    def test_log_retirement(self):
        x_count = 0
        y_count = 0
        def x(yaml, tx, rcpt):
            nonlocal x_count
            logging.debug('x')
            x_count += 1
            if x_count == 1:
                return MatcherResult.PRECONDITION_UNMET
            return MatcherResult.MATCH
        def y(yaml, tx, rcpt):
            nonlocal y_count
            logging.debug('y')
            y_count += 1
            return MatcherResult.MATCH

        matchers = {'x': x, 'y': y}

        match_x_yaml = {
            'match': {'matcher': 'x'},
            'action': 'LOG',
            'name': 'match_x',
            'tag': 'my_group'}
        match_y_yaml = {
            'match': {'matcher': 'y'},
            'name': 'match_y',
            'tag': 'my_group'}

        filter_x = PolicyActionFilter(match_x_yaml, matchers=matchers)
        filter_y = PolicyActionFilter(match_y_yaml, matchers=matchers)

        tx = TransactionMetadata()
        filter_x.wire_downstream(tx)
        filter_y.wire_downstream(tx)

        # first call: x precondition noops group
        filter_x.on_update(TransactionMetadata())
        self.assertIsNotNone(
            eout := tx.get_ephemeral_filter_output(filter_x.fullname()))
        self.assertIn('my_group', eout.unmet_precondition_tags)

        filter_y.on_update(TransactionMetadata())
        self.assertEqual(1, x_count)
        self.assertEqual(0, y_count)
        tx.ephemeral_filter_output = None

        self.assertIsNone(out := tx.get_filter_output(filter_x.fullname()))

        # second call: x matches, log action does not retire group -> keep going
        filter_x.on_update(TransactionMetadata())
        filter_y.on_update(TransactionMetadata())

        self.assertEqual(2, x_count)
        self.assertEqual(1, y_count)

        self.assertIsNotNone(out := tx.get_filter_output(filter_x.fullname()))
        self.assertIn('match_x', out.matched_rules)
        self.assertIn('match_y', out.matched_rules)
        self.assertIn('my_group', out.matched_tags)

        # third call: both matchers previously matched -> noop
        filter_x.on_update(TransactionMetadata())
        filter_y.on_update(TransactionMetadata())

        self.assertEqual(2, x_count)
        self.assertEqual(1, y_count)


    def test_expr(self):
        matchers = {
            'match': lambda tx, yaml, rcpt: MatcherResult.MATCH,
            'nomatch': lambda tx, yaml, rcpt: MatcherResult.NO_MATCH,
            'precond': lambda tx, yaml, rcpt: MatcherResult.PRECONDITION_UNMET }

        match_yaml = {
            'all': [
                {'matcher': 'match'},
                {'matcher': 'match'}
            ]}
        f = PolicyActionFilter(
            yaml = {'tag': 'test_expr',
                    'match': match_yaml},
            matchers = matchers)
        self.assertEqual(MatcherResult.MATCH, f._match_rec(None, match_yaml, rcpt_num=None))

        match_yaml = {
            'all': [
                {'matcher': 'match'},
                {'matcher': 'nomatch'}
            ]}
        f = PolicyActionFilter(
            yaml = {'tag': 'test_expr',
                    'match': match_yaml},
            matchers = matchers)
        self.assertEqual(MatcherResult.NO_MATCH, f._match_rec(None, match_yaml, rcpt_num=None))

        match_yaml = {'any': [
            {'matcher': 'match'},
            {'matcher': 'nomatch'}
            ]}
        f = PolicyActionFilter(
            yaml = {'tag': 'test_expr',
                    'match': match_yaml},
            matchers = matchers)
        self.assertEqual(MatcherResult.MATCH, f._match_rec(None, match_yaml, rcpt_num=None))

        match_yaml = {'any': [
            {'matcher': 'nomatch'},
            {'matcher': 'nomatch'}
            ]}
        f = PolicyActionFilter(
            yaml = {'tag': 'test_expr',
                    'match': match_yaml},
            matchers = matchers)
        self.assertEqual(MatcherResult.NO_MATCH, f._match_rec(None, match_yaml, rcpt_num=None))

        match_yaml = {'not': {'matcher': 'match'}}
        f = PolicyActionFilter(
            yaml = {'tag': 'test_expr',
                    'match': match_yaml},
            matchers = matchers)
        self.assertEqual(MatcherResult.NO_MATCH, f._match_rec(None, match_yaml, rcpt_num=None))

        match_yaml = {'not': {'matcher': 'nomatch'}}
        f = PolicyActionFilter(
            yaml = {'tag': 'test_expr',
                    'match': match_yaml},
            matchers = matchers)
        self.assertEqual(MatcherResult.MATCH, f._match_rec(None, match_yaml, rcpt_num=None))

        match_yaml = {'not': {'matcher': 'precond'}}
        f = PolicyActionFilter(
            yaml = {'tag': 'test_expr',
                    'match': match_yaml},
            matchers = matchers)
        self.assertEqual(MatcherResult.PRECONDITION_UNMET,
                         f._match_rec(None, match_yaml, rcpt_num=None))

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

    def test_custom_err(self):
        filter = PolicyActionFilter(
            {'tag': 'test_smoke',
             'code': 450,
             'message': 'custom error text',
             'action': 'REJECT'},
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
        logging.debug(tx)
        self.assertEqual(450, tx.mail_response.code)
        self.assertEqual('custom error text', tx.mail_response.message)

    def test_policy_match(self):
        filter = PolicyActionFilter(
            {'tag': 'test_smoke',
             'action': 'REJECT',
             'match': {
                 'not': {
                     'any': [{
                         'matcher': 'koukan.policy_action_filter.PolicyActionFilter',
                         'tag': 'my_tag'
                     }, {
                         'matcher': 'koukan.policy_action_filter.PolicyActionFilter',
                         'rule': 'my_rule'
                     }]
                 }}
             },
            matchers={})

        filter.wire_downstream(TransactionMetadata())
        tx = filter.downstream_tx
        tx.add_filter_output(filter.fullname(), PolicyActionFilterOutput())
        prev = tx.copy()
        tx.mail_from = Mailbox('alice@example.com')
        filter.on_update(prev.delta(tx))
        self.assertEqual(550, tx.mail_response.code)

        filter.wire_downstream(TransactionMetadata())
        tx = filter.downstream_tx
        out = PolicyActionFilterOutput()
        out._add_tag('my_tag')
        tx.add_filter_output(filter.fullname(), out)
        prev = tx.copy()
        tx.mail_from = Mailbox('alice@example.com')
        filter.on_update(prev.delta(tx))
        self.assertIsNone(tx.mail_response)

        filter.wire_downstream(TransactionMetadata())
        tx = filter.downstream_tx
        out = PolicyActionFilterOutput()
        out._add_rule('my_rule')
        tx.add_filter_output(filter.fullname(), out)
        prev = tx.copy()
        tx.mail_from = Mailbox('alice@example.com')
        filter.on_update(prev.delta(tx))
        self.assertIsNone(tx.mail_response)



if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d %(message)s')

    unittest.main()
