# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0

import logging
import unittest

from koukan.filter import Mailbox, TransactionMetadata
from koukan.policy_action_filter import (
    PolicyActionFilter,
    PolicyActionFilterOutput )
from koukan.blob import InlineBlob
from koukan.sender import Sender

# matches if yaml['match'] true
class FilterOutput:
    expect_call = True
    def __init__(self, yaml) :
        self.yaml = yaml
    def match(self, yaml : dict) -> bool:
        assert self.expect_call
        return self.yaml.get('match', False)

class PolicyActionFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d %(message)s')

    def test_smoke(self):
        match_yaml = {'filter_name': 'my_filter',
                      'match': False}
        filter = PolicyActionFilter({'tag': 'test_smoke',
                                     'match': [match_yaml]})
        filter.wire_downstream(TransactionMetadata())
        tx = filter.downstream_tx
        tx.sender = Sender('ingress', 'smtp-mx')
        tx.filter_output = {}
        prev = tx.copy()
        tx.mail_from = Mailbox('alice')

        filter_output = FilterOutput(match_yaml)
        tx.add_filter_output('my_filter', filter_output)
        filter.on_update(prev.delta(tx))
        self.assertIsNone(out := tx.get_filter_output(filter.fullname()))
        # self.assertNotIn('test_smoke', out.matched_tags)

        match_yaml['match'] = True
        filter.on_update(prev.delta(tx))
        self.assertIsNotNone(out := tx.get_filter_output(filter.fullname()))
        self.assertIn('test_smoke', out.matched_tags)

        filter_output.expect_call = False
        filter.on_update(prev.delta(tx))

    def test_missing_input(self):
        match_yaml = {'filter_name': 'my_filter',
                      'match': False}
        filter = PolicyActionFilter({'tag': 'test_smoke',
                                     'match': [match_yaml]})
        filter.wire_downstream(TransactionMetadata())
        tx = filter.downstream_tx
        tx.sender = Sender('ingress', 'smtp-mx')
        tx.filter_output = {}
        prev = tx.copy()
        tx.mail_from = Mailbox('alice')

        filter.on_update(prev.delta(tx))
        self.assertIsNone(out := tx.get_filter_output(filter.fullname()))

    def test_reject(self):
        match_yaml = {'filter_name': 'my_filter',
                      'match': True}
        filter = PolicyActionFilter({'tag': 'test_smoke',
                                     'match': [match_yaml],
                                     'action': 'REJECT'})
        filter.wire_downstream(TransactionMetadata())
        tx = filter.downstream_tx
        tx.sender = Sender('ingress', 'smtp-mx')
        tx.filter_output = {}
        prev = tx.copy()
        tx.mail_from = Mailbox('alice')

        filter_output = FilterOutput(match_yaml)
        tx.add_filter_output('my_filter', filter_output)
        filter.on_update(prev.delta(tx))
        self.assertIsNotNone(out := tx.get_filter_output(filter.fullname()))
        self.assertIn('test_smoke', out.matched_tags)
        self.assertEqual(550, tx.mail_response.code)

if __name__ == '__main__':
    unittest.main()
