# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import unittest
import logging

from koukan.policy_factory import PolicyFactory
from koukan.filter import TransactionMetadata

class PolicyFactoryTest(unittest.TestCase):
    def test_smoke(self):
        yaml = {'modules': {
            'transaction_matcher': {
                'hello': 'koukan.hello_matcher',
                'nohello': 'koukan.hello_matcher:nomatch'
            }
        }}

        f = PolicyFactory()
        f.load_matchers(yaml)

        match_yaml = {}
        tx = TransactionMetadata()
        self.assertTrue(f.matchers['hello'](match_yaml, tx))
        self.assertFalse(f.matchers['nohello'](match_yaml, tx))

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d %(message)s')

    unittest.main()
