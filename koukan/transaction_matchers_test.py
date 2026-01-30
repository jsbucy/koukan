# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import logging
import unittest

from koukan.filter import HostPort, TransactionMetadata
from koukan.transaction_matchers import (
    match_network_address,
    match_tls )
from koukan.matcher_result import MatcherResult

class NetworkAddressMatcherTest(unittest.TestCase):
    def test_smoke(self):
        tx = TransactionMetadata()
        self.assertEqual(MatcherResult.PRECONDITION_UNMET, match_network_address({'cidr': '1.0.0.0/8'}, tx))
        tx.remote_host = HostPort('1.2.3.4', 8000)
        self.assertEqual(MatcherResult.MATCH, match_network_address({'cidr': '1.0.0.0/8'}, tx))
        self.assertEqual(MatcherResult.NO_MATCH, match_network_address({'cidr': '1.1.0.0/16'}, tx))

class TlsMatcherTest(unittest.TestCase):
    def test_smoke(self):
        tx = TransactionMetadata()
        self.assertEqual(MatcherResult.PRECONDITION_UNMET, match_tls({}, tx))
        tx.smtp_meta = {'tls': True}
        self.assertEqual(MatcherResult.MATCH, match_tls({}, tx))

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d %(message)s')
    unittest.main()
