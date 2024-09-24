# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import unittest
import logging

from koukan.filter import Mailbox, Response, TransactionMetadata
from koukan.relay_auth_filter import RelayAuthFilter
from koukan.fake_endpoints import FakeSyncFilter

class RelayAuthFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

    def test_no_smtp_auth(self):
        upstream = FakeSyncFilter()
        reject_all_filter = RelayAuthFilter(upstream)
        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        upstream_delta = reject_all_filter.on_update(tx, tx.copy())
        self.assertEqual(tx.mail_response.code, 550)
        self.assertTrue(tx.mail_response.message.startswith('5.7.1'))


    def test_smtp_auth_fail(self):
        upstream = FakeSyncFilter()
        require_auth_filter = RelayAuthFilter(
            upstream=upstream,
            smtp_auth = True)

        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        upstream_delta = require_auth_filter.on_update(tx, tx.copy())
        self.assertEqual(tx.mail_response.code, 550)
        self.assertTrue(tx.mail_response.message.startswith('5.7.1'))

    def test_smtp_auth_success(self):
        upstream = FakeSyncFilter()
        def exp(tx, delta):
            upstream_delta = TransactionMetadata(
                mail_response=Response())
            self.assertIsNotNone(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream.add_expectation(exp)

        require_auth_filter = RelayAuthFilter(
            upstream = upstream, smtp_auth = True)

        tx = TransactionMetadata(mail_from=Mailbox('alice'),
                                 smtp_meta = {'auth': True})
        require_auth_filter.on_update(tx, tx.copy())
        self.assertTrue(tx.mail_response.ok())

if __name__ == '__main__':
    unittest.main()
