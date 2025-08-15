# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import unittest
import logging

from koukan.filter import Mailbox, Response, TransactionMetadata
from koukan.relay_auth_filter import RelayAuthFilter

class RelayAuthFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
            '%(message)s')

    def test_no_smtp_auth(self):
        filter = RelayAuthFilter()
        tx = TransactionMetadata()
        filter.wire_downstream(tx)

        delta = TransactionMetadata(mail_from=Mailbox('alice'))
        tx.merge_from(delta)
        filter.on_update(delta)
        self.assertEqual(tx.mail_response.code, 550)
        self.assertTrue(tx.mail_response.message.startswith('5.7.1'))


    def test_smtp_auth_fail(self):
        filter = RelayAuthFilter(smtp_auth = True)
        tx = TransactionMetadata()
        filter.wire_downstream(tx)

        delta = TransactionMetadata(mail_from=Mailbox('alice'))
        tx.merge_from(delta)
        filter.on_update(delta)
        self.assertEqual(tx.mail_response.code, 550)
        self.assertTrue(tx.mail_response.message.startswith('5.7.1'))

    def test_smtp_auth_success(self):
        filter = RelayAuthFilter(smtp_auth = True)
        tx = TransactionMetadata()
        filter.wire_downstream(tx)

        delta = TransactionMetadata(mail_from=Mailbox('alice'),
                                    rcpt_to=[Mailbox('bob')],
                                    smtp_meta = {'auth': True})
        tx.merge_from(delta)
        filter.on_update(delta)
        self.assertIsNone(tx.mail_response)

if __name__ == '__main__':
    unittest.main()
