import unittest
import logging

from filter import Mailbox, Response, TransactionMetadata
from relay_auth_filter import RelayAuthFilter
from fake_endpoints import SyncEndpoint

class RelayAuthFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

    def test_basic(self):
        reject_all_filter = RelayAuthFilter()
        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        reject_all_filter.on_update(tx)
        self.assertEqual(tx.mail_response.code, 550)
        self.assertTrue(tx.mail_response.message.startswith('5.7.1'))


        require_auth_filter = RelayAuthFilter(smtp_auth = True)

        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        require_auth_filter.on_update(tx)
        self.assertEqual(tx.mail_response.code, 550)
        self.assertTrue(tx.mail_response.message.startswith('5.7.1'))

        next = SyncEndpoint()
        next.set_mail_response(Response())
        require_auth_filter = RelayAuthFilter(next = next, smtp_auth = True)

        tx = TransactionMetadata(mail_from=Mailbox('alice'),
                                 smtp_meta = {'auth': True})
        require_auth_filter.on_update(tx)
        self.assertTrue(tx.mail_response.ok())

if __name__ == '__main__':
    unittest.main()
