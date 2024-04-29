import unittest
import logging

from filter import HostPort, Mailbox, TransactionMetadata
from remote_host_filter import RemoteHostFilter

class RemoteHostFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

    def testBasic(self):
        for addr in ['2600:3c01::f03c:93ff:fed6:3469', '45.79.77.192']:
            tx = TransactionMetadata(
                remote_host=HostPort(addr, 12345),
                mail_from=Mailbox('alice'))
            filter = RemoteHostFilter()
            filter.on_update(tx)
            logging.info('%s %s', tx.remote_hostname, tx.fcrdns)
            self.assertEqual(tx.remote_hostname, 'tachygraph.gloop.org.')
            self.assertTrue(tx.fcrdns)
            self.assertIsNone(tx.mail_response)

if __name__ == '__main__':
    unittest.main()
