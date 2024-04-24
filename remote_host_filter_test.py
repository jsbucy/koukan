import unittest
import logging

from filter import HostPort, TransactionMetadata
from remote_host_filter import RemoteHostFilter

class RemoteHostFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

    def testBasic(self):
        tx = TransactionMetadata(
            remote_host=HostPort('45.79.77.192', 12345))
        filter = RemoteHostFilter()
        filter.on_update(tx)
        logging.info('%s %s', tx.remote_hostname, tx.fcrdns)

if __name__ == '__main__':
    unittest.main()
