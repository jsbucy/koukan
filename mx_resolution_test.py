import unittest
import logging

from mx_resolution import StaticResolutionFilter
from filter import HostPort, Resolution, TransactionMetadata
from fake_endpoints import FakeSyncFilter

class StaticResolutionFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

    def test_smoke(self):
        upstream = FakeSyncFilter()

        filter = StaticResolutionFilter(
            Resolution([HostPort('1.2.3.4', 25)]),
            upstream,
            suffix='.com',
            overwrite=True)

        def exp(tx, delta):
            self.assertEqual(tx.resolution.hosts[0].host,
                             '1.2.3.4')
            return TransactionMetadata()

        upstream.add_expectation(exp)

        tx = TransactionMetadata()
        tx.resolution = Resolution([HostPort('example.CoM', 25)])
        upstream_delta = filter.on_update(tx, tx.copy())

if __name__ == '__main__':
    unittest.main()
