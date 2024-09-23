import unittest
import logging

from koukan.filter import Mailbox, TransactionMetadata
from koukan.response import Response

class FilterAdaptersTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')


if __name__ == '__main__':
    unittest.main()