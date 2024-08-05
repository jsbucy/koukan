import unittest
import logging

from message_parser import MessageParser
from blob import InlineBlob

class MessageParserFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

    def test_smoke(self):
        parser = MessageParser(blob_factory=lambda: InlineBlob())

        with open('testdata/multipart.msg', 'rb') as f:
            parser.parse(f)

if __name__ == '__main__':
    unittest.main()
