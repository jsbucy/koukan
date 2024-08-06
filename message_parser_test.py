import unittest
import logging

from message_parser import MessageParser
from blob import InlineBlob

class MessageParserFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

    def test_smoke(self):
        parser = MessageParser(blob_factory=lambda: InlineBlob(b''),
                               max_inline=0)

        with open('testdata/trivial.msg', 'rb') as f:
            parsed = parser.parse(f)

        logging.debug('parsed.json %s', parsed.json)
        for blob in parsed.blobs:
            logging.debug('blob %s', blob.read(0))

        self.assertIn(['subject', 'hello'],
                      parsed.json['parts']['headers'])
        # NOTE: I can't figure out how to get email.parser to
        # leave the line endings alone
        self.assertEqual(parsed.blobs[0].read(0),
                         b'world!\n')



if __name__ == '__main__':
    unittest.main()
