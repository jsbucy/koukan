import unittest
import logging

from message_parser_filter import MessageParserFilter
from blob import InlineBlob
from filter import TransactionMetadata
from response import Response

from fake_endpoints import FakeSyncFilter

class MessageParserFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

    def test_smoke(self):
        with open('testdata/multipart.msg', 'rb') as f:
            b = f.read()
        tx=TransactionMetadata(
            body_blob=InlineBlob(b, len(b)))
        tx.options = {'receive_parsing': {}}

        upstream = FakeSyncFilter()
        def exp(tx, delta):
            upstream_delta = TransactionMetadata()

            self.assertEqual(
                tx.parsed_blobs[0].read(0), b'image/png')
            self.assertEqual(
                tx.parsed_json['parts']['content_type'],
                'multipart/mixed')
            upstream_delta.data_response = Response()
            tx.merge_from(upstream_delta)
            return upstream_delta
        upstream.add_expectation(exp)  # raw/body

        filter = MessageParserFilter(upstream)
        upstream_delta = filter.on_update(tx, tx.copy())


if __name__ == '__main__':
    unittest.main()
