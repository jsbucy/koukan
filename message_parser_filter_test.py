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

        upstream = FakeSyncFilter()
        def exp(tx, delta):
            upstream_delta = TransactionMetadata()
            if delta.parsed_blobs:
                logging.debug('blobs %s', delta.parsed_blobs)
            if delta.parsed_json:
                logging.debug('parsed json %s', delta.parsed_json)
                upstream_delta.data_response = Response()
            tx.merge_from(upstream_delta)
            return upstream_delta
        upstream.add_expectation(exp)  # raw/body

        filter = MessageParserFilter(upstream)
        upstream_delta = filter.on_update(tx, tx.copy())

if __name__ == '__main__':
    unittest.main()
