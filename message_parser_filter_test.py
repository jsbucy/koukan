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
        with open('testdata/nested.msg', 'rb') as f:
            b = f.read()
        tx=TransactionMetadata(
            body_blob=InlineBlob(b, len(b)))

        upstream = FakeSyncFilter()
        def exp(tx, delta):
            upstream_delta = TransactionMetadata()
            if delta.parsed_blobs:
                upstream_delta.parsed_blob_ids = [
                    str(i) for i in range(0, len(delta.parsed_blobs)) ]
            elif delta.parsed_json:
                logging.debug(delta.parsed_json)
                upstream_delta.data_response = Response()
            tx.merge_from(upstream_delta)
            return upstream_delta
        upstream.add_expectation(exp)
        upstream.add_expectation(exp)

        filter = MessageParserFilter(upstream)
        upstream_delta = filter.on_update(tx, tx.copy())

if __name__ == '__main__':
    unittest.main()
