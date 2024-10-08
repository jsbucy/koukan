# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import unittest
import logging

from koukan.message_parser_filter import MessageParserFilter
from koukan.blob import InlineBlob
from koukan.filter import TransactionMetadata
from koukan.response import Response

from koukan.fake_endpoints import FakeSyncFilter

class MessageParserFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

    def test_smoke(self):
        with open('testdata/multipart.msg', 'rb') as f:
            b = f.read()
        tx=TransactionMetadata(
            body_blob=InlineBlob(b, len(b)))
        tx.options = {'receive_parsing': None}

        upstream = FakeSyncFilter()
        def exp(tx, delta):
            upstream_delta = TransactionMetadata()

            exp_blobs = [b'yolocat', b'yolocat2']
            self.assertEqual(len(exp_blobs), len(tx.parsed_blobs))
            for i in range(0, len(exp_blobs)):
                self.assertEqual(tx.parsed_blobs[i].pread(0), exp_blobs[i])

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
