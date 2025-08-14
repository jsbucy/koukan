# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import unittest
import logging

from koukan.message_parser_filter import MessageParserFilter
from koukan.blob import InlineBlob
from koukan.filter import TransactionMetadata
from koukan.response import Response
from koukan.message_builder import MessageBuilderSpec


class MessageParserFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
            '%(message)s')

    def test_smoke(self):
        with open('testdata/multipart.msg', 'rb') as f:
            b = f.read()
        delta = TransactionMetadata(body=InlineBlob(b[0:10]))
        delta.options = {'receive_parsing': {}}

        filter = MessageParserFilter()
        filter.wire_downstream(TransactionMetadata())
        filter.downstream_tx.merge_from(delta)
        filter.wire_upstream(TransactionMetadata())

        filter.on_update(delta)
        self.assertIsNone(filter.upstream.body)


        delta = TransactionMetadata(body=InlineBlob(b, last=True))
        filter.downstream_tx.body = delta.body
        filter.on_update(delta)

        tx = filter.upstream
        logging.debug(tx)
        exp_blobs = [b'yolocat', b'yolocat2']
        self.assertTrue(isinstance(tx.body, MessageBuilderSpec))
        self.assertEqual(len(exp_blobs), len(tx.body.blobs))
        for i in range(0, len(exp_blobs)):
            self.assertEqual(tx.body.blobs[i].pread(0), exp_blobs[i])

        self.assertEqual(
            tx.body.json['parts']['content_type'],
            'multipart/mixed')

    def test_noop(self):
        # receive_parsing not in options
        body = InlineBlob(b'hello, world!', last=True)
        delta = TransactionMetadata(body=body)

        filter = MessageParserFilter()
        filter.wire_downstream(TransactionMetadata())
        filter.downstream_tx.merge_from(delta)
        filter.wire_upstream(TransactionMetadata())

        filter.on_update(delta)
        self.assertEqual(body, filter.upstream.body)



if __name__ == '__main__':
    unittest.main()
