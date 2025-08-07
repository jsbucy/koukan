# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import unittest
import logging

from koukan.message_parser_filter import MessageParserFilter
from koukan.blob import InlineBlob
from koukan.filter import TransactionMetadata
from koukan.response import Response
from koukan.message_builder import MessageBuilderSpec


class MessageParserFilterTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
            '%(message)s')

    async def test_smoke(self):
        with open('testdata/multipart.msg', 'rb') as f:
            b = f.read()
        delta = TransactionMetadata(body=InlineBlob(b[0:10]))
        delta.options = {'receive_parsing': {}}

        filter = MessageParserFilter()
        filter.wire_downstream(TransactionMetadata())
        filter.downstream.merge_from(delta)
        filter.wire_upstream(TransactionMetadata())

        async def upstream():
            self.assertIsNone(filter.upstream.body)
            return TransactionMetadata()
        await filter.on_update(delta, upstream)

        async def upstream():
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
            upstream_delta = TransactionMetadata(
                data_response = Response())
            tx.merge_from(upstream_delta)
            return upstream_delta

        delta = TransactionMetadata(body=InlineBlob(b, last=True))
        filter.downstream.body = delta.body
        await filter.on_update(delta, upstream)


    async def test_noop(self):
        b = b'hello, world!'
        delta = TransactionMetadata(body=InlineBlob(b, last=True))

        filter = MessageParserFilter()
        filter.wire_downstream(TransactionMetadata())
        filter.downstream.merge_from(delta)
        filter.wire_upstream(TransactionMetadata())

        async def upstream():
            self.assertEqual(b, filter.upstream.body.pread(0))
            upstream_delta=TransactionMetadata(data_response=Response(201))
            filter.upstream.merge_from(upstream_delta)
            return upstream_delta
        await filter.on_update(delta, upstream)
        self.assertEqual(201, filter.downstream.data_response.code)


if __name__ == '__main__':
    unittest.main()
