# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import unittest
import logging
from datetime import datetime, timezone
import tempfile
from functools import partial

from koukan.blob import Blob, InlineBlob
from koukan.filter import HostPort, Mailbox, TransactionMetadata
from koukan.response import Response
from koukan.storage_schema import BlobSpec
from koukan.rest_schema import BlobUri

from koukan.message_builder_filter import MessageBuilderFilter
from koukan.message_builder import MessageBuilderSpec

class MessageBuilderFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
            '%(message)s')
        self.filter = MessageBuilderFilter()
        self.filter.wire_downstream(TransactionMetadata())
        self.filter.wire_upstream(TransactionMetadata())

    def test_smoke(self):
        tx = self.filter.downstream_tx
        delta = TransactionMetadata()
        delta.remote_host=HostPort('example.com', port=25000)
        delta.mail_from=Mailbox('alice')
        delta.rcpt_to=[Mailbox('bob@domain')]
        delta.body = MessageBuilderSpec(
            {"text_body": [{
                "content_type": "text/html",
                "content": {"create_id": "blob_rest_id"}
                }]},
            blobs = [InlineBlob(b'hello, world!', last=True,
                                rest_id='blob_rest_id')]
        )
        delta.body.check_ids()

        tx.merge_from(delta)
        self.filter.on_update(delta)
        upstream_body = self.filter.upstream.body
        self.assertTrue(isinstance(upstream_body, Blob))
        self.assertTrue(upstream_body.finalized())
        self.assertNotEqual(
            upstream_body.pread(0).find(b'MIME-Version'), -1)

    def test_noop(self):
        tx = self.filter.downstream_tx
        body = InlineBlob(b'hello, world!')
        delta = TransactionMetadata(
            remote_host=HostPort('example.com', port=25000),
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')],
            body=body)

        tx.merge_from(delta)
        filter_result = self.filter.on_update(delta)
        self.assertEqual(body, self.filter.upstream.body)
        self.assertIsNone(filter_result.downstream_delta)

    def test_exception(self):
        tx = self.filter.downstream_tx
        delta = TransactionMetadata(
            remote_host=HostPort('example.com', port=25000),
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob@domain')])

        # MessageBuilder currently raises ValueError() if date is
        # missing unix_secs
        delta.body = MessageBuilderSpec(
            {'headers': [['date', {}]],
             "text_body": [{
                 "content_type": "text/html",
                 "content": {"create_id": "blob_rest_id"}
             }]},
            # non-finalized blob to tickle early-reject path
            blobs=[InlineBlob(b'hello, ', last=False,
                              rest_id='blob_rest_id')])
        delta.body.check_ids()

        tx.merge_from(delta)
        filter_result = self.filter.on_update(delta)
        self.assertEqual(550, filter_result.downstream_delta.data_response.code)


if __name__ == '__main__':
    unittest.main()
