# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import unittest
import logging
from datetime import datetime, timezone
import tempfile

from koukan.blob import Blob, InlineBlob
from koukan.filter import HostPort, Mailbox, TransactionMetadata
from koukan.fake_endpoints import FakeSyncFilter
from koukan.response import Response
from koukan.storage_schema import BlobSpec
from koukan.rest_schema import BlobUri

from koukan.message_builder_filter import MessageBuilderFilter
from koukan.message_builder import MessageBuilderSpec

class MessageBuilderFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

    def test_smoke(self):
        upstream = FakeSyncFilter()
        message_builder = MessageBuilderFilter(upstream)

        tx = TransactionMetadata(
            remote_host=HostPort('example.com', port=25000),
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob@domain')])
        tx_delta = TransactionMetadata()
        tx.body = MessageBuilderSpec(
            {"text_body": [{
                "content_type": "text/html",
                "content": {"create_id": "blob_rest_id"}
                }]},
            blobs = [InlineBlob(b'hello, world!', last=True,
                                rest_id='blob_rest_id')]
        )
        tx.body.check_ids()

        def exp(tx, delta):
            self.assertTrue(isinstance(delta.body, Blob))
            self.assertTrue(delta.body.finalized())
            self.assertNotEqual(
                delta.body.pread(0).find(b'MIME-Version'), -1)
            upstream_delta = TransactionMetadata(
                mail_response=Response(201),
                rcpt_response=[Response(202)],
                data_response=Response(203))
            tx.merge_from(upstream_delta)
            return upstream_delta

        upstream.add_expectation(exp)
        upstream_delta = message_builder.on_update(tx, tx.copy())
        self.assertEqual(upstream_delta.mail_response.code, 201)
        self.assertEqual([r.code for r in upstream_delta.rcpt_response], [202])
        self.assertEqual(upstream_delta.data_response.code, 203)

    def test_noop(self):
        upstream = FakeSyncFilter()
        message_builder = MessageBuilderFilter(upstream)

        body = InlineBlob(b'hello, world!')
        tx = TransactionMetadata(
            remote_host=HostPort('example.com', port=25000),
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')],
            body=body)

        def exp(tx, delta):
            self.assertEqual(tx.mail_from.mailbox, 'alice')
            self.assertEqual([m.mailbox for m in tx.rcpt_to], ['bob'])
            self.assertEqual(tx.body, body)
            self.assertEqual(delta.body, body)
            upstream_delta = TransactionMetadata(
                mail_response=Response(201),
                rcpt_response=[Response(202)],
                data_response=Response(203))
            tx.merge_from(upstream_delta)
            return upstream_delta
        upstream.add_expectation(exp)
        upstream_delta = message_builder.on_update(tx, tx.copy())
        self.assertEqual(upstream_delta.mail_response.code, 201)
        self.assertEqual([r.code for r in upstream_delta.rcpt_response], [202])
        self.assertEqual(upstream_delta.data_response.code, 203)

    def test_exception(self):
        upstream = FakeSyncFilter()
        message_builder = MessageBuilderFilter(upstream)

        tx = TransactionMetadata(
            remote_host=HostPort('example.com', port=25000),
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob@domain')])
        tx_delta = TransactionMetadata()
        # MessageBuilder currently raises ValueError() if date is
        # missing unix_secs
        tx.body = MessageBuilderSpec(
            {'headers': [['date', {}]],
             "text_body": [{
                 "content_type": "text/html",
                 "content": {"create_id": "blob_rest_id"}
             }]},
            # non-finalized blob to tickle early-reject path
            blobs=[InlineBlob(b'hello, ', last=False,
                              rest_id='blob_rest_id')])
        tx.body.check_ids()
        def exp(tx, delta):
            self.fail()
        upstream.add_expectation(exp)

        upstream_delta = message_builder.on_update(tx, tx.copy())
        self.assertEqual(upstream_delta.mail_response.code, 450)
        self.assertEqual([r.code for r in upstream_delta.rcpt_response], [450])
        self.assertEqual(upstream_delta.data_response.code, 550)


if __name__ == '__main__':
    unittest.main()
