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

class MessageBuilderFilterTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
            '%(message)s')
        self.filter = MessageBuilderFilter()
        self.filter.wire_downstream(TransactionMetadata())
        self.filter.wire_upstream(TransactionMetadata())

    async def test_smoke(self):
        tx = self.filter.downstream
        tx.remote_host=HostPort('example.com', port=25000)
        tx.mail_from=Mailbox('alice')
        tx.rcpt_to=[Mailbox('bob@domain')]
        tx.body = MessageBuilderSpec(
            {"text_body": [{
                "content_type": "text/html",
                "content": {"create_id": "blob_rest_id"}
                }]},
            blobs = [InlineBlob(b'hello, world!', last=True,
                                rest_id='blob_rest_id')]
        )
        tx.body.check_ids()

        async def exp(tx):
            logging.debug(tx)
            self.assertTrue(isinstance(tx.body, Blob))
            self.assertTrue(tx.body.finalized())
            self.assertNotEqual(
                tx.body.pread(0).find(b'MIME-Version'), -1)
            upstream_delta = TransactionMetadata(
                mail_response=Response(201),
                rcpt_response=[Response(202)],
                data_response=Response(203))
            tx.merge_from(upstream_delta)
            return upstream_delta

        await self.filter.update(
            self.filter.downstream.copy(),
            partial(exp, self.filter.upstream))
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])
        self.assertEqual(tx.data_response.code, 203)

    async def test_noop(self):
        tx = self.filter.downstream
        body = InlineBlob(b'hello, world!')
        tx.remote_host=HostPort('example.com', port=25000)
        tx.mail_from=Mailbox('alice')
        tx.rcpt_to=[Mailbox('bob')]
        tx.body=body

        async def exp(tx):
            logging.debug(tx)
            self.assertEqual(tx.mail_from.mailbox, 'alice')
            self.assertEqual([m.mailbox for m in tx.rcpt_to], ['bob'])
            self.assertEqual(tx.body, body)
            self.assertEqual(tx.body, body)
            upstream_delta = TransactionMetadata(
                mail_response=Response(201),
                rcpt_response=[Response(202)],
                data_response=Response(203))
            tx.merge_from(upstream_delta)
            return upstream_delta

        await self.filter.update(
            tx.copy(), partial(exp, self.filter.upstream))
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])
        self.assertEqual(tx.data_response.code, 203)

    async def test_exception(self):
        tx = self.filter.downstream
        tx.remote_host=HostPort('example.com', port=25000)
        tx.mail_from=Mailbox('alice')
        tx.rcpt_to=[Mailbox('bob@domain')]

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
        async def exp():
            self.fail()

        await self.filter.update(tx.copy(), exp)
        self.assertEqual(tx.mail_response.code, 250)
        self.assertEqual([r.code for r in tx.rcpt_response], [250])
        self.assertEqual(tx.data_response.code, 550)


if __name__ == '__main__':
    unittest.main()
