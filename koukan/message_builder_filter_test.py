import unittest
import logging
from datetime import datetime, timezone
import tempfile

from koukan.blob import InlineBlob
from koukan.filter import HostPort, Mailbox, TransactionMetadata
from koukan.fake_endpoints import FakeSyncFilter
from koukan.response import Response
from koukan.storage import Storage
from koukan.rest_schema import BlobUri

from koukan.message_builder_filter import MessageBuilderFilter

import koukan.sqlite_test_utils as sqlite_test_utils

class MessageBuilderFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')
        self.db_dir, self.db_url = sqlite_test_utils.create_temp_sqlite_for_test()
        self.storage = Storage.connect(self.db_url)

    def tearDown(self):
        self.db_dir.cleanup()

    def test_basic(self):
        upstream = FakeSyncFilter()
        message_builder = MessageBuilderFilter(self.storage, upstream)

        tx = TransactionMetadata(
            remote_host=HostPort('example.com', port=25000),
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob@domain')])
        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.create('tx_rest_id', tx)
        self.assertEqual(tx_cursor.tx.rest_id, 'tx_rest_id')
        blob_uri = BlobUri(tx_id='tx_rest_id', blob='blob_rest_id')
        blob_writer = self.storage.create_blob(blob_uri)
        d = b'hello, world!'
        blob_writer.append_data(0, d, len(d))

        tx_delta = TransactionMetadata()
        tx.message_builder = {
            "text_body": [
                {
                    "content_type": "text/html",
                    "blob_rest_id": "blob_rest_id"
                }
            ]
        }
        tx_cursor.load()
        tx_cursor.write_envelope(tx_delta, reuse_blob_rest_id=[blob_uri])

        def exp(tx, delta):
            self.assertTrue(delta.body_blob.finalized())
            self.assertNotEqual(
                delta.body_blob.read(0).find(b'MIME-Version'), -1)
            self.assertIsNone(tx.message_builder)
            self.assertIsNone(delta.message_builder)
            upstream_delta = TransactionMetadata(
                mail_response=Response(201),
                rcpt_response=[Response(202)],
                data_response=Response(203))
            tx.merge_from(upstream_delta)
            return upstream_delta

        upstream.add_expectation(exp)
        tx.rest_id = tx_cursor.rest_id  # XXX pass tx_cursor.tx instead?
        upstream_delta = message_builder.on_update(tx, tx.copy())
        self.assertEqual(upstream_delta.mail_response.code, 201)
        self.assertEqual([r.code for r in upstream_delta.rcpt_response], [202])
        self.assertEqual(upstream_delta.data_response.code, 203)

    def test_noop(self):
        upstream = FakeSyncFilter()
        message_builder = MessageBuilderFilter(self.storage, upstream)

        body_blob = InlineBlob(b'hello, world!')
        tx = TransactionMetadata(
            remote_host=HostPort('example.com', port=25000),
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')],
            body_blob=body_blob)

        def exp(tx, delta):
            self.assertEqual(tx.mail_from.mailbox, 'alice')
            self.assertEqual([m.mailbox for m in tx.rcpt_to], ['bob'])
            self.assertEqual(tx.body_blob, body_blob)
            self.assertEqual(delta.body_blob, body_blob)
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

if __name__ == '__main__':
    unittest.main()
