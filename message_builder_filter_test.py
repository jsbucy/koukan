import unittest
import logging
from datetime import datetime, timezone
import tempfile

from blob import InlineBlob
from filter import HostPort, Mailbox, TransactionMetadata
from fake_endpoints import FakeSyncFilter
from response import Response
from storage import Storage

from message_builder_filter import MessageBuilderFilter

class MessageBuilderFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')
        self.storage = Storage.get_sqlite_inmemory_for_test()

    def test_basic(self):
        upstream = FakeSyncFilter()
        message_builder = MessageBuilderFilter(self.storage, upstream)

        tx = TransactionMetadata(
            remote_host=HostPort('example.com', port=25000),
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob@domain')])
        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.create('tx_rest_id', tx)

        blob_writer = self.storage.create_blob(
            tx_rest_id='tx_rest_id', blob_rest_id='blob_rest_id')
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
        tx_cursor.write_envelope(tx_delta, reuse_blob_rest_id=['blob_rest_id'])

        def exp(tx, delta):
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
        tx.tx_db_id = tx_cursor.id
        upstream_delta = message_builder.on_update(tx, tx.copy())
        self.assertEqual(upstream_delta.mail_response.code, 201)
        self.assertEqual([r.code for r in upstream_delta.rcpt_response], [202])
        self.assertEqual(upstream_delta.data_response.code, 203)


if __name__ == '__main__':
    unittest.main()
