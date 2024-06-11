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
        blob_writer = self.storage.get_blob_writer()
        blob_writer.create(rest_id='xyz')
        d = b'hello, world!'
        blob_writer.append_data(0, d, len(d))

        upstream = FakeSyncFilter()
        message_builder = MessageBuilderFilter(self.storage, upstream)

        tx = TransactionMetadata(
            remote_host=HostPort('example.com', port=25000),
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob@domain')])
        tx.message_builder = {
            "text_body": [
                {
                    "content_type": "text/html",
                    "blob_rest_id": "xyz"
                }
            ]
        }
        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.create('rest_id', tx, reuse_blob_rest_id=['xyz'])

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
