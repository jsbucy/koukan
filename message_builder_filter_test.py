import unittest
import logging
from datetime import datetime, timezone
import tempfile

from blob import InlineBlob
from filter import HostPort, Mailbox, TransactionMetadata
from fake_endpoints import SyncEndpoint
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

        next = SyncEndpoint()
        message_builder = MessageBuilderFilter(
            self.storage, next)

        next.set_mail_response(Response(201))
        next.add_rcpt_response(Response(202))
        next.add_data_response(Response(203))
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

        message_builder.on_update(tx)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])
        self.assertEqual(tx.data_response.code, 203)
        msg = next.body_blob.read(0)
        self.assertNotEqual(msg.find(b'MIME-Version'), -1)

if __name__ == '__main__':
    unittest.main()
