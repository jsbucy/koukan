from typing import List, Optional

import unittest
import logging
from threading import Thread
import time

from storage import Storage, TransactionCursor
from response import Response
from fake_endpoints import SyncEndpoint
from filter import Mailbox, TransactionMetadata

from blob import InlineBlob

from storage_writer_filter import StorageWriterFilter

class StorageWriterFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

        self.storage = Storage()
        self.storage.connect(db=Storage.get_inmemory_for_test())

    def dump_db(self):
        for l in self.storage.db.iterdump():
            print(l)

    def update(self, filter, tx):
        filter.on_update(tx)

    def append(self, rresp : List[Optional[Response]], filter, blob, last):
        rresp.append(filter.append_data(last=last, blob=blob))

    def testBasic(self):
        filter = StorageWriterFilter(self.storage)
        filter._create()

        tx = TransactionMetadata()
        tx.host = 'outbound-gw'
        tx.mail_from = Mailbox('alice')
        t = Thread(target=lambda: self.update(filter, tx))
        t.start()

        tx_cursor = self.storage.load_one()
        self.assertIsNotNone(tx_cursor)

        time.sleep(0.1)
        while tx_cursor.tx.mail_from is None:
            tx_cursor.wait()
        tx_cursor.set_mail_response(Response(201))
        t.join(timeout=1)
        self.assertEqual(tx.mail_response.code, 201)

        tx = TransactionMetadata()
        tx.rcpt_to = [Mailbox('bob')]
        t = Thread(target=lambda: self.update(filter, tx))
        t.start()
        time.sleep(0.1)
        while len(tx_cursor.tx.rcpt_to) < 1:
            tx_cursor.wait()
        tx_cursor.add_rcpt_response([Response(202)])
        t.join(timeout=1)
        self.assertEqual(tx.rcpt_response[0].code, 202)

        filter.append_data(last=False, blob=InlineBlob(
            'from: alice\r\nto: bob\r\nsubject: hello\r\n\r\n'))

        blob_writer = self.storage.get_blob_writer()
        blob_writer.create('blob_rest_id')
        d = b'hello, world!\r\n'
        blob_writer.append_data(d, len(d))
        del blob_writer

        blob_reader = self.storage.get_blob_reader()
        self.assertIsNotNone(blob_reader.load(rest_id='blob_rest_id'))

        rresp = []
        t = Thread(target=lambda: self.append(rresp, filter, blob_reader, True))
        t.start()
        time.sleep(0.1)

        while tx_cursor.max_i is None or tx_cursor.max_i < 1:
            tx_cursor.wait()
        tx_cursor.set_data_response(Response(203))
        t.join(timeout=1)
        self.assertEqual(rresp[0].code, 203)


if __name__ == '__main__':
    unittest.main()
