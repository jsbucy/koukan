from typing import List, Optional

import unittest
import logging
from threading import Thread
import time

from storage import Storage, TransactionCursor
from response import Response
from filter import Mailbox, TransactionMetadata

from blob import InlineBlob

from storage_writer_filter import StorageWriterFilter

class StorageWriterFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

        self.storage = Storage.get_sqlite_inmemory_for_test()

    def dump_db(self):
        for l in self.storage.db.iterdump():
            print(l)

    def update(self, filter, tx, timeout):
        filter.on_update(tx, timeout)

    def start_update(self, filter, tx, timeout=None):
        t = Thread(target=lambda: self.update(filter, tx, timeout=timeout),
                   daemon=True)
        t.start()
        time.sleep(0.1)
        return t

    def join(self, t, timeout=1):
        t.join(timeout=timeout)
        self.assertFalse(t.is_alive())

    def testBlob(self):
        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: str(time.time()))
        filter._create(TransactionMetadata(host = 'outbound-gw'))

        tx = TransactionMetadata(mail_from = Mailbox('alice'))
        t = self.start_update(filter, tx)

        tx_cursor = self.storage.load_one()
        self.assertIsNotNone(tx_cursor)

        while tx_cursor.tx.mail_from is None:
            tx_cursor.wait()
        tx_cursor.set_mail_response(Response(201))

        self.join(t)

        self.assertEqual(tx.mail_response.code, 201)

        tx = TransactionMetadata()
        tx.rcpt_to = [Mailbox('bob')]
        t = self.start_update(filter, tx)
        while len(tx_cursor.tx.rcpt_to) < 1:
            tx_cursor.wait()
        tx_cursor.add_rcpt_response([Response(202)])
        self.join(t)

        self.assertEqual(tx.rcpt_response[0].code, 202)

        blob_writer = self.storage.get_blob_writer()
        blob_writer.create('blob_rest_id')
        d = b'hello, '
        blob_writer.append_data(d)

        blob_reader = self.storage.get_blob_reader()
        self.assertIsNotNone(blob_reader.load(rest_id='blob_rest_id'))

        # update w/incomplete blob ->noop
        tx = TransactionMetadata()
        tx.body_blob = blob_reader
        t = self.start_update(filter, tx)
        self.join(t)

        d = b'world!'
        blob_writer.append_data(d, blob_writer.length + len(d))

        blob_reader.load()

        t = self.start_update(filter, tx)

        while not tx_cursor.tx.body:
            tx_cursor.wait()
        tx_cursor.set_data_response(Response(203))

        self.join(t)

        self.assertEqual(tx.data_response.code, 203)

    def testTimeoutMail(self):
        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: str(time.time()))
        filter._create(TransactionMetadata(host = 'outbound-gw'))

        tx = TransactionMetadata(mail_from = Mailbox('alice'))
        t = self.start_update(filter, tx, timeout=1)
        self.join(t, 2)
        self.assertEqual(tx.mail_response.code, 400)

    def testTimeoutRcpt(self):
        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: str(time.time()))
        filter._create(TransactionMetadata(host = 'outbound-gw'))

        tx = TransactionMetadata(mail_from = Mailbox('alice'),
                                 rcpt_to = [Mailbox('bob')])
        t = self.start_update(filter, tx, timeout=1)

        tx_cursor = self.storage.load_one()
        self.assertIsNotNone(tx_cursor)

        while tx_cursor.tx.mail_from is None:
            tx_cursor.wait()
        tx_cursor.set_mail_response(Response(201))

        self.join(t, 2)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [400])

    def testTimeoutData(self):
        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: str(time.time()))
        filter._create(TransactionMetadata(host = 'outbound-gw'))

        blob_writer = self.storage.get_blob_writer()
        blob_writer.create('blob_rest_id')
        d = b'hello, world!'
        blob_writer.append_data(d, len(d))

        blob_reader = self.storage.get_blob_reader()
        self.assertIsNotNone(blob_reader.load(rest_id='blob_rest_id'))

        tx = TransactionMetadata(mail_from = Mailbox('alice'),
                                 rcpt_to = [Mailbox('bob')],
                                 body_blob=blob_reader)
        t = self.start_update(filter, tx, timeout=1)

        tx_cursor = self.storage.load_one()
        self.assertIsNotNone(tx_cursor)

        while tx_cursor.tx.mail_from is None:
            tx_cursor.wait()
        tx_cursor.set_mail_response(Response(201))
        tx_cursor.add_rcpt_response([Response(202)])

        self.join(t, 2)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])
        self.assertEqual(tx.data_response.code, 400)


if __name__ == '__main__':
    unittest.main()
