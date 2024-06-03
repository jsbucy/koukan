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

    def update(self, filter, tx, tx_delta, timeout):
        filter.update(tx, tx_delta, timeout)

    def start_update(self, filter, tx, tx_delta, timeout=None):
        t = Thread(target=lambda: self.update(
            filter, tx, tx_delta, timeout=timeout),
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

        tx = TransactionMetadata()
        tx_delta = TransactionMetadata(mail_from = Mailbox('alice'))
        tx.merge_from(tx_delta)
        t = self.start_update(filter, tx, tx_delta)

        tx_cursor = self.storage.load_one()
        self.assertIsNotNone(tx_cursor)

        for i in range(0,5):
            if tx_cursor.tx.mail_from is not None:
                break
            tx_cursor.wait()
        else:
            self.fail('no mail_from')
        tx_cursor.write_envelope(
            TransactionMetadata(mail_response=Response(201)))

        self.join(t)

        self.assertEqual(tx.mail_response.code, 201)

        tx_delta = TransactionMetadata(rcpt_to = [Mailbox('bob')])
        tx.merge_from(tx_delta)
        t = self.start_update(filter, tx, tx_delta)
        for i in range(0,5):
            if len(tx_cursor.tx.rcpt_to) == 1:
                break
            tx_cursor.wait(1)
        else:
            self.fail('no rcpt')
        tx_cursor.write_envelope(
            TransactionMetadata(rcpt_response=[Response(202)]))
        self.join(t)

        self.assertEqual(tx.rcpt_response[0].code, 202)

        blob_writer = self.storage.get_blob_writer()
        blob_writer.create('blob_rest_id')
        d = b'hello, '
        blob_writer.append_data(0, d)

        blob_reader = self.storage.get_blob_reader()
        self.assertIsNotNone(blob_reader.load(rest_id='blob_rest_id'))

        # update w/incomplete blob ->noop
        tx_delta = TransactionMetadata()
        tx_delta.body_blob = blob_reader
        tx.merge_from(tx_delta)
        t = self.start_update(filter, tx, tx_delta)
        self.join(t)

        d = b'world!'
        appended, length, content_length = blob_writer.append_data(
            blob_writer.length, d, blob_writer.length + len(d))
        self.assertTrue(appended)
        self.assertEqual(length, content_length)

        blob_reader.load()

        tx_delta = TransactionMetadata(body_blob=blob_reader)
        tx.merge_from(tx_delta)
        t = self.start_update(filter, tx, tx_delta)

        for i in range(0,5):
            if tx_cursor.tx.body is not None:
                break
            tx_cursor.wait(1)
        else:
            self.fail('no body')
        tx_cursor.write_envelope(
            TransactionMetadata(data_response=Response(203)))

        self.join(t)

        self.assertEqual(tx.data_response.code, 203)

    def testTimeoutMail(self):
        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: str(time.time()))
        filter._create(TransactionMetadata(host = 'outbound-gw'))

        tx = TransactionMetadata(mail_from = Mailbox('alice'))
        t = self.start_update(filter, tx, tx, timeout=2)
        self.join(t, 3)
        self.assertIsNone(tx.mail_response)

    def testTimeoutRcpt(self):
        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: str(time.time()))
        filter._create(TransactionMetadata(host = 'outbound-gw'))

        tx = TransactionMetadata(mail_from = Mailbox('alice'),
                                 rcpt_to = [Mailbox('bob')])
        t = self.start_update(filter, tx, tx, timeout=2)

        tx_cursor = self.storage.load_one()
        self.assertIsNotNone(tx_cursor)

        while tx_cursor.tx.mail_from is None:
            tx_cursor.wait()
        tx_cursor.write_envelope(
            TransactionMetadata(mail_response=Response(201)))

        self.join(t, 3)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual(tx.rcpt_response, [])

    def testTimeoutData(self):
        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: str(time.time()))
        filter._create(TransactionMetadata(host = 'outbound-gw'))

        blob_writer = self.storage.get_blob_writer()
        blob_writer.create('blob_rest_id')
        d = b'hello, world!'
        blob_writer.append_data(0, d, len(d))

        blob_reader = self.storage.get_blob_reader()
        self.assertIsNotNone(blob_reader.load(rest_id='blob_rest_id'))

        tx = TransactionMetadata(mail_from = Mailbox('alice'),
                                 rcpt_to = [Mailbox('bob')],
                                 body_blob=blob_reader)
        t = self.start_update(filter, tx, tx, timeout=2)

        tx_cursor = self.storage.load_one()
        self.assertIsNotNone(tx_cursor)

        while tx_cursor.tx.mail_from is None:
            tx_cursor.wait()
        tx_cursor.write_envelope(
            TransactionMetadata(mail_response=Response(201)))
        tx_cursor.write_envelope(
            TransactionMetadata(rcpt_response=[Response(202)]))

        self.join(t, 3)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])
        self.assertIsNone(tx.data_response)


if __name__ == '__main__':
    unittest.main()
