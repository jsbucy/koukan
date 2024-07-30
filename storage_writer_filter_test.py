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
        with self.storage.begin_transaction() as db_tx:
            for l in db_tx.connection.iterdump():
                logging.debug('%s', l)

    def update(self, filter, tx, tx_delta):
        upstream_delta = filter.update(tx, tx_delta)
        self.assertTrue(len(upstream_delta.rcpt_response) <=
                        len(tx.rcpt_to))

    def start_update(self, filter, tx, tx_delta):
        # xxx executor
        t = Thread(target=lambda: self.update(filter, tx, tx_delta),
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
        tx = filter.get()
        self.assertEqual(tx.mail_response.code, 201)

        tx_delta = TransactionMetadata(rcpt_to = [Mailbox('bob')])
        tx.merge_from(tx_delta)
        t = self.start_update(filter, tx, tx_delta)
        for i in range(0,5):
            if len(tx_cursor.tx.rcpt_to) == 1:
                break
            tx_cursor.wait(1)
            tx_cursor.load()
        else:
            self.fail('no rcpt')
        tx_cursor.write_envelope(
            TransactionMetadata(rcpt_response=[Response(202)]))
        self.join(t)

        tx = filter.get()
        self.assertEqual(
            [rr.code for rr in tx.rcpt_response], [202])

        blob_writer = self.storage.get_blob_writer()
        blob_writer.create('blob_rest_id')
        d = b'hello, '
        blob_writer.append_data(0, d)

        blob_reader = self.storage.get_blob_reader()
        self.assertIsNotNone(blob_reader.load(
            rest_id='blob_rest_id', no_tx_id=True))

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

        blob_reader.load(no_tx_id=True)

        tx_delta = TransactionMetadata(body_blob=blob_reader)
        tx.merge_from(tx_delta)
        t = self.start_update(filter, tx, tx_delta)

        for i in range(0,5):
            if tx_cursor.tx.body is not None:
                break
            tx_cursor.wait(1)
            tx_cursor.load()
        else:
            self.fail('no body')
        tx_cursor.write_envelope(
            TransactionMetadata(data_response=Response(203)))

        self.join(t)
        tx = filter.get()
        self.assertEqual(tx.data_response.code, 203)

    def test_message_builder(self):
        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: 'test_message_builder')
        filter._create(TransactionMetadata(host = 'outbound-gw'))

        blob_writer = filter.get_blob_writer(
            create=True,
            blob_rest_id='test_message_builder_blob')
        body = b'hello, world!'
        blob_writer.append_data(0, body, len(body))

        tx = TransactionMetadata(
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob')])
        tx.message_builder = {
            "text_body": [{
                "content_type": "text/plain",
                "content_uri": "/transactions/test_message_builder/blob/test_message_builder_blob"
            }]
        }

        t = self.start_update(filter, tx, tx.copy())

        upstream_cursor = self.storage.get_transaction_cursor()
        upstream_cursor.load(rest_id='test_message_builder')
        self.assertEqual(upstream_cursor.tx.message_builder,
                         tx.message_builder)
        upstream_delta = TransactionMetadata(
            mail_response=Response(201),
            rcpt_response=[Response(202)],
            data_response=Response(203))
        upstream_cursor.write_envelope(upstream_delta)

        self.join(t, timeout=5)

        blob_reader = self.storage.get_blob_reader()
        self.assertIsNotNone(blob_reader.load(
            rest_id='test_message_builder_blob', tx_id=upstream_cursor.id))


    def testTimeoutMail(self):
        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: str(time.time()))
        filter._create(TransactionMetadata(host = 'outbound-gw'))

        tx = TransactionMetadata(mail_from = Mailbox('alice'))
        t = self.start_update(filter, tx, tx)
        self.join(t, 3)
        self.assertIsNone(tx.mail_response)

    def testTimeoutRcpt(self):
        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: str(time.time()))
        filter._create(TransactionMetadata(host = 'outbound-gw'))

        tx = TransactionMetadata(mail_from = Mailbox('alice'),
                                 rcpt_to = [Mailbox('bob')])
        t = self.start_update(filter, tx, tx)

        tx_cursor = self.storage.load_one()
        self.assertIsNotNone(tx_cursor)

        while tx_cursor.tx.mail_from is None:
            tx_cursor.wait()
        tx_cursor.write_envelope(
            TransactionMetadata(mail_response=Response(201)))

        self.join(t, 3)
        tx = filter.get()
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
        self.assertIsNotNone(blob_reader.load(
            rest_id='blob_rest_id', no_tx_id=True))

        tx = TransactionMetadata(mail_from = Mailbox('alice'),
                                 rcpt_to = [Mailbox('bob')],
                                 body_blob=blob_reader)
        t = self.start_update(filter, tx, tx)

        tx_cursor = self.storage.load_one()
        self.assertIsNotNone(tx_cursor)

        while tx_cursor.tx.mail_from is None:
            tx_cursor.wait()
        tx_cursor.write_envelope(
            TransactionMetadata(mail_response=Response(201)))
        tx_cursor.write_envelope(
            TransactionMetadata(rcpt_response=[Response(202)]))

        self.join(t, 3)
        tx = filter.get()
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])
        self.assertIsNone(tx.data_response)

    def test_tx_body_inline_reuse(self):
        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: 'inline')
        b = 'hello, world!'
        tx = TransactionMetadata(
                host = 'outbound-gw',
                inline_body = b)
        filter.update(tx, tx.copy())

        filter2 = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: 'reuse')
        tx2 = TransactionMetadata(
                host = 'outbound-gw',
                body = '/transactions/inline/body')
        filter2.update(tx2, tx2.copy())

        cursor = self.storage.get_transaction_cursor()
        cursor.load(rest_id='reuse')
        blob_reader = self.storage.get_blob_reader()
        self.assertIsNotNone(
            blob_reader.load(rest_id=cursor.body_rest_id, tx_id=cursor.id))
        self.assertEqual(blob_reader.read(0), b.encode('utf-8'))


if __name__ == '__main__':
    unittest.main()
