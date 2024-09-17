from typing import List, Optional

import unittest
import logging
from threading import Thread
import time

from koukan.storage import Storage, TransactionCursor
from koukan.response import Response
from koukan.filter import Mailbox, TransactionMetadata
from koukan.rest_schema import BlobUri

from koukan.blob import InlineBlob

from koukan.storage_writer_filter import StorageWriterFilter

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

    def test_create(self):
        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: 'tx_rest_id',
            create_leased = True)
        tx = TransactionMetadata(
            host='submission',
            mail_from=Mailbox('alice'))
        filter.update(tx, tx.copy())
        cursor = filter.release_transaction_cursor()
        self.assertEqual(cursor.rest_id, 'tx_rest_id')


    def test_invalid(self):
        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: 'tx_rest_id')

        tx = TransactionMetadata(
            host = 'outbound-gw',
            body = '/transactions/123/body',
            body_blob = InlineBlob(b'wat'))
        upstream_delta = filter.update(tx, tx.copy())
        self.assertEqual(upstream_delta.data_response.code, 550)

    def test_cancel(self):
        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: 'tx_rest_id')

        tx = TransactionMetadata(host = 'outbound-gw')
        filter.update(tx, tx.copy())
        tx = TransactionMetadata(cancelled = True)
        filter.update(tx, tx.copy())

        cursor = self.storage.get_transaction_cursor()
        cursor.load(rest_id='tx_rest_id')
        self.assertEqual(cursor.final_attempt_reason, 'downstream cancelled')

    def test_cancel_noop(self):
        orig_tx_cursor = self.storage.get_transaction_cursor()
        orig_tx = TransactionMetadata(
            host = 'outbound-gw',
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob')])
        orig_tx_cursor.create('tx_rest_id', orig_tx, create_leased=True)
        orig_tx_cursor.load(start_attempt=True)
        orig_tx_cursor.write_envelope(
            TransactionMetadata(mail_response=Response(550)),
            finalize_attempt=True,
            final_attempt_reason='upstream permfail')

        filter = StorageWriterFilter(self.storage, rest_id='tx_rest_id')
        filter.get()
        cancel = TransactionMetadata(cancelled=True)
        filter.update(cancel, cancel.copy())

        orig_tx_cursor.load()
        self.assertIsNone(orig_tx_cursor.tx.cancelled)

    # representative of Exploder which writes body_blob=BlobReader
    def test_body_blob_reader(self):
        orig_tx_cursor = self.storage.get_transaction_cursor()
        orig_tx = TransactionMetadata(
            host = 'outbound-gw',
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob')])
        orig_tx_cursor.create('orig_tx_rest_id', orig_tx, create_leased=True)
        blob_writer = self.storage.create_blob(
            BlobUri(orig_tx_cursor.rest_id, tx_body=True))

        d = b'hello, '
        blob_writer.append_data(0, d)

        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: 'tx_rest_id')
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
        for i in range(0,5):
            tx = filter.get()
            if tx.mail_response:
                break
            filter.wait(1)
        else:
            self.fail('no mail_response')
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

        blob_reader = self.storage.get_blob_for_read(
            BlobUri('orig_tx_rest_id', tx_body=True))

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
        b1 = b'hello, '
        blob_writer.append_data(0, b1)
        blob_writer = filter.get_blob_writer(
            create=False,
            blob_rest_id='test_message_builder_blob')
        b2 = b'world!'
        blob_writer.append_data(len(b1), b2, len(b1) + len(b2))

        tx = TransactionMetadata(
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob')])
        tx.message_builder = {
            "text_body": [{
                "content_type": "text/plain",
                "content_uri": "/transactions/test_message_builder/blob/"
                               "test_message_builder_blob"
            }]
        }

        filter = StorageWriterFilter(
            self.storage,
            rest_id = 'test_message_builder')
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

        blob_reader = self.storage.get_blob_for_read(
            BlobUri(blob='test_message_builder_blob',
                    tx_id='test_message_builder'))
        self.assertIsNotNone(blob_reader)


        # now do it again reusing the same blob
        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: 'test_message_builder_reuse')
        tx = TransactionMetadata(
            host = 'outbound-gw',
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob')],
            message_builder = {
            "text_body": [{
                "content_type": "text/plain",
                "content_uri": "/transactions/test_message_builder/blob/"
                               "test_message_builder_blob"
            }]
            })
        upstream_delta = filter.update(tx, tx.copy())
        self.assertIsNone(upstream_delta.data_response)
        upstream_cursor = self.storage.get_transaction_cursor()
        upstream_cursor.load(rest_id='test_message_builder_reuse')
        self.assertEqual(
            upstream_cursor.tx.message_builder['text_body'][0]['blob_rest_id'],
            'test_message_builder_blob')


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

    def test_tx_body_inline_reuse(self):
        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: 'inline')
        b = 'hello, world!'
        tx = TransactionMetadata(
                host = 'outbound-gw',
                inline_body = b)
        # create w/ tx.inline_body
        filter.update(tx, tx.copy())

        filter2 = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: 'reuse')
        tx2 = TransactionMetadata(
                host = 'outbound-gw',
                body = '/transactions/inline/body')
        # create w/ body blob uri
        filter2.update(tx2, tx2.copy())

        blob_reader = self.storage.get_blob_for_read(
            BlobUri(tx_id='reuse', tx_body=True))
        self.assertIsNotNone(blob_reader)
        self.assertEqual(blob_reader.read(0), b.encode('utf-8'))

    def test_create_leased(self):
        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: 'inline',
            create_leased=True)

        tx = TransactionMetadata(
            host = 'outbound-gw',
            mail_from=Mailbox('alice'))
        filter.update(tx, tx.copy())

        self.assertIsNone(self.storage.load_one())

if __name__ == '__main__':
    unittest.main()
