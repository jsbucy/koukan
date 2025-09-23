# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import List, Optional

import unittest
import logging
from threading import Thread
import time

from koukan.storage import Storage, TransactionCursor
from koukan.storage_schema import BlobSpec
from koukan.response import Response
from koukan.filter import Mailbox, TransactionMetadata
from koukan.rest_schema import BlobUri

from koukan.blob import Blob, InlineBlob

from koukan.storage_writer_filter import StorageWriterFilter

import koukan.sqlite_test_utils as sqlite_test_utils

from koukan.message_builder import MessageBuilderSpec


class StorageWriterFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d %(message)s')
        self.db_dir, self.db_url = sqlite_test_utils.create_temp_sqlite_for_test()
        self.storage = Storage.connect(self.db_url, 'http://storage_writer_filter_test')

    def tearDown(self):
        self.db_dir.cleanup()

    def dump_db(self):
        with self.storage.begin_transaction() as db_tx:
            for l in db_tx.connection.iterdump():
                logging.debug('%s', l)

    def update(self, filter, tx, tx_delta):
        upstream_delta = filter.update(tx, tx_delta)
        self.assertTrue(len(upstream_delta.rcpt_response) <=
                        len(tx.rcpt_to))

    def start_update(self, filter, tx, tx_delta):
        logging.debug('start_update')

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

    def test_body_blob(self):
        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: 'tx_rest_id',
            create_leased = True)
        tx = TransactionMetadata(
            host='submission',
            mail_from=Mailbox('alice'), rcpt_to=[Mailbox('bob')])
        filter.update(tx, tx.copy())

        filter = StorageWriterFilter(
            self.storage,
            rest_id = 'tx_rest_id',
            create_leased = False)
        blob_writer = filter.get_blob_writer(create=True, tx_body=True)
        d = b'hello, world!'
        chunk1 = 7
        blob_writer.append_data(0, d[0:chunk1])

        blob_writer = filter.get_blob_writer(
            create=False, tx_body=True)
        blob_writer.append_data(chunk1, d[chunk1:], len(d))

        tx = filter.get()
        self.assertTrue(filter.tx_cursor.input_done)

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
        orig_tx_cursor.start_attempt()
        prev = orig_tx_cursor.tx.copy()
        tx = orig_tx_cursor.tx
        tx.mail_response = Response(550)
        orig_tx_cursor.write_envelope(
            prev.delta(tx),
            finalize_attempt=True,
            final_attempt_reason='upstream permfail')

        filter = StorageWriterFilter(self.storage, rest_id='tx_rest_id')
        tx = filter.get()
        self.assertIsNotNone(tx.final_attempt_reason)
        prev = tx.copy()
        tx.cancelled = True
        filter.update(tx, prev.delta(tx))

        orig_tx_cursor.load()
        self.assertIsNone(orig_tx_cursor.tx.cancelled)

    # representative of Exploder which writes body_blob=BlobReader
    def test_body_blob_reader(self):
        orig_tx = TransactionMetadata(
            host = 'outbound-gw',
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob')])

        orig_filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: 'orig_tx_rest_id')

        orig_filter.update(orig_tx, orig_tx.copy())
        blob_writer = orig_filter.get_blob_writer(
            create=True, tx_body=True)

        d = b'hello, '
        blob_writer.append_data(0, d)

        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: 'tx_rest_id',
            create_leased=True)
        tx = TransactionMetadata(host = 'outbound-gw')
        filter.update(tx, tx.copy())

        upstream_cursor = filter.release_transaction_cursor()
        upstream_cursor.start_attempt()

        tx = TransactionMetadata(mail_from = Mailbox('alice'))
        t = self.start_update(filter, tx, tx.copy())

        for i in range(0,5):
            if upstream_cursor.tx.mail_from is not None:
                break
            upstream_cursor.wait(1)
            upstream_cursor.load()
        else:
            self.fail('no mail_from')
        upstream_cursor.write_envelope(
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

        tx = filter.get()
        tx_delta = TransactionMetadata(rcpt_to = [Mailbox('bob')])
        tx.merge_from(tx_delta)
        t = self.start_update(filter, tx, tx_delta)
        for i in range(0,5):
            if len(upstream_cursor.tx.rcpt_to) == 1:
                break
            upstream_cursor.wait(1)
            upstream_cursor.load()
        else:
            self.fail('no rcpt')
        upstream_cursor.write_envelope(
            TransactionMetadata(rcpt_response=[Response(202)]))
        self.join(t)

        tx = filter.get()
        self.assertEqual(
            [rr.code for rr in tx.rcpt_response], [202])

        # update w/incomplete blob ->noop
        tx_delta = TransactionMetadata()
        tx_delta.body = orig_filter.get().body
        tx.merge_from(tx_delta)
        with self.assertRaises(Exception):
            filter.update(tx, tx_delta)

        d2 = b'world!'
        appended, length, content_length = blob_writer.append_data(
            blob_writer.length, d2, blob_writer.length + len(d2))
        self.assertTrue(appended)
        self.assertEqual(length, content_length)

        tx_delta = TransactionMetadata(body=orig_filter.get().body)
        tx.merge_from(tx_delta)
        t = self.start_update(filter, tx, tx_delta)

        for i in range(0,5):
            if upstream_cursor.tx.body is not None and upstream_cursor.tx.body.finalized():
                self.assertEqual(d + d2, upstream_cursor.tx.body.pread(0))
                break
            upstream_cursor.wait(1)
            upstream_cursor.load()
        else:
            self.fail('no body')
        upstream_cursor.write_envelope(
            TransactionMetadata(data_response=Response(203)))

        self.join(t)

        tx = filter.get()
        self.assertEqual(tx.data_response.code, 203)

    def test_message_builder_blob_reuse(self):
        message_builder_json = {
            "text_body": [{
                "content_type": "text/plain",
                "content": {"create_id": "test_message_builder_blob"}
            }]
        }

        orig_filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: 'test_message_builder')
        orig_tx = TransactionMetadata(host = 'outbound-gw')
        orig_tx.body = MessageBuilderSpec(message_builder_json)
        orig_tx.body.parse_blob_specs()
        orig_filter.update(orig_tx, orig_tx.copy())

        logging.debug(orig_filter.tx_cursor.blobs)
        blob_writer = orig_filter.get_blob_writer(
            create=False, blob_rest_id='test_message_builder_blob')
        b1 = b'hello, '
        blob_writer.append_data(0, b1)
        blob_writer = orig_filter.get_blob_writer(
            create=False, blob_rest_id='test_message_builder_blob')
        b2 = b'world!'
        blob_writer.append_data(len(b1), b2, len(b1) + len(b2))

        # now do it again reusing the same blob
        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: 'test_message_builder_reuse')
        message_builder_json['text_body'][0]['content'] = {'reuse_uri': '/transactions/test_message_builder/blob/test_message_builder_blob'}
        tx = TransactionMetadata(
            host = 'outbound-gw',
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob')],
            body = MessageBuilderSpec(message_builder_json))
        tx.body.parse_blob_specs()
        upstream_delta = filter.update(tx, tx.copy())
        self.assertIsNone(upstream_delta.data_response)
        upstream_cursor = self.storage.get_transaction_cursor()
        upstream_cursor.load(rest_id='test_message_builder_reuse')
        logging.debug(upstream_cursor.tx.body.json)
        self.assertEqual(
            upstream_cursor.tx.body.json['text_body'][0]['content']['create_id'],
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
        b = b'hello, world!'
        tx = TransactionMetadata(
            host = 'outbound-gw',
            body = InlineBlob(b, last=True))
        # create w/ tx.inline_body
        filter.update(tx, tx.copy())

        self.dump_db()

        filter2 = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: 'reuse')
        tx2 = TransactionMetadata(
                host = 'outbound-gw',
                body = BlobSpec(reuse_uri=BlobUri('inline', tx_body=True)))
        # create w/ body blob uri
        filter2.update(tx2, tx2.copy())

        tx_reader = self.storage.get_transaction_cursor()
        tx_reader.load(rest_id='inline')
        self.assertTrue(isinstance(tx_reader.tx.body, Blob))
        self.assertEqual(tx_reader.tx.body.pread(0), b)

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
