# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import List, Optional

import unittest
import logging
from threading import Thread
import time

from koukan.storage import Storage, TransactionCursor
from koukan.storage_schema import BlobSpec, VersionConflictException
from koukan.response import Response
from koukan.filter import Mailbox, TransactionMetadata
from koukan.rest_schema import BlobUri

from koukan.blob import Blob, InlineBlob

from koukan.storage_writer_filter import StorageWriterFilter, Timeouts

import koukan.sqlite_test_utils as sqlite_test_utils

from koukan.message_builder import MessageBuilderSpec
from koukan.sender import Sender


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
        for i in range(0, 5):
            try:
                upstream_delta = filter.update(tx, tx_delta)
                self.assertTrue(len(upstream_delta.rcpt_response) <=
                                len(tx.rcpt_to))
            except VersionConflictException:
                logging.debug('VersionConflictException')
                filter.get()
                time.sleep(0.3)
                if i == 4:
                    raise

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

    def test_smoke(self):
        endpoint_yaml = {
            'sf_mode': 'upstream_unavailability'
        }
        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: 'tx_rest_id',
            create_leased = True,
            sender=Sender('ingress'),
            endpoint_yaml = lambda sender: endpoint_yaml)
        tx = TransactionMetadata(
            sender=Sender('ingress'),
            mail_from=Mailbox('alice'))
        filter.update(tx, tx.copy())
        upstream_cursor = filter.release_transaction_cursor(0)
        self.assertEqual(upstream_cursor.rest_id, 'tx_rest_id')

        filter = StorageWriterFilter(
            self.storage,
            rest_id = 'tx_rest_id',
            create_leased = True,
            endpoint_yaml = lambda sender: endpoint_yaml)
        prev = filter.get()
        tx = prev.copy()
        tx.rcpt_to = [Mailbox('bob@example.com')]
        filter.update(tx, prev.delta(tx))

        upstream_sender = None
        upstream_cursor2 = None
        def upstream(sender, cursor):
            nonlocal upstream_sender, upstream_cursor2
            upstream_sender = sender
            upstream_cursor2 = cursor
            return True

        filter = StorageWriterFilter(
            self.storage,
            rest_id = 'tx_rest_id',
            create_leased = True,
            tx_handler = upstream,
            endpoint_yaml = lambda sender: endpoint_yaml)
        prev = filter.get()
        logging.debug(prev.sender)
        tx = prev.copy()
        tx.rcpt_to.append(Mailbox('bob2@example.com'))
        filter.update(tx, prev.delta(tx))

        tx = filter.get()
        self.assertEqual(
            ['bob@example.com', 'bob2@example.com'],
            [r.mailbox for r in tx.rcpt_to])
        self.assertIsNotNone(upstream_sender)
        self.assertIsNotNone(u2 := upstream_cursor2())
        self.assertEqual(['bob2@example.com'],
                         [r.mailbox for r in u2.tx.rcpt_to])

    def test_store_and_forward_unavailability(self):
        timeouts = Timeouts()
        endpoint_yaml = {
            'sf_timeout': 1,
            'sf_mode': 'upstream_unavailability'
        }
        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: 'tx_rest_id',
            create_leased = True,
            endpoint_yaml = lambda sender: endpoint_yaml,
            sender=Sender('ingress'),
            timeouts=timeouts)
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'))
        filter.update(tx, tx.copy())
        tx = filter.get()
        self.assertIsNone(tx.mail_response)
        time.sleep(1)
        tx = filter.get()
        self.assertEqual(250, tx.mail_response.code)

        prev = tx.copy()
        tx.rcpt_to.append(Mailbox('bob@example.com'))
        filter.update(tx, prev.delta(tx))
        tx = filter.get()
        self.assertEqual([None], tx.rcpt_response)
        time.sleep(1)
        tx = filter.get()
        self.assertEqual([250], [r.code for r in tx.rcpt_response])

        prev = tx.copy()
        tx.body = InlineBlob(b'Hello, world!')
        filter.update(tx, prev.delta(tx))
        tx = filter.get()
        self.assertIsNone(tx.data_response)
        time.sleep(1)
        tx = filter.get()
        self.assertEqual(250, tx.data_response.code)

        cursor = self.storage.get_transaction_cursor()
        tx = cursor.load(rest_id='tx_rest_id')
        self.assertEqual({}, tx.notification)
        self.assertEqual({}, tx.retry)

    def test_body_blob(self):
        # from creation, gets handed off to OH
        upstream_filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: 'tx_rest_id',
            create_leased = True,
            sender = Sender('ingress'),
            endpoint_yaml = lambda sender: {})
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'), rcpt_to=[Mailbox('bob')])
        upstream_filter.update(tx, tx.copy())

        # RestHandler
        downstream_filter = StorageWriterFilter(
            self.storage,
            rest_id = 'tx_rest_id',
            create_leased = False,
            endpoint_yaml = lambda sender: {})
        blob_writer = downstream_filter.get_blob_writer(create=True, tx_body=True)
        d = b'hello, world!'
        chunk1 = 7
        blob_writer.append_data(0, d[0:chunk1])

        blob_writer = downstream_filter.get_blob_writer(
            create=False, tx_body=True)
        blob_writer.append_data(chunk1, d[chunk1:], len(d))

        tx = upstream_filter.get()
        self.assertTrue(upstream_filter.tx_group.tx_cursors[0].input_done)

    def test_cancel(self):
        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: 'tx_rest_id',
            sender = Sender('ingress'),
            endpoint_yaml = lambda sender: {})

        tx = TransactionMetadata(sender=Sender('gateway'))
        filter.update(tx, tx.copy())
        tx = TransactionMetadata(cancelled = True)
        filter.update(tx, tx.copy())

        cursor = self.storage.get_transaction_cursor()
        cursor.load(rest_id='tx_rest_id')
        self.assertEqual(cursor.final_attempt_reason, 'downstream cancelled')

    # failing because TxGroup doesn't replicate the noop
    # behavior from TxCursor
    def test_cancel_noop(self):
        orig_tx_cursor = self.storage.get_transaction_cursor()
        orig_tx = TransactionMetadata(
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob')])
        orig_tx_cursor.create('tx_rest_id', orig_tx, create_leased=True)
        orig_tx_cursor.start_attempt()
        attempt_delta = TransactionMetadata(mail_response = Response(550))
        orig_tx_cursor.write_envelope(
            tx_delta = TransactionMetadata(),
            attempt_delta = attempt_delta,
            finalize_attempt=True,
            final_attempt_reason='upstream permfail')

        filter = StorageWriterFilter(
            self.storage,
            rest_id='tx_rest_id',
            sender = Sender('ingress'),
            endpoint_yaml = lambda sender: {})
        tx = filter.get()
        self.assertIsNotNone(tx.final_attempt_reason)
        prev = tx.copy()
        tx.cancelled = True
        filter.update(tx, prev.delta(tx))

        orig_tx_cursor.load()
        self.assertIsNone(orig_tx_cursor.tx.cancelled)

    # representative of Exploder which writes body_blob=BlobReader
    # Exploder going away but what about add_route?
    def test_body_blob_reader(self):
        orig_tx = TransactionMetadata(
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob')])

        endpoint_yaml = {
            'sf_mode': 'mixed_data_response'
        }

        orig_filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: 'orig_tx_rest_id',
            sender = Sender('ingress'),
            endpoint_yaml = lambda sender: endpoint_yaml)

        orig_filter.update(orig_tx, orig_tx.copy())
        blob_writer = orig_filter.get_blob_writer(
            create=True, tx_body=True)

        d = b'hello, '
        blob_writer.append_data(0, d)

        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: 'tx_rest_id',
            create_leased=True,
            sender = Sender('ingress'),
            endpoint_yaml = lambda sender: endpoint_yaml)
        tx = TransactionMetadata(sender=Sender('exploder'))
        filter.update(tx, tx.copy())

        upstream_cursor = filter.release_transaction_cursor(0)
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
        for i in range(0, 5):
            try:
                upstream_cursor.write_envelope(
                    tx_delta = TransactionMetadata(),
                    attempt_delta=TransactionMetadata(
                        mail_response=Response(201)))
            except VersionConflictException:
                logging.debug('VersionConflictException')
                if i == 4:
                    raise
                time.sleep(0.3)
                upstream_cursor.load()

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
            tx_delta=TransactionMetadata(),
            attempt_delta=TransactionMetadata(rcpt_response=[Response(202)]))
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
            blob_writer.len(), d2, blob_writer.len() + len(d2))
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
            tx_delta=TransactionMetadata(),
            attempt_delta=TransactionMetadata(data_response=Response(203)))

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
            rest_id_factory = lambda: 'test_message_builder',
            sender = Sender('ingress'),
            endpoint_yaml = lambda sender: {})
        orig_tx = TransactionMetadata()
        orig_tx.body = MessageBuilderSpec(message_builder_json)
        orig_tx.body.parse_blob_specs()
        orig_filter.update(orig_tx, orig_tx.copy())

        logging.debug(orig_filter.tx_group.tx_cursors[0].blobs)
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
            rest_id_factory = lambda: 'test_message_builder_reuse',
            sender = Sender('ingress'),
            endpoint_yaml = lambda sender: {})
        message_builder_json['text_body'][0]['content'] = {'reuse_uri': '/transactions/test_message_builder/blob/test_message_builder_blob'}
        tx = TransactionMetadata(
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
            rest_id_factory = lambda: str(time.time()),
            sender = Sender('ingress'),
            endpoint_yaml = lambda sender: {})
        filter._create(TransactionMetadata())

        tx = TransactionMetadata(mail_from = Mailbox('alice'))
        t = self.start_update(filter, tx, tx)
        self.join(t, 3)
        self.assertIsNone(tx.mail_response)

    def testTimeoutRcpt(self):
        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: str(time.time()),
            sender = Sender('ingress'),
            endpoint_yaml = lambda sender: {})
        filter._create(TransactionMetadata())

        tx = TransactionMetadata(mail_from = Mailbox('alice'),
                                 rcpt_to = [Mailbox('bob')])
        t = self.start_update(filter, tx, tx)

        tx_cursor = self.storage.load_one()
        self.assertIsNotNone(tx_cursor)

        while tx_cursor.tx.mail_from is None:
            tx_cursor.wait()
        tx_cursor.write_envelope(
            tx_delta=TransactionMetadata(),
            attempt_delta=TransactionMetadata(mail_response=Response(201)))

        self.join(t, 3)
        tx = filter.get()
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual(tx.rcpt_response, [])

    def test_tx_body_inline_reuse(self):
        filter = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: 'inline',
            sender = Sender('ingress'),
            endpoint_yaml = lambda sender: {})
        b = b'hello, world!'
        tx = TransactionMetadata(
            body = InlineBlob(b, last=True))
        # create w/ tx.inline_body
        filter.update(tx, tx.copy())

        self.dump_db()

        filter2 = StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: 'reuse',
            sender = Sender('ingress'),
            endpoint_yaml = lambda sender: {})
        tx2 = TransactionMetadata(
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
            create_leased=True,
            sender = Sender('ingress'),
            endpoint_yaml = lambda sender: {})

        tx = TransactionMetadata(
            mail_from=Mailbox('alice'))
        filter.update(tx, tx.copy())

        self.assertIsNone(self.storage.load_one())

if __name__ == '__main__':
    unittest.main()
