# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional

from typing import List
from threading import Thread
import time
import unittest
import logging
import base64
import os
from datetime import datetime, timedelta

from koukan.blob import Blob
from koukan.storage import BlobCursor, Storage, TransactionCursor
from koukan.storage_schema import BlobSpec, VersionConflictException
from koukan.response import Response
from koukan.filter import HostPort, Mailbox, TransactionMetadata
from koukan.rest_schema import BlobUri

from koukan.message_builder import MessageBuilderSpec

import koukan.postgres_test_utils as postgres_test_utils
import koukan.sqlite_test_utils as sqlite_test_utils

def setUpModule():
    postgres_test_utils.setUpModule()

def tearDownModule():
    postgres_test_utils.tearDownModule()


class StorageTestBase(unittest.TestCase):
    sqlite : bool

    def setUp(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
            '%(message)s')

    def tearDown(self):
        self.s._del_session()
        self.s.engine.dispose()

    def test_basic_lifecycle(self):
        downstream = self.s.get_transaction_cursor()
        downstream.create(
            'tx_rest_id',
            TransactionMetadata(
                remote_host=HostPort('remote_host', 2525), host='host'),
            create_leased=True)

        downstream.write_envelope(TransactionMetadata(
            mail_from=Mailbox('alice')))

        upstream = downstream
        del downstream

        buggy = self.s.get_transaction_cursor()
        self.assertIsNone(
            buggy.load(rest_id='tx_rest_id', start_attempt=True))

        self.assertIsNotNone(upstream.load(start_attempt=True))
        upstream.write_envelope(TransactionMetadata(
            mail_response=Response(450)))

        downstream = self.s.get_transaction_cursor()
        self.assertIsNotNone(downstream.load(rest_id='tx_rest_id'))
        downstream.write_envelope(TransactionMetadata(
            rcpt_to=[Mailbox('bob')]))
        self.assertEqual(downstream.tx.remote_host.host, 'remote_host')
        self.assertEqual(downstream.tx.host, 'host')
        self.assertEqual(downstream.tx.mail_from.mailbox, 'alice')
        self.assertEqual(downstream.tx.mail_response.code, 450)
        self.assertEqual(downstream.tx.rcpt_to[0].mailbox, 'bob')

        with self.s.begin_transaction() as db_tx:
            self.assertTrue(downstream.check_input_done(db_tx))

        downstream.write_envelope(
            TransactionMetadata(body=BlobSpec(create_tx_body=True)))
        blob_writer = downstream.get_blob_for_append(
            BlobUri(tx_id='tx_rest_id', tx_body=True))

        with self.s.begin_transaction() as db_tx:
            self.assertFalse(downstream.check_input_done(db_tx))
        logging.debug(blob_writer.blob_uri)
        blob_writer.append_data(0, d=b'abc')
        self.assertFalse(blob_writer.last)

        downstream.load()
        tx_version = downstream.version
        blob_writer.append_data(3, d=b'xyz', content_length=9)
        self.assertFalse(blob_writer.last)

        # blob write should ping tx version
        downstream.load()
        self.assertNotEqual(tx_version, downstream.version)

        blob_writer = downstream.get_blob_for_append(
            BlobUri(tx_id='tx_rest_id', tx_body=True))
        blob_writer.append_data(6, d=b'uvw', content_length=9)
        self.assertTrue(blob_writer.last)
        del blob_writer

        downstream.load()
        downstream.write_envelope(
            TransactionMetadata(
                retry={'max_attempts': 100}))
        logging.info('test_basic check tx input done')
        self.assertTrue(downstream.input_done)

        upstream.load()
        self.assertEqual(upstream.tx.retry['max_attempts'], 100)
        upstream.write_envelope(TransactionMetadata(
            rcpt_response=[Response(456)]))

        upstream.write_envelope(TransactionMetadata(),
                                finalize_attempt = True)

        blob_reader = upstream.tx.body
        self.assertTrue(isinstance(blob_reader, Blob))
        b = blob_reader.pread(0, 3)
        self.assertEqual(b'abc', b)
        b = blob_reader.pread(3)
        self.assertEqual(b'xyzuvw', b)
        b = blob_reader.pread(4, 3)
        self.assertEqual(b'yzu', b)

        upstream = self.s.load_one()
        self.assertFalse(upstream.no_final_notification)
        upstream.write_envelope(TransactionMetadata(), finalize_attempt=True)

        upstream = self.s.load_one()
        self.assertIsNotNone(upstream)
        self.assertEqual(upstream.id, downstream.id)
        self.assertIsNone(upstream.tx.mail_response)
        self.assertEqual(upstream.tx.rcpt_response, [])
        self.assertIsNone(upstream.tx.data_response)

        upstream.write_envelope(
            TransactionMetadata(),
            final_attempt_reason = 'retry max attempts',
            finalize_attempt = True)
        self.assertIsNone(self.s.load_one())

        self.assertTrue(self.s._refresh_session())
        downstream.load()
        downstream.write_envelope(
            TransactionMetadata(notification={'yes': True}))
        upstream = self.s.load_one()
        self.assertIsNotNone(upstream)
        self.assertTrue(upstream.no_final_notification)
        upstream.write_envelope(TransactionMetadata(), notification_done=True)
        self.assertIsNone(self.s.load_one())

        logging.debug(self.s.debug_dump())

    def test_mixed_notify_retry(self):
        cursor = self.s.get_transaction_cursor()
        cursor.create(
            'tx_rest_id',
            TransactionMetadata(
                remote_host=HostPort('remote_host', 2525), host='host'),
            create_leased=True)
        with self.assertRaises(AssertionError):
            cursor.write_envelope(TransactionMetadata(
                mail_from=Mailbox('alice'),
                retry=None,
                notification={'host': 'submission'}))

    def test_blob_8bitclean(self):
        cursor = self.s.get_transaction_cursor()
        cursor.create(
            'tx_rest_id',
            TransactionMetadata(body=BlobSpec(create_tx_body=True)),
            create_leased=True)
        blob_writer = cursor.get_blob_for_append(BlobUri('tx_rest_id', tx_body=True))
        # 32 random bytes, not valid utf8, etc.
        d = base64.b64decode('LNGxKynVCXMDfb6HD4PMryGN7/wb8WoAz1YcDgRBLdc=')
        self.assertEqual(d[23], 0)  # contains null octets
        with self.assertRaises(UnicodeDecodeError):
            s = d.decode('utf-8')
        blob_writer.append_data(0, d, len(d)*2)
        blob_writer.append_data(len(d), d, len(d)*2)

        tx_reader = self.s.get_transaction_cursor()
        tx_reader.load(rest_id='tx_rest_id')
        self.assertEqual(tx_reader.tx.body.pread(0), d+d)

    def test_body_reuse(self):
        tx_writer = self.s.get_transaction_cursor()
        tx_writer.create('tx_rest_id', TransactionMetadata(
            remote_host=HostPort('remote_host', 2525), host='host'))
        tx_writer.write_envelope(TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')]))
        tx_writer.write_envelope(
            TransactionMetadata(body=BlobSpec(create_tx_body=True)))
        blob_writer = tx_writer.get_blob_for_append(
            BlobUri(tx_id='tx_rest_id', tx_body=True))
        # incomplete blob
        d = b'hello, world!'
        blob_writer.append_data(0, d[0:7], None)

        # write a tx attempting to reuse a non-existent blob rest id,
        # this should fail
        tx_writer2 = self.s.get_transaction_cursor()
        tx = TransactionMetadata(
            remote_host=HostPort('remote_host', 2525), host='host',
            mail_from=Mailbox('alice'), rcpt_to=[Mailbox('bob')])

        with self.assertRaises(ValueError):
            tx_writer2.create('tx_rest_id2', TransactionMetadata(
                body=MessageBuilderSpec(
                    {"text_body": []},
                    blob_specs=[BlobSpec(reuse_uri=BlobUri(tx_id='tx_rest_id', tx_body=True))])))
        # non-finalized blobs cannot be reused
        with self.assertRaises(ValueError):
            tx.body = MessageBuilderSpec(
                {"text_body": []},
                blob_specs = [BlobSpec(reuse_uri=BlobUri(tx_id='tx_rest_id',
                                                         tx_body=True))])
            #'body_rest_id'
            tx_writer2 = self.s.get_transaction_cursor()
            tx_writer2.create('tx_rest_id2', tx)

        # shouldn't have been ref'd into tx2
        tx_writer2.load()
        self.assertIsNone(tx_writer2.tx.body)

        blob_writer.append_data(7, d[7:], len(d))

        logging.debug('test_body_reuse reuse success')

        tx_writer.load()
        tx_writer = self.s.get_transaction_cursor()
        tx.body = MessageBuilderSpec({"text_body": []}, blob_specs=[
            BlobSpec(reuse_uri=BlobUri(tx_id='tx_rest_id', tx_body=True))])
        tx_writer.create('tx_rest_id2', tx)
        self.assertTrue(tx_writer.input_done)

        tx_writer.load()
        self.assertTrue(isinstance(tx_writer.tx.body, Blob))
        self.assertTrue(tx_writer.tx.body.finalized())
        self.assertEqual(tx_writer.tx.body.content_length(), len(d))


    def test_blob_reuse(self):
        tx_writer = self.s.get_transaction_cursor()
        tx_writer.create('tx_rest_id1', TransactionMetadata(
            remote_host=HostPort('remote_host', 2525), host='host'))
        tx_writer.write_envelope(
            TransactionMetadata(
                mail_from=Mailbox('alice'),
                rcpt_to=[Mailbox('bob')],
                body = MessageBuilderSpec(
                    {"text_body": []},
                    blob_specs = [BlobSpec(create_id='blob_rest_id1'),
                                  BlobSpec(create_id='blob_rest_id2'),
                                  BlobSpec(create_id='blob_rest_id3')])))
        self.assertEqual(3, len(tx_writer.blobs))
        contents = []
        for i, blob in enumerate(tx_writer.blobs):
            b = b'hello, world %d!' % i
            contents.append(b)
            blob.append_data(0, b, last=True)

        tx_writer2 = self.s.get_transaction_cursor()
        tx_writer2.create('tx_rest_id2', TransactionMetadata(
            remote_host=HostPort('remote_host', 2525), host='host'))
        tx_writer2.write_envelope(
            TransactionMetadata(
                mail_from=Mailbox('alice'),
                rcpt_to=[Mailbox('bob')],
                body = MessageBuilderSpec(
                    {"text_body": []},
                    blob_specs=[BlobSpec(reuse_uri=tx_writer.blobs[0].blob_uri),
                                BlobSpec(reuse_uri=tx_writer.blobs[1].blob_uri),
                                BlobSpec(reuse_uri=tx_writer.blobs[2].blob_uri),
                                BlobSpec(create_id='blob_rest_id4')])))
        blob4 = BlobUri(tx_id='tx_rest_id2', blob='blob_rest_id4')
        blob_writer4 = tx_writer2.get_blob_for_append(blob4)
        b4 = b'another blob'
        blob_writer4.append_data(0, b4, len(b4))
        tx_writer2.load()

        # verify blobs were ref'd into tx_rest_id2
        self.assertEqual(4, len(tx_writer2.tx.body.blobs))
        exp_content = contents+[b4]
        for i,blob in enumerate(tx_writer2.tx.body.blobs):
            self.assertEqual(exp_content[i], blob.pread(0))


    def test_message_builder_no_blob(self):
        tx_writer = self.s.get_transaction_cursor()
        tx = TransactionMetadata(
            remote_host=HostPort('remote_host', 2525),
            host='host')
        tx.body = MessageBuilderSpec({ "headers": [ ["subject", "hello"] ] })
        tx_writer.create('tx_rest_id', tx)
        self.assertTrue(tx_writer.input_done)

        # add test
        # create tx w/message_builder that creates a blob
        # verify !input_done
        # write blob to completion
        # verify input_done

    def test_sessions(self):
        stale_session = self.s.session_id
        self.s = self._connect()

        self.assertEqual(0, self.s._gc_session(timedelta(seconds=1)))
        for i in range(0,4):
            logging.debug('%d', i)
            time.sleep(0.5)
            self.s._refresh_session()

        self.assertEqual(1, self.s._gc_session(timedelta(seconds=1)))
        self.assertFalse(self.s.testonly_get_session(stale_session)['live'])
        self.assertTrue(self.s.testonly_get_session(self.s.session_id)['live'])

    def test_recovery(self):
        old_session = self._connect()
        old_tx = old_session.get_transaction_cursor()
        old_tx.create('tx_rest_id', TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')]))
        old_tx.write_envelope(
            TransactionMetadata(body=BlobSpec(create_tx_body=True)))
        blob_writer = old_tx.get_blob_for_append(
            BlobUri(tx_id='tx_rest_id', tx_body=True))
        b = b'hello, world!'
        blob_writer.append_data(0, b, len(b))
        # don't cleanup the session
        old_session.session_id = None
        try:
            del old_session
        except:
            pass

        self.assertEqual(0, self.s.recover(session_ttl=timedelta(hours=1)))
        time.sleep(2)
        self.s._refresh_session()
        self.assertEqual(1, self.s.recover(session_ttl=timedelta(seconds=1)))

        reader = self.s.load_one()
        self.assertIsNotNone(reader)
        self.assertEqual(reader.id, old_tx.id)
        self.assertEqual(reader.tx.mail_from.mailbox, 'alice')

    def test_non_durable(self):
        writer = self.s.get_transaction_cursor()
        writer.create('xyz', TransactionMetadata(
            local_host=HostPort('local_host', 25),
            remote_host=HostPort('remote_host', 2525),
            host='host',
            retry={}))
        writer.write_envelope(TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')]))

        reader = self.s.load_one()
        self.assertIsNotNone(reader)

        reader.write_envelope(
            TransactionMetadata(),
            final_attempt_reason = 'retry policy max attempts',
            finalize_attempt = True)

        reader = self.s.get_transaction_cursor()
        self.assertTrue(reader.load(writer.id))

        reader = self.s.load_one()
        self.assertIsNone(reader)

    def test_gc(self):
        tx_writer = self.s.get_transaction_cursor()
        tx_writer.create(
            'xyz',
            TransactionMetadata(
                host='host',
                local_host=HostPort('local_host', 25),
                remote_host=HostPort('remote_host', 2525),
                retry={}))
        tx_writer.write_envelope(
            TransactionMetadata(body=BlobSpec(create_tx_body=True)))
        blob_writer = tx_writer.get_blob_for_append(
            BlobUri(tx_id='xyz', tx_body=True))
        d = b'hello, world!'
        blob_writer.append_data(0, d, len(d))

        tx_writer.load()
        tx_writer.write_envelope(TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')],))

        tx_reader = self.s.load_one()
        blob_reader = BlobCursor(self.s)
        self.assertEqual(tx_reader.id, tx_writer.id)

        # not expired, leased
        count = self.s.gc(ttl=timedelta(seconds=10))
        self.assertEqual(count, (0, 0))
        self.assertTrue(tx_reader.load())
        self.assertTrue(isinstance(tx_reader.tx.body, Blob))
        self.assertEqual(d, tx_reader.tx.body.pread(0))
        self.assertEqual(tx_reader.tx.body.content_length(), len(d))

        time.sleep(2)

        # expired, leased
        self.assertEqual((0, 0), self.s.gc(ttl=timedelta(seconds=1)))
        self.assertTrue(tx_reader.load())
        self.assertTrue(isinstance(tx_reader.tx.body, Blob))
        self.assertEqual(d, tx_reader.tx.body.pread(0))
        self.assertEqual(tx_reader.tx.body.content_length(), len(d))

        tx_reader.write_envelope(TransactionMetadata(),
                                 final_attempt_reason = 'upstream success',
                                 finalize_attempt = True)

        time.sleep(2)

        # expired, unleased
        self.assertEqual((1,1), self.s.gc(ttl=timedelta(seconds=1)))

        self.assertIsNone(tx_reader.load())

    def test_waiting_slowpath(self):
        writer = self.s.get_transaction_cursor()
        writer.create('xyz', TransactionMetadata(
            local_host=HostPort('local_host', 25),
            remote_host=HostPort('remote_host', 2525),
            host='host'))
        reader = self.s.get_transaction_cursor()
        self.assertIsNotNone(reader.load(writer.id))
        self.assertIsNone(reader.tx.mail_from)
        self.assertFalse(bool(reader.tx.rcpt_to))
        self.assertFalse(reader.wait(1))

        writer.write_envelope(TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')]))

        self.assertTrue(reader.wait(0))
        reader.load()
        self.assertEqual(reader.tx.mail_from.mailbox, 'alice')
        self.assertEqual(reader.tx.rcpt_to[0].mailbox, 'bob')

    def wait_for(self, reader, rv):
        logging.info('test wait')
        rv[0] = reader.wait(5)

    def test_waiting_inflight(self):
        tx_cursor = self.s.get_transaction_cursor()
        tx_cursor.create('xyz', TransactionMetadata(
            host='outbound',
            retry={}))

        # needs to be inflight to wait
        reader = self.s.load_one()
        self.assertIsNotNone(reader)
        self.assertEqual(reader.id, tx_cursor.id)
        self.assertIsNone(reader.tx.mail_from)
        self.assertFalse(bool(reader.tx.rcpt_to))

        rv = [None]
        t = Thread(target = lambda: self.wait_for(reader, rv))
        t.start()
        time.sleep(0.1)
        logging.info('test append')

        tx_cursor.load()
        tx_cursor.write_envelope(TransactionMetadata(
            mail_from=Mailbox('alice'), rcpt_to=[Mailbox('bob')]))

        t.join()

        self.assertTrue(rv[0])
        reader.load()
        self.assertEqual(reader.tx.mail_from.mailbox, 'alice')


    @staticmethod
    def wait_blob(reader, rv):
        rv[0] = reader.wait(5)

    def start_wait(self, reader, rv):
        t = Thread(target =
                   lambda: StorageTestBase.wait_blob(reader, rv))
        t.start()
        time.sleep(0.1)
        return t

    def join(self, t):
        t.join(timeout=5)
        self.assertFalse(t.is_alive())

    def reader(self, reader : TransactionCursor, dd : List[bytes]):
        d = bytes()
        while (reader.tx.body.content_length() is None or
               reader.tx.body.len() < reader.tx.body.content_length()):
            logging.info('reader %d', len(d))
            reader.load()
            d += reader.tx.body.pread(len(d))
        dd[0] = d

    def test_blob_waiting_poll(self):
        cursor = self.s.get_transaction_cursor()
        cursor.create(
            'tx_rest_id',
            TransactionMetadata(body=BlobSpec(create_tx_body=True)),
            create_leased=True)
        blob_writer = cursor.get_blob_for_append(
            BlobUri('tx_rest_id', tx_body=True))

        tx_reader = self.s.get_transaction_cursor()
        tx_reader.load(rest_id='tx_rest_id')

        dd = [None]
        t = Thread(target = lambda: self.reader(tx_reader, dd), daemon=True)
        t.start()

        d = None
        with open('/dev/urandom', 'rb') as f:
            d = f.read(128)

        for i in range(0, len(d)):
            logging.info('test_blob_waiting2 %d', i)
            blob_writer.append_data(i, d[i:i+1], len(d))
            self.assertEqual(blob_writer.length, i+1)

        t.join()
        self.assertFalse(t.is_alive())
        self.assertEqual(d, dd[0])
        self.assertEqual(len(d), tx_reader.tx.body.content_length())
        self.assertEqual(len(d), tx_reader.tx.body.len())


class StorageTestSqlite(StorageTestBase):
    def setUp(self):
        super().setUp()
        self.dir, self.db_url = sqlite_test_utils.create_temp_sqlite_for_test()
        self.s = self._connect()

    def tearDown(self):
        self.dir.cleanup()

    def _connect(self):
        return Storage.connect(self.db_url, session_uri='http://storage-test')


class StorageTestPostgres(StorageTestBase):
    pg : Optional[object] = None
    storage_yaml : Optional[dict] = None

    def setUp(self):
        super().setUp()

        self.s = self._connect()

    def _connect(self):
        if self.pg is None:
            self.storage_yaml = {}
            self.pg, self.pg_url = postgres_test_utils.setup_postgres()

        return Storage.connect(self.pg_url, session_uri='http://storage-test')


if __name__ == '__main__':
    unittest.main()
