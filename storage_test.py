from typing import Optional

from threading import Thread
import time
import unittest
import logging
import base64
import tempfile
import os
from datetime import datetime, timedelta

import testing.postgresql

import sqlite3
import psycopg
import psycopg.errors
from storage import BlobCursor, Storage, TransactionCursor
from storage_schema import InvalidActionException, VersionConflictException
from response import Response
from filter import HostPort, Mailbox, TransactionMetadata
from rest_schema import BlobUri

from version_cache import IdVersionMap

import postgres_test_utils


def setUpModule():
    postgres_test_utils.setUpModule()

def tearDownModule():
    postgres_test_utils.tearDownModule()


class StorageTestBase(unittest.TestCase):
    sqlite : bool
    version_cache : IdVersionMap

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s [%(thread)d] %(message)s')
        self.version_cache = IdVersionMap()

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
        with self.assertRaises(VersionConflictException):
            buggy.load(rest_id='tx_rest_id', start_attempt=True)

        upstream.load(start_attempt=True)
        upstream.write_envelope(TransactionMetadata(
            mail_response=Response(450)))

        downstream = self.s.get_transaction_cursor()
        downstream.load(rest_id='tx_rest_id')
        downstream.write_envelope(TransactionMetadata(
            rcpt_to=[Mailbox('bob')]))
        self.assertEqual(downstream.tx.remote_host.host, 'remote_host')
        self.assertEqual(downstream.tx.host, 'host')
        self.assertEqual(downstream.tx.mail_from.mailbox, 'alice')
        self.assertEqual(downstream.tx.mail_response.code, 450)
        self.assertEqual(downstream.tx.rcpt_to[0].mailbox, 'bob')

        with self.s.begin_transaction() as db_tx:
            self.assertTrue(downstream.check_input_done(db_tx))

        blob_writer = self.s.create_blob(
            BlobUri(tx_id='tx_rest_id', tx_body=True))

        with self.s.begin_transaction() as db_tx:
            self.assertFalse(downstream.check_input_done(db_tx))

        blob_writer.append_data(0, d=b'abc')
        self.assertFalse(blob_writer.last)

        downstream.load()
        tx_version = downstream.version
        blob_writer.append_data(3, d=b'xyz', content_length=9)
        self.assertFalse(blob_writer.last)

        # blob write should ping tx version
        downstream.load()
        self.assertNotEqual(tx_version, downstream.version)

        blob_writer = self.s.get_blob_for_append(
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

        blob_reader = self.s.get_blob_for_read(
            BlobUri(tx_id='tx_rest_id', tx_body=True))
        b = blob_reader.read(0, 3)
        self.assertEqual(b'abc', b)
        b = blob_reader.read(3)
        self.assertEqual(b'xyzuvw', b)
        b = blob_reader.read(4, 3)
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

    def test_blob_8bitclean(self):
        blob_writer = BlobCursor(self.s)
        blob_writer.create()
        # 32 random bytes, not valid utf8, etc.
        d = base64.b64decode('LNGxKynVCXMDfb6HD4PMryGN7/wb8WoAz1YcDgRBLdc=')
        self.assertEqual(d[23], 0)  # contains null octets
        with self.assertRaises(UnicodeDecodeError):
            s = d.decode('utf-8')
        blob_writer.append_data(0, d, len(d)*2)
        blob_writer.append_data(len(d), d, len(d)*2)
        blob_reader = BlobCursor(self.s)
        blob_reader.load(blob_id=blob_writer.id)
        self.assertEqual(blob_reader.read(0), d+d)

    def test_body_reuse(self):
        tx_writer = self.s.get_transaction_cursor()
        tx_writer.create('tx_rest_id', TransactionMetadata(
            remote_host=HostPort('remote_host', 2525), host='host'))
        tx_writer.write_envelope(TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')]))
        tx_writer.write_envelope(TransactionMetadata())
        blob_writer = self.s.create_blob(
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
            tx_writer2.create('tx_rest_id2', tx, reuse_blob_rest_id=[
                BlobUri(tx_id='tx_rest_id', tx_body=True)])

        # non-finalized blobs cannot be reused
        with self.assertRaises(ValueError):
            tx.body = 'body_rest_id'
            tx_writer2 = self.s.get_transaction_cursor()
            tx_writer2.create('tx_rest_id2', tx, reuse_blob_rest_id=[
                BlobUri(tx_id='tx_rest_id', tx_body=True)])

        # shouldn't have been ref'd into tx2
        self.assertIsNone(self.s.get_blob_for_read(
            BlobUri(tx_id='tx_rest_id2', tx_body=True)))

        blob_writer.append_data(7, d[7:], len(d))

        logging.debug('test_body_reuse reuse success')

        tx_writer.load()
        tx_writer = self.s.get_transaction_cursor()
        tx_writer.create('tx_rest_id2', tx, reuse_blob_rest_id=[
            BlobUri(tx_id='tx_rest_id', tx_body=True)])
        self.assertTrue(tx_writer.input_done)

        blob_reader = self.s.get_blob_for_read(
            BlobUri(tx_id='tx_rest_id2', tx_body=True))
        self.assertEqual(blob_reader.content_length(), len(d))


    def test_blob_reuse(self):
        tx_writer = self.s.get_transaction_cursor()
        tx_writer.create('tx_rest_id1', TransactionMetadata(
            remote_host=HostPort('remote_host', 2525), host='host'))
        tx_writer.write_envelope(TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')]))
        blob1 = BlobUri(tx_id='tx_rest_id1', blob='blob_rest_id1')
        blob_writer1 = self.s.create_blob(blob1)
        b1 = b'hello, world!'
        blob_writer1.append_data(0, b1, len(b1))

        tx_writer2 = self.s.get_transaction_cursor()
        tx_writer2.create('tx_rest_id2', TransactionMetadata(
            remote_host=HostPort('remote_host', 2525), host='host'))
        tx_writer2.write_envelope(TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')]))
        blob2 = BlobUri(tx_id='tx_rest_id2', blob='blob_rest_id2')
        blob_writer2 = self.s.create_blob(blob2)
        b2 = b'another blob'
        blob_writer2.append_data(0, b2, len(b2))
        tx_writer2.load()
        reuse_blob_rest_id=[blob1]  # create_blob(blob2) refs into tx
        tx_writer2.write_envelope(
            TransactionMetadata(message_builder={}),
            reuse_blob_rest_id=reuse_blob_rest_id)

        for blob in reuse_blob_rest_id:
            blob_reader = BlobCursor(self.s)
            self.assertIsNotNone(blob_reader.load(blob2))

    def test_message_builder_no_blob(self):
        tx_writer = self.s.get_transaction_cursor()
        tx = TransactionMetadata(
            remote_host=HostPort('remote_host', 2525),
            host='host')
        tx.message_builder = { "headers": [ ["subject", "hello"] ] }
        tx_writer.create('tx_rest_id', tx)
        self.assertFalse(tx_writer.input_done)

        tx_writer = self.s.get_transaction_cursor()
        tx = TransactionMetadata(
            remote_host=HostPort('remote_host', 2525),
            host='host')
        tx.message_builder = { "headers": [ ["subject", "hello"] ] }
        tx_writer.create('tx_rest_id2', tx, message_builder_blobs_done=True)
        self.assertTrue(tx_writer.input_done)

    def test_sessions(self):
        self.assertTrue(self.s._refresh_session())

        self.s._gc_session(timedelta(minutes=1))

    def test_recovery(self):
        old_session = self._connect()
        old_session._init_session(datetime.fromtimestamp(1234567890))
        old_tx = old_session.get_transaction_cursor()
        old_tx.create('tx_rest_id', TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')]))
        blob_writer = old_session.create_blob(
            BlobUri(tx_id='tx_rest_id', tx_body=True))
        b = b'hello, world!'
        blob_writer.append_data(0, b, len(b))
        old_session._del_session()
        old_session.engine = None
        try:
            del old_session
        except:
            pass

        self.s.recover(ttl=timedelta(hours=24))

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
        tx_writer.create('xyz', TransactionMetadata(
            host='host',
            local_host=HostPort('local_host', 25),
            remote_host=HostPort('remote_host', 2525),
            retry={}))
        blob_writer = self.s.create_blob(BlobUri(tx_id='xyz', tx_body=True))
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
        count = self.s.gc(ttl=10)
        self.assertEqual(count, (0, 0))
        self.assertTrue(tx_reader.load())
        blob_reader = self.s.get_blob_for_read(
            BlobUri(tx_id='xyz', tx_body=True))
        self.assertEqual(blob_reader.content_length(), len(d))

        # expired, leased
        count = self.s.gc(ttl=0)
        self.assertEqual(count, (0, 0))
        self.assertTrue(tx_reader.load())

        blob_reader = self.s.get_blob_for_read(
            BlobUri(tx_id='xyz', tx_body=True))
        self.assertEqual(blob_reader.content_length(), len(d))

        tx_reader.write_envelope(TransactionMetadata(),
                              final_attempt_reason = 'upstream success',
                              finalize_attempt = True)

        # expired, unleased
        self.assertEqual(self.s.gc(ttl=0), (1,1))

        self.assertIsNone(self.s.get_blob_for_read(
            BlobUri(tx_id='xyz', tx_body=True)))

        # TODO fix preconditions and write test to ensure second
        # cursor cannot start attempt on leased tx.

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

    def reader(self, reader, dd):
        d = bytes()
        while (reader.content_length() is None or
               reader.len() < reader.content_length()):
            logging.info('reader %d', len(d))
            reader.load()
            d += reader.read(len(d))
        dd[0] = d

    def test_blob_waiting_poll(self):
        blob_writer = BlobCursor(self.s)
        blob_writer.create()

        reader = BlobCursor(self.s)
        reader.load(blob_id=blob_writer.id)

        dd = [None]
        t = Thread(target = lambda: self.reader(reader, dd), daemon=True)
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
        self.assertEqual(dd[0], d)
        self.assertEqual(reader.content_length(), len(d))
        self.assertEqual(reader.len(), len(d))


class StorageTestSqlite(StorageTestBase):
    def setUp(self):
        super().setUp()
        tempdir = tempfile.TemporaryDirectory()
        self.filename = tempdir.name + '/db'
        conn = sqlite3.connect(self.filename)
        cursor = conn.cursor()
        with open("init_storage.sql", "r") as f:
            cursor.executescript(f.read())

        self.s = Storage.connect_sqlite(self.version_cache, self.filename)

    def _connect(self):
        return Storage(self.s.tx_versions, self.s.engine)


class StorageTestSqliteInMemory(StorageTestBase):
    def setUp(self):
        super().setUp()
        self.s = Storage.get_sqlite_inmemory_for_test()

    def _connect(self):
        return Storage(self.s.tx_versions, self.s.engine)


class StorageTestPostgres(StorageTestBase):
    pg : Optional[object] = None
    storage_yaml : Optional[dict] = None

    def setUp(self):
        super().setUp()

        self.s = self._connect()

    def _connect(self):
        if self.pg is None:
            self.storage_yaml = {}
            self.pg = postgres_test_utils.setup_postgres(self.storage_yaml)

        return Storage.connect_postgres(
            self.version_cache,
            db_user='postgres',
            db_name='storage_test',
            unix_socket_dir=self.storage_yaml['unix_socket_dir'],
            port=self.storage_yaml['port'])


if __name__ == '__main__':
    unittest.main()
