from threading import Thread
import time
import unittest
import logging
import base64
import tempfile
import os

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

def setUpModule():
    global pg_factory
    pg_factory = testing.postgresql.PostgresqlFactory(cache_initialized_db=True)


def tearDownModule():
    global pg_factory
    pg_factory.clear_cache()


class StorageTestBase(unittest.TestCase):
    sqlite : bool
    version_cache : IdVersionMap

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s [%(thread)d] %(message)s')
        self.version_cache = IdVersionMap()

    def tearDown(self):
        self.s.engine.dispose()

    def dump_db(self):
        with self.s.begin_transaction() as db_tx:
            for l in db_tx.connection.iterdump():
                logging.debug(l)

    def test_basic(self):
        tx_writer = self.s.get_transaction_cursor()
        tx_writer.create('tx_rest_id', TransactionMetadata(
            remote_host=HostPort('remote_host', 2525), host='host'))

        tx_writer.write_envelope(TransactionMetadata(
            mail_from=Mailbox('alice')))

        tx_reader = self.s.load_one()
        self.assertIsNotNone(tx_reader)
        self.assertEqual(tx_writer.id, tx_reader.id)
        tx_reader.write_envelope(TransactionMetadata(
            mail_response=Response(450)))

        tx_writer.load()  # pick up version
        tx_writer.write_envelope(TransactionMetadata(
            rcpt_to=[Mailbox('bob')]))
        self.assertEqual(tx_writer.tx.remote_host.host, 'remote_host')
        self.assertEqual(tx_writer.tx.host, 'host')
        self.assertEqual(tx_writer.tx.mail_from.mailbox, 'alice')
        self.assertEqual(tx_writer.tx.mail_response.code, 450)
        self.assertEqual(tx_writer.tx.rcpt_to[0].mailbox, 'bob')

        with self.s.begin_transaction() as db_tx:
            self.assertTrue(tx_writer.check_input_done(db_tx))

        blob_writer = self.s.create_blob(
            BlobUri(tx_id='tx_rest_id', tx_body=True))

        with self.s.begin_transaction() as db_tx:
            self.assertFalse(tx_writer.check_input_done(db_tx))

        blob_writer.append_data(0, d=b'abc')
        self.assertFalse(blob_writer.last)

        tx_writer.load()
        tx_version = tx_writer.version
        blob_writer.append_data(3, d=b'xyz', content_length=9)
        self.assertFalse(blob_writer.last)

        # blob write should ping tx version
        tx_writer.load()
        self.assertNotEqual(tx_version, tx_writer.version)

        blob_writer = self.s.get_blob_for_append(
            BlobUri(tx_id='tx_rest_id', tx_body=True))
#        self.dump_db()
        blob_writer.append_data(6, d=b'uvw', content_length=9)
        self.assertTrue(blob_writer.last)
        del blob_writer

        tx_writer.load()
        tx_writer.write_envelope(
            TransactionMetadata(
                retry={'max_attempts': 100}))
        logging.info('test_basic check tx input done')
        self.assertTrue(tx_writer.input_done)

        tx_reader.load()
        self.assertEqual(tx_reader.tx.retry['max_attempts'], 100)
        tx_reader.write_envelope(TransactionMetadata(
            rcpt_response=[Response(456)]))

        tx_reader.write_envelope(TransactionMetadata(),
                                 finalize_attempt = True)

        blob_reader = self.s.get_blob_for_read(
            BlobUri(tx_id='tx_rest_id', tx_body=True))
        #self.assertIsNotNone( blob_reader
        b = blob_reader.read(0, 3)
        self.assertEqual(b'abc', b)
        b = blob_reader.read(3)
        self.assertEqual(b'xyzuvw', b)
        b = blob_reader.read(4, 3)
        self.assertEqual(b'yzu', b)

        tx_reader = self.s.load_one()
        self.assertFalse(tx_reader.no_final_notification)
        tx_reader.write_envelope(TransactionMetadata(), finalize_attempt=True)

        tx_reader = self.s.load_one()
        self.assertIsNotNone(tx_reader)
        self.assertEqual(tx_reader.id, tx_writer.id)
        self.assertIsNone(tx_reader.tx.mail_response)
        self.assertEqual(tx_reader.tx.rcpt_response, [])
        self.assertIsNone(tx_reader.tx.data_response)

        tx_reader.write_envelope(
            TransactionMetadata(),
            final_attempt_reason = 'retry max attempts',
            finalize_attempt = True)
        self.assertIsNone(self.s.load_one())
        tx_reader.write_envelope(
            TransactionMetadata(notification={'yes': True}))

        tx_reader = self.s.load_one()
        self.assertTrue(tx_reader.no_final_notification)
        tx_reader.write_envelope(TransactionMetadata(), notification_done=True)
        self.assertIsNone(self.s.load_one())

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
            mail_from=Mailbox('alice'), rcpt_to=[Mailbox('bob')],
            body = 'q')

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
        self.assertTrue(tx_writer.input_done)

    def test_recovery(self):
        # note storage_test_recovery.sql has
        # creation_session_id = (select min(id) from sessions)
        # since self.s session will be created before the one in the
        # .sql and we create a fresh db for each test
        self.load_recovery()
        #self.dump_db()
        self.s.recover()
        reader = self.s.load_one()
        self.assertIsNotNone(reader)
        self.assertEqual(reader.creation, 1707248590)
        self.assertEqual(reader.tx.mail_from.mailbox, 'alice@example.com')

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
        #self.dump_db()

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
        filename = tempdir.name + '/db'
        conn = sqlite3.connect(filename)
        cursor = conn.cursor()
        with open("init_storage.sql", "r") as f:
            cursor.executescript(f.read())

        self.s = Storage.connect_sqlite(self.version_cache, filename)

    def load_recovery(self):
        with open('storage_test_recovery.sql', 'r') as f:
            with self.s.begin_transaction() as db_tx:
                db_tx.connection.cursor().executescript(f.read())


class StorageTestSqliteInMemory(StorageTestBase):
    def setUp(self):
        super().setUp()
        self.s = Storage.get_sqlite_inmemory_for_test()

    def load_recovery(self):
        with open('storage_test_recovery.sql', 'r') as f:
            with self.s.begin_transaction() as db_tx:
                db_tx.connection.cursor().executescript(f.read())

class StorageTestPostgres(StorageTestBase):
    def postgres_url(self, unix_socket_dir, port, db):
        url = 'postgresql://postgres@/' + db + '?'
        url += ('host=' + unix_socket_dir)
        url += ('&port=%d' % port)
        return url

    def setUp(self):
        super().setUp()

        global pg_factory
        self.pg = pg_factory()
        unix_socket_dir = self.pg.base_dir + '/tmp'
        port = self.pg.dsn()['port']
        url = self.postgres_url(unix_socket_dir, port, 'postgres')
        logging.info('StorageTest setup_postgres %s', url)

        with psycopg.connect(url) as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                try:
                    cursor.execute('drop database storage_test;')
                except psycopg.errors.InvalidCatalogName:
                    pass
                cursor.execute('create database storage_test;')
                conn.commit()

        url = self.postgres_url(unix_socket_dir, port, 'storage_test')
        with psycopg.connect(url) as conn:
            with open('init_storage_postgres.sql', 'r') as f:
                with conn.cursor() as cursor:
                    cursor.execute(f.read())

        self.s = Storage.connect_postgres(
            self.version_cache,
            db_user='postgres', db_name='storage_test',
            unix_socket_dir=unix_socket_dir, port=port)

    def load_recovery(self):
        with open('storage_test_recovery.sql', 'r') as f:
            with self.s.begin_transaction() as db_tx:
                db_tx.connection.cursor().execute(f.read())



if __name__ == '__main__':
    unittest.main()
