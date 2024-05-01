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
from storage import Storage, TransactionCursor
from storage_schema import InvalidActionException, VersionConflictException
from response import Response
from filter import HostPort, Mailbox, TransactionMetadata

def setUpModule():
    global pg_factory
    pg_factory = testing.postgresql.PostgresqlFactory(cache_initialized_db=True)


def tearDownModule():
    global pg_factory
    pg_factory.clear_cache()


class StorageTestBase(unittest.TestCase):
    sqlite : bool
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s [%(thread)d] %(message)s')

    def tearDown(self):
        self.s.engine.dispose()

    def dump_db(self):
        with self.s.conn() as conn:
            for l in conn.connection.iterdump():
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
        tx_reader.set_mail_response(Response(450))

        tx_writer.load()  # pick up version
        tx_writer.write_envelope(TransactionMetadata(
            rcpt_to=[Mailbox('bob')]))
        self.assertEqual(tx_writer.tx.remote_host.host, 'remote_host')
        self.assertEqual(tx_writer.tx.host, 'host')
        self.assertEqual(tx_writer.tx.mail_from.mailbox, 'alice')
        self.assertEqual(tx_writer.tx.mail_response.code, 450)
        self.assertEqual(tx_writer.tx.rcpt_to[0].mailbox, 'bob')

        blob_writer = self.s.get_blob_writer()
        blob_writer.create('blob_rest_id')

        blob_writer.append_data(d=b'abc')
        self.assertFalse(blob_writer.last)
        blob_writer.append_data(d=b'xyz', content_length=9)
        self.assertFalse(blob_writer.last)
        blob_writer.append_data(d=b'uvw', content_length=9)
        self.assertTrue(blob_writer.last)

        tx_writer.write_envelope(TransactionMetadata(body='tx_rest_id',
                                                     max_attempts =100),
                                 reuse_blob_rest_id=['blob_rest_id'])

        logging.info('test_basic check tx input done')
        self.assertTrue(tx_writer.input_done)

        tx_reader.load()
        self.assertEqual(tx_reader.tx.max_attempts, 100)
        tx_reader.add_rcpt_response([Response(456, 'busy')])
        tx_reader.write_envelope(TransactionMetadata(),
                                 #output_done = False,
                                 finalize_attempt = True)

        expected_content = [
            b'abc',
            b'xyz',
            b'uvw'
        ]
        blob_reader = self.s.get_blob_reader()
        self.assertEqual(blob_reader.load(rest_id='blob_rest_id'), 9)
        b = blob_reader.read(0, 3)
        self.assertEqual(b'abc', b)
        b = blob_reader.read(3)
        self.assertEqual(b'xyzuvw', b)
        b = blob_reader.read(4, 3)
        self.assertEqual(b'yzu', b)

        r2 = self.s.load_one()
        self.assertIsNotNone(r2)
        self.assertEqual(r2.id, tx_writer.id)
        self.assertIsNone(r2.tx.mail_response)
        self.assertEqual(r2.tx.rcpt_response, [])
        self.assertIsNone(r2.tx.data_response)

        r2.write_envelope(TransactionMetadata(),
                          final_attempt_reason = 'retry max attempts',
                          finalize_attempt = True)

        self.assertIsNone(self.s.load_one())

    def test_blob_8bitclean(self):
        blob_writer = self.s.get_blob_writer()
        blob_writer.create('blob_rest_id')
        # 32 random bytes, not valid utf8, etc.
        d = base64.b64decode('LNGxKynVCXMDfb6HD4PMryGN7/wb8WoAz1YcDgRBLdc=')
        self.assertEqual(d[23], 0)  # contains null octets
        with self.assertRaises(UnicodeDecodeError):
            s = d.decode('utf-8')
        blob_writer.append_data(d, len(d)*2)
        blob_writer.append_data(d, len(d)*2)
        del blob_writer
        blob_reader = self.s.get_blob_reader()
        blob_reader.load(rest_id='blob_rest_id')
        self.assertEqual(blob_reader.read(0), d+d)


    def test_blob_reuse(self):
        tx_writer = self.s.get_transaction_cursor()
        tx_writer.create('tx_rest_id', TransactionMetadata(
            remote_host=HostPort('remote_host', 2525), host='host'))
        tx_writer.write_envelope(TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')]))
        tx_writer.write_envelope(TransactionMetadata())
        blob_writer = self.s.get_blob_writer()
        blob_writer.create('body_rest_id')
        d = b'hello, world!'
        blob_writer.append_data(d, len(d))

        # write a tx attempting to reuse a non-existent blob rest id,
        # this should fail
        tx_writer = self.s.get_transaction_cursor()
        tx = TransactionMetadata(
            remote_host=HostPort('remote_host', 2525), host='host',
            mail_from=Mailbox('alice'), rcpt_to=[Mailbox('bob')])
        tx.body = 'q'

        with self.assertRaises(ValueError):
            tx_writer.create('tx_rest_id2', tx, reuse_blob_rest_id=[tx.body])

        reader = self.s.get_transaction_cursor()
        self.assertIsNone(reader.load(rest_id='tx_rest_id2'))

        tx.body = 'body_rest_id'
        tx_writer = self.s.get_transaction_cursor()
        tx_writer.create('tx_rest_id2', tx, reuse_blob_rest_id=[tx.body])
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
            host='host'))
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
        blob_writer = self.s.get_blob_writer()
        blob_writer.create(rest_id=str(time.time()))
        d = b'hello, world!'
        blob_writer.append_data(d, len(d))

        writer = self.s.get_transaction_cursor()
        writer.create('xyz', TransactionMetadata(
            host='host',
            local_host=HostPort('local_host', 25),
            remote_host=HostPort('remote_host', 2525)),
            reuse_blob_rest_id=[blob_writer.rest_id])
        writer.write_envelope(TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')],))

        reader = self.s.load_one()
        blob_reader = self.s.get_blob_reader()
        self.assertEqual(reader.id, writer.id)

        # not expired, leased
        count = self.s.gc(ttl=10)
        self.assertEqual(count, (0, 0))
        self.assertTrue(reader.load())
        self.assertIsNotNone(blob_reader.load(db_id=blob_writer.id))

        # expired, leased
        count = self.s.gc(ttl=0)
        self.assertEqual(count, (0, 0))
        self.assertTrue(reader.load())
        self.assertIsNotNone(blob_reader.load(db_id=blob_writer.id))

        reader.write_envelope(TransactionMetadata(),
                              final_attempt_reason = 'upstream success',
                              finalize_attempt = True)

        # expired, unleased
        self.assertEqual(self.s.gc(ttl=0), (1,1))
        self.assertFalse(reader.load())
        self.assertIsNone(blob_reader.load(db_id=blob_writer.id))

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
        self.assertEqual(reader.tx.mail_from.mailbox, 'alice')
        self.assertEqual(reader.tx.rcpt_to[0].mailbox, 'bob')

    @staticmethod
    def wait_for(reader, rv):
        logging.info('test wait')
        rv[0] = reader.wait(5)

    def test_waiting_inflight(self):
        tx_cursor = self.s.get_transaction_cursor()
        tx_cursor.create('xyz', TransactionMetadata(
           host='outbound'))

        # needs to be inflight to wait
        reader = self.s.load_one()
        self.assertIsNotNone(reader)
        self.assertEqual(reader.id, tx_cursor.id)
        self.assertIsNone(reader.tx.mail_from)
        self.assertFalse(bool(reader.tx.rcpt_to))

        rv = [None]
        t = Thread(target = lambda: StorageTestBase.wait_for(reader, rv))
        t.start()
        time.sleep(0.1)
        logging.info('test append')

        tx_cursor.load()
        tx_cursor.write_envelope(TransactionMetadata(
            mail_from=Mailbox('alice'), rcpt_to=[Mailbox('bob')]))

        t.join()

        self.assertTrue(rv[0])
        self.assertEqual(reader.tx.mail_from.mailbox, 'alice')

    @staticmethod
    def wait_created(s, rv, db_id):
        logging.info('StorageTestBase.wait_created %s', db_id)
        rv[0] = s.wait_created(db_id, 1)

    def test_wait_created(self):
        created_id = None
        for i in range(1,3):
            logging.info('StorageTestBase.test_wait_created i=%d', i)
            rv = [None]
            t = Thread(target = lambda: StorageTestBase.wait_created(
                self.s, rv, created_id))
            t.start()
            time.sleep(0.1)

            writer = self.s.get_transaction_cursor()
            writer.create('xyz%d' % i, TransactionMetadata(host='host'))

            logging.info('StorageTestBase.test_wait_created join')
            t.join(timeout=2)
            logging.info('StorageTestBase.test_wait_created join done')
            self.assertFalse(t.is_alive())

            self.assertTrue(rv[0])
            created_id = self.s.created_id

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
        blob_writer = self.s.get_blob_writer()
        blob_writer.create('blob_rest_id')

        reader = self.s.get_blob_reader()
        reader.load(blob_writer.id)

        dd = [None]
        t = Thread(target = lambda: self.reader(reader, dd), daemon=True)
        t.start()

        d = None
        with open('/dev/urandom', 'rb') as f:
            d = f.read(128)

        for i in range(0, len(d)):
            logging.info('test_blob_waiting2 %d', i)
            blob_writer.append_data(d[i:i+1], len(d))
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

        self.s = Storage.connect_sqlite(filename)

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
            db_user='postgres', db_name='storage_test',
            unix_socket_dir=unix_socket_dir, port=port)

    def load_recovery(self):
        with open('storage_test_recovery.sql', 'r') as f:
            with self.s.begin_transaction() as db_tx:
                db_tx.connection.cursor().execute(f.read())



if __name__ == '__main__':
    unittest.main()
