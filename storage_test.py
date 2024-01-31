from threading import Thread
import time
import unittest
import logging

from storage import Storage, TransactionCursor
from storage_schema import InvalidActionException, VersionConflictException
from response import Response
from filter import HostPort, Mailbox, TransactionMetadata

class StorageTest(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')
        self.s = Storage()
        self.s.connect(db=Storage.get_inmemory_for_test())

    def dump_db(self):
        for l in self.s.db.iterdump():
            print(l)

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

        tx_writer = self.s.get_transaction_cursor()
        tx_writer.load(rest_id='tx_rest_id')
        tx_writer.append_blob(d=b'abc', last=False)

        tx_writer = self.s.get_transaction_cursor()
        tx_writer.load(rest_id='tx_rest_id')
        tx_writer.append_blob(d=b'xyz', last=False)
        self.assertEqual(tx_writer.append_blob(
            blob_rest_id='blob_id', last=False),
                         tx_writer.APPEND_BLOB_UNKNOWN)

        blob_writer = self.s.get_blob_writer()
        self.assertIsNotNone(blob_writer.create('blob_rest_id'))

        #tx_writer = self.s.get_transaction_cursor()
        #tx_writer.load(rest_id='tx_rest_id')
        tx_writer.load()
        self.assertEqual(tx_writer.append_blob(
            blob_rest_id='blob_rest_id', last=False),
                         tx_writer.APPEND_BLOB_OK)

        blob_writer.append_data(b'blob1')
        blob_writer.append_data(b'blob2', 10)
        self.assertTrue(blob_writer.last)

        tx_writer.append_blob(d=b'qrs', last=True)
        tx_writer.load()
        self.assertTrue(tx_writer.input_done)

        tx_writer.set_max_attempts(100)

        tx_reader.load()
        tx_reader.add_rcpt_response([Response(456, 'busy')])
        tx_reader.finalize_attempt(False)

        # reader = self.s.get_transaction_cursor()
        # reader.load(db_id=tx_writer.id)
        # self.assertEqual(reader.id, tx_writer.id)
        # xxx self.assertEqual(reader.length, 23)
        self.assertTrue(tx_reader.last)
        self.assertEqual(tx_reader.max_i, 3)

        expected_content = [
            b'abc',
            b'xyz',
            b'blob1blob2',
            b'qrs'
        ]
        for i,c in enumerate(expected_content):
            logging.info('%d', i)
            blob = tx_reader.read_content(i)
            self.assertIsNotNone(blob)
            self.assertEqual(c, blob.contents())

        r2 = self.s.load_one()
        self.assertIsNotNone(r2)
        self.assertEqual(r2.id, tx_writer.id)
        self.assertIsNone(r2.tx.mail_response)
        self.assertEqual(r2.tx.rcpt_response, [])
        self.assertIsNone(r2.tx.data_response)

        r2.finalize_attempt(True)

        self.assertIsNone(self.s.load_one())

        stale_append = self.s.get_transaction_cursor()
        stale_append.load(tx_writer.id)
        with self.assertRaises(InvalidActionException):
            stale_append.append_blob(d=b'stale', last=True)

    def test_recovery(self):
        with open('storage_test_recovery.sql', 'r') as f:
            self.s.db.cursor().executescript(f.read())
        self.s.recover()
        # self.dump_db()
        reader = self.s.load_one()
        self.assertIsNotNone(reader)
        #self.assertEqual(12, reader.length)
        self.assertEqual(reader.id, 12345)
        self.assertEqual(reader.tx.mail_from.mailbox, 'alice@example.com')
        self.assertEqual(reader.max_i, 0)

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

        reader.finalize_attempt(False)

        reader = self.s.get_transaction_cursor()
        self.assertTrue(reader.load(writer.id))
        #self.dump_db()

        reader = self.s.load_one()
        self.assertIsNone(reader)

    def test_gc_non_durable(self):
        writer = self.s.get_transaction_cursor()
        writer.create('xyz', TransactionMetadata(
            host='host',
            local_host=HostPort('local_host', 25),
            remote_host=HostPort('remote_host', 2525)))
        writer.write_envelope(TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')],))
        self.assertFalse(writer.input_done)
        self.assertEqual(self.s.gc_non_durable(min_age = 1), 0)
        time.sleep(2)
#        self.dump_db()
        self.assertEqual(self.s.gc_non_durable(min_age = 1), 1)


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
    def wait_for(reader, rv, fn):
        logging.info('test wait')
        rv[0] = reader.wait_for(fn, 5)

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

        fn = lambda: (reader.max_i is not None)
        self.assertFalse(reader.wait_for(fn, timeout=0.1))

        rv = [None]
        t = Thread(target = lambda: StorageTest.wait_for(reader, rv, fn))
        t.start()
        time.sleep(1)
        logging.info('test append')
        tx_cursor.load()
        tx_cursor.append_blob(d=b'hello, world!', last=True)
        t.join()

        self.assertTrue(rv[0])
        # xxx self.assertEqual(13, reader.length)

    @staticmethod
    def wait_created(s, rv, db_id):
        logging.info('StorageTest.wait_created %s', db_id)
        rv[0] = s.wait_created(db_id, 1)

    def test_wait_created(self):
        created_id = None
        for i in range(1,3):
            logging.info('StorageTest.test_wait_created i=%d', i)
            rv = [None]
            t = Thread(target =
                       lambda: StorageTest.wait_created(self.s, rv, created_id))
            t.start()
            time.sleep(0.1)

            writer = self.s.get_transaction_cursor()
            writer.create('xyz%d' % i, TransactionMetadata(host='host'))

            logging.info('StorageTest.test_wait_created join')
            t.join(timeout=2)
            logging.info('StorageTest.test_wait_created join done')
            self.assertFalse(t.is_alive())

            self.assertTrue(rv[0])
            created_id = self.s.created_id

    @staticmethod
    def wait_blob(reader, rv):
        rv[0] = reader.wait_length(5)

    def test_blob_waiting(self):
        blob_writer = self.s.get_blob_writer()
        blob_writer.create('blob_rest_id')

        reader = self.s.get_blob_reader()
        reader.load(blob_writer.id)

        rv = [False]
        t = Thread(target =
                   lambda: StorageTest.wait_blob(reader, rv))
        t.start()
        time.sleep(0.1)

        blob_writer.append_data(b'xyz', 3)

        t.join(timeout=5)
        self.assertFalse(t.is_alive())

        self.assertTrue(rv[0])
        self.assertEqual(reader.length, 3)
        self.assertTrue(reader.last)

if __name__ == '__main__':
    unittest.main()
