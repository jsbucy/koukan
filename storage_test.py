from threading import Thread
import time
import unittest
import logging
import base64

from storage import Storage, TransactionCursor
from storage_schema import InvalidActionException, VersionConflictException
from response import Response
from filter import HostPort, Mailbox, TransactionMetadata

class StorageTest(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s [%(thread)d] %(message)s')
        self.s = Storage.get_inmemory_for_test()

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

        tx_writer = self.s.get_transaction_cursor()
        tx_writer.load(rest_id='tx_rest_id')
        tx_writer.write_envelope(TransactionMetadata(),
                                 create_body_rest_id='blob_rest_id')
        blob_writer = self.s.get_blob_writer()
        blob_writer.load('blob_rest_id')

        blob_writer.append_data(d=b'abc')
        self.assertFalse(blob_writer.last)
        blob_writer.append_data(d=b'xyz', content_length=9)
        self.assertFalse(blob_writer.last)
        blob_writer.append_data(d=b'uvw', content_length=9)
        self.assertTrue(blob_writer.last)

        logging.info('test_basic check tx input done')
        tx_writer.load()
        self.assertTrue(tx_writer.input_done)

        tx_writer.set_max_attempts(100)

        tx_reader.load()
        tx_reader.add_rcpt_response([Response(456, 'busy')])
        tx_reader.finalize_attempt(False)

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

        r2.finalize_attempt(True)

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
        tx_writer.write_envelope(TransactionMetadata(),
                                 create_body_rest_id='body_rest_id')
        blob_writer = self.s.get_blob_writer()
        blob_writer.load('body_rest_id')
        d = b'hello, world!'
        blob_writer.append_data(d, len(d))

        tx_writer = self.s.get_transaction_cursor()
        tx = TransactionMetadata(
            remote_host=HostPort('remote_host', 2525), host='host',
            mail_from=Mailbox('alice'), rcpt_to=[Mailbox('bob')])
        tx.body = 'q'

        with self.assertRaises(AssertionError):
            tx_writer.create('tx_rest_id2', tx)

        tx.body = 'body_rest_id'
        tx_writer.create('tx_rest_id2', tx)
        self.assertTrue(tx_writer.input_done)

    def test_recovery(self):
        with open('storage_test_recovery.sql', 'r') as f:
            with self.s.conn() as conn:
                conn.connection.cursor().executescript(f.read())
        self.s.recover()
        reader = self.s.load_one()
        self.assertIsNotNone(reader)
        self.assertEqual(reader.id, 12345)
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
        t = Thread(target = lambda: StorageTest.wait_for(reader, rv))
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
        rv[0] = reader.wait(5)

    def start_wait(self, reader, rv):
        t = Thread(target =
                   lambda: StorageTest.wait_blob(reader, rv))
        t.start()
        time.sleep(0.1)
        return t

    def join(self, t):
        t.join(timeout=5)
        self.assertFalse(t.is_alive())

    def reader(self, reader, wait, dd):
        d = bytes()
        while (reader.content_length() is None or
               reader.len() < reader.content_length()):
            logging.info('reader %d', len(d))
            if wait:
                reader.wait()
            else:
                reader.load()
            d += reader.read(len(d))
        dd[0] = d

    def _test_blob_waiting(self, wait):
        blob_writer = self.s.get_blob_writer()
        blob_writer.create('blob_rest_id')

        reader = self.s.get_blob_reader()
        reader.load(blob_writer.id)

        dd = [None]
        # xxx both ways
        t = Thread(target = lambda: self.reader(reader, wait, dd), daemon=True)
        t.start()

        d = None
        with open('/dev/urandom', 'rb') as f:
            d = f.read(1024)

        for i in range(0, len(d)):
            logging.info('test_blob_waiting2 %d', i)
            blob_writer.append_data(d[i:i+1], len(d))
            self.assertEqual(blob_writer.length, i+1)

        t.join()
        self.assertFalse(t.is_alive())
        self.assertEqual(dd[0], d)
        self.assertEqual(reader.content_length(), len(d))
        self.assertEqual(reader.len(), len(d))

    def test_blob_waiting_wait(self):
        self._test_blob_waiting(True)
    def test_blob_waiting_poll(self):
        self._test_blob_waiting(False)

if __name__ == '__main__':
    unittest.main()
