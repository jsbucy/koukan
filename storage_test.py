
from storage import Storage, Action, Status

from response import Response

from threading import Thread

import time

import unittest
import logging

class StorageTest(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
        self.s = Storage()
        self.s.connect(db=Storage.get_inmemory_for_test())

    def dump_db(self):
        for l in self.s.db.iterdump():
            print(l)

    def test_basic(self):
        writer = self.s.get_transaction_cursor()
        writer.create('tx_rest_id')
        logging.info('version %d', writer.version)
        writer.write_envelope(
            'local_host', 'remote_host',
            'alice', None,
            'bob', None, 'host')
        writer.append_data(b'abc')
        writer.append_data(b'xyz')
        self.assertEqual(writer.append_blob('blob_id', 1),
                         writer.APPEND_BLOB_UNKNOWN)

        blob_writer = self.s.get_blob_writer()
        self.assertIsNotNone(blob_writer.start('blob_rest_id'))
        blob_writer.append_data(b'blob1', False)
        blob_writer.append_data(b'blob2', True)

        self.assertEqual(writer.append_blob(blob_writer.id, blob_writer.offset),
                         writer.APPEND_BLOB_OK)
        writer.append_data(b'qrs')

        self.assertTrue(writer.finalize_payload(Status.WAITING))
        self.dump_db()

        writer.append_action(Action.TEMP_FAIL, Response(400))


#        reader = self.s.load_one()
#        self.assertIsNotNone(reader)
        reader = self.s.get_transaction_cursor()
        reader.load(id=writer.id)
        self.assertEqual(reader.id, writer.id)
        # xxx self.assertEqual(reader.length, 23)
        self.assertTrue(reader.last)
        self.assertEqual(reader.max_i, 3)

        expected_content = [
            b'abc',
            b'xyz',
            b'blob1blob2',
            b'qrs'
        ]
        for i,c in enumerate(expected_content):
            logging.info('%d', i)
            blob = reader.read_content(i)
            self.assertIsNotNone(blob)
            self.assertEqual(c, blob.contents())

        #self.assertIsNone(self.s.load_one())

        reader.append_action(Action.TEMP_FAIL, Response(456))

        r2 = self.s.load_one()
        self.assertIsNotNone(r2)
        self.assertEqual(r2.id, writer.id)

        r2.append_action(Action.DELIVERED, Response(234))

        self.assertIsNone(self.s.load_one())

        r3 = self.s.get_transaction_cursor()
        r3.load(writer.id)
        actions = r3.load_last_action(3)
        self.assertEqual(Action.DELIVERED, actions[0][1])
        resp = actions[0][2]
        self.assertIsNotNone(resp)
        self.assertEqual(234, resp.code)
        self.assertEqual(Action.LOAD, actions[1][1])
        self.assertIsNone(actions[1][2])
        self.assertEqual(Action.TEMP_FAIL, actions[2][1])
        resp = actions[2][2]
        self.assertIsNotNone(resp)
        self.assertEqual(456, resp.code)

    def test_recovery(self):
        with open('storage_test_recovery.sql', 'r') as f:
            self.s.db.cursor().executescript(f.read())
        self.s.recover()
        # self.dump_db()
        reader = self.s.load_one()
        self.assertIsNotNone(reader)
        self.assertEqual(12, reader.length)
        self.assertEqual(reader.id, 12345)
        self.assertEqual(reader.mail_from, 'alice@example.com')
        self.assertEqual(reader.i, 0)

    def test_non_durable(self):
        writer = self.s.get_transaction_cursor()
        writer.create('xyz')
        writer.write_envelope(
            'local_host', 'remote_host',
            'alice', None,
            'bob', None, 'host')
        writer.append_action(Action.TEMP_FAIL, Response(400))

        reader = self.s.get_transaction_cursor()
        self.assertTrue(reader.load(writer.id))
        self.assertEqual(reader.status, Status.ONESHOT_DONE)

        reader = self.s.load_one()
        self.assertIsNone(reader)

    def test_waiting_slowpath(self):
        writer = self.s.get_transaction_cursor()
        writer.create('xyz')

        reader = self.s.get_transaction_cursor()
        self.assertTrue(reader.load(writer.id))
        self.assertIsNone(reader.mail_from)
        self.assertIsNone(reader.rcpt_to)
        self.assertFalse(reader.wait(1))

        writer.write_envelope(
            'local_host', 'remote_host',
            'alice', None,
            'bob', None, 'host')

        self.assertTrue(reader.wait(0))
        self.assertEqual(reader.mail_from, 'alice')
        self.assertEqual(reader.rcpt_to, 'bob')

    @staticmethod
    def wait_for(reader, rv, fn):
        rv[0] = reader.wait_for(fn, 5)

    @staticmethod
    def wait_attr(reader, rv, attr):
        rv[0] = reader.wait_attr_not_none(attr, 5)

    def test_waiting_inflight(self):
        writer = self.s.get_transaction_cursor()
        writer.create('xyz')

        reader = self.s.get_transaction_cursor()
        self.assertTrue(reader.load(writer.id))
        self.assertIsNone(reader.mail_from)
        self.assertIsNone(reader.rcpt_to)

        rv = [None]
        t = Thread(target =
                   lambda: StorageTest.wait_attr(reader, rv, 'mail_from'))
        t.start()
        time.sleep(0.1)

        writer.write_envelope(
            'local_host', 'remote_host',
            'alice', None,
            'bob', None, 'host')

        t.join()

        self.assertTrue(rv[0])
        self.assertEqual(reader.mail_from, 'alice')
        self.assertEqual(reader.rcpt_to, 'bob')

        self.assertEqual(reader.length, 0)
        self.assertFalse(reader.wait_for(lambda: reader.length > 0, timeout=0.1))

        rv = [None]
        t = Thread(target =
                   lambda: StorageTest.wait_for(reader, rv, lambda: reader.length > 0))
        t.start()
        time.sleep(0.1)
        writer.append_data(b'hello, world!')
        t.join()

        self.assertTrue(rv[0])
        # xxx self.assertEqual(13, reader.length)

    @staticmethod
    def wait_created(s, rv, id):
        rv[0] = s.wait_created(id, 5)

    def test_wait_created(self):
        rv = [None]
        t = Thread(target =
                   lambda: StorageTest.wait_created(self.s, rv, None))
        t.start()
        time.sleep(0.1)

        writer = self.s.get_transaction_cursor()
        writer.create('xyz')

        t.join(timeout=5)
        self.assertFalse(t.is_alive())

        self.assertTrue(rv[0])

    @staticmethod
    def wait_blob(reader, rv):
        rv[0] = reader.wait_length(5)

    def test_blob_waiting(self):
        writer = self.s.get_blob_writer()
        writer.start('blob_rest_id')

        reader = self.s.get_blob_reader()
        reader.start(writer.id)

        rv = [False]
        t = Thread(target =
                   lambda: StorageTest.wait_blob(reader, rv))
        t.start()
        time.sleep(0.1)

        writer.append_data(b'xyz', True)

        t.join(timeout=5)
        self.assertFalse(t.is_alive())

        self.assertTrue(rv[0])
        self.assertEqual(reader.length, 3)
        self.assertTrue(reader.last)

if __name__ == '__main__':
    unittest.main()
