
from storage import Storage, Action, Status

import unittest
import logging

class StorageTest(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
        self.s = Storage()
        self.s.connect(db=Storage.get_inmemory_for_test())

    def test_basic(self):
        writer = self.s.get_transaction_cursor()
        writer.create('xyz')
        writer.write_envelope(
            'local_host', 'remote_host',
            'alice', None,
            'bob', None, 'host')
        writer.append_data(b'abc')
        writer.append_data(b'xyz')
        self.assertEqual(writer.append_blob('blob_id'),
                         writer.APPEND_BLOB_UNKNOWN)

        blob_writer = self.s.get_blob_writer()
        self.assertIsNotNone(blob_writer.start())
        blob_writer.append_data(b'blob1')
        blob_writer.append_data(b'blob2')
        self.assertTrue(blob_writer.finalize())

        self.assertEqual(writer.append_blob(blob_writer.id),
                         writer.APPEND_BLOB_OK)
        writer.append_data(b'qrs')

        self.assertTrue(writer.finalize_payload(Status.WAITING))
        writer.append_action(Action.TEMP_FAIL)

        for l in self.s.db.iterdump():
            print(l)

        reader = self.s.load_one()
        self.assertIsNotNone(reader)
        self.assertEqual(reader.id, writer.id)
        self.assertEqual(reader.length, writer.offset)
        expected_content = [
            b'abc',
            b'xyz',
            b'blob1blob2',
            b'qrs'
        ]
        offset = 0
        for i,c in enumerate(expected_content):
            logging.info('%d', offset)
            blob = reader.read_content(offset)
            self.assertIsNotNone(blob)
            self.assertEqual(c, blob.contents())
            offset += blob.len()

        self.assertIsNone(self.s.load_one())

        reader.append_action(Action.TEMP_FAIL)

        r2 = self.s.load_one()
        self.assertIsNotNone(r2)
        self.assertEqual(r2.id, writer.id)

        r2.append_action(Action.DELIVERED)

        self.assertIsNone(self.s.load_one())

    def test_recovery(self):
        with open('storage_test_recovery.sql', 'r') as f:
            self.s.db.cursor().executescript(f.read())
        self.s.recover()
        for l in self.s.db.iterdump():
            print(l)
        reader = self.s.load_one()
        self.assertIsNotNone(reader)
        self.assertEqual(reader.id, 12345)
        self.assertEqual(reader.mail_from, 'alice@example.com')

    def test_non_durable(self):
        writer = self.s.get_transaction_cursor()
        writer.create('xyz')
        writer.write_envelope(
            'local_host', 'remote_host',
            'alice', None,
            'bob', None, 'host')
        writer.append_action(Action.TEMP_FAIL)

        reader = self.s.get_transaction_cursor()
        self.assertTrue(reader.load(writer.id))
        self.assertEqual(reader.status, Status.ONESHOT_DONE)

        reader = self.s.load_one()
        self.assertIsNone(reader)


if __name__ == '__main__':
    unittest.main()
