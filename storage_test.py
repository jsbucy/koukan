
from storage import Storage, Action

import unittest

class StorageTest(unittest.TestCase):

    def setUp(self):
        self.s = Storage()
        self.s.connect(db=Storage.get_inmemory_for_test())

    def test_basic(self):
        writer = self.s.get_transaction_writer()
        writer.start('local_host', 'remote_host', 'alice', None, 'bob', None, 'host', False)
        writer.append_data('abc')
        writer.append_data('xyz')
        self.assertEqual(writer.append_blob('blob_id'),
                         writer.APPEND_BLOB_UNKNOWN)

        blob_writer = self.s.get_blob_writer()
        self.assertIsNotNone(blob_writer.start())
        blob_writer.append_data('blob1')
        blob_writer.append_data('blob2')
        self.assertTrue(blob_writer.finalize())

        self.assertEqual(writer.append_blob(blob_writer.id),
                         writer.APPEND_BLOB_OK)
        writer.append_data('qrs')

        self.assertTrue(writer.finalize())

        reader = self.s.load_one()
        self.assertEqual(reader.id, writer.id)
        expected_content = [
            'abc',
            'xyz',
            'blob1',
            'blob2',
            'qrs'
        ]
        for i,c in enumerate(expected_content):
            d = reader.read_content()
            self.assertIsNotNone(d)
            self.assertEqual(c, d)

        self.assertIsNone(self.s.load_one())

        self.s.append_transaction_actions(reader.id, Action.TEMP_FAIL)

        r2 = self.s.load_one()
        self.assertEqual(r2.id, writer.id)

        self.s.append_transaction_actions(r2.id, Action.DELIVERED)

        self.assertIsNone(self.s.load_one())

# for l in s.db.iterdump():
#     print(l)


if __name__ == '__main__':
    unittest.main()
