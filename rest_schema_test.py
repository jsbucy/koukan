import unittest
import logging

from rest_schema import make_blob_uri, parse_blob_uri

class RestSchemaTest(unittest.TestCase):
    def setUp(self):
        pass

    def test_parse_blob_uri(self):
        self.assertIsNone(parse_blob_uri('/transactions'))
        self.assertIsNone(parse_blob_uri('/transactions/123'))
        self.assertIsNone(parse_blob_uri('/transactions/123/blob/'))
        self.assertEqual(parse_blob_uri(make_blob_uri('my-tx', 'my-blob')),
                         ('my-tx', 'my-blob'))

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')
    unittest.main()
