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
        uri = parse_blob_uri(make_blob_uri('my-tx', 'my-blob'))
        self.assertEqual(uri.tx_id, 'my-tx')
        self.assertEqual(uri.blob, 'my-blob')
        self.assertFalse(uri.tx_body)

        uri = parse_blob_uri(make_blob_uri('my-tx', tx_body=True))
        self.assertEqual(uri.tx_id, 'my-tx')
        self.assertTrue(uri.tx_body)
        self.assertIsNone(uri.blob)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')
    unittest.main()
