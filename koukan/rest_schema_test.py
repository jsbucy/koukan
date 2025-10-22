# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import unittest
import logging

from koukan.rest_schema import BlobUri, WhichJson, make_blob_uri, parse_blob_uri

class RestSchemaTest(unittest.TestCase):
    def setUp(self):
        pass

    def test_parse_blob_uri(self):
        self.assertIsNone(parse_blob_uri('/transactions'))
        self.assertIsNone(parse_blob_uri('/transactions/123'))
        self.assertIsNone(parse_blob_uri('/transactions/123/blob/'))
        uri = parse_blob_uri(
            make_blob_uri('my-tx', 'my-blob', base_uri='http://router'))
        self.assertEqual(uri.tx_id, 'my-tx')
        self.assertEqual(uri.blob, 'my-blob')
        self.assertFalse(uri.tx_body)

        uri = parse_blob_uri(make_blob_uri('my-tx', tx_body=True))
        self.assertEqual(uri.tx_id, 'my-tx')
        self.assertTrue(uri.tx_body)
        self.assertIsNone(uri.blob)

    def test_delta(self):
        uri = BlobUri('tx123', tx_body=True,
                      parsed_uri='http://0.router/transactions/tx123/body')
        uri2 = BlobUri('tx123', tx_body=True,
                       parsed_uri='http://router/transactions/tx123/body')
        self.assertFalse(uri.delta(uri2, WhichJson.ALL))

        uri3 = BlobUri('tx123', tx_body=True,
                       parsed_uri='http://0.router/transactions/tx124/body')
        with self.assertRaises(ValueError):
            uri.delta(uri3, WhichJson.ALL)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')
    unittest.main()
