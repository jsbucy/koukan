import unittest
import logging
from threading import Thread
import time

from flask import Request as FlaskRequest
from werkzeug.datastructures import ContentRange

from storage import Storage, TransactionCursor
from storage_schema import VersionConflictException
from response import Response
from transaction import RestServiceTransaction
from fake_endpoints import SyncEndpoint
from filter import Mailbox, TransactionMetadata


class TransactionTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

        self.storage = Storage.get_sqlite_inmemory_for_test()

    def dump_db(self):
        for l in self.storage.db.iterdump():
            print(l)

    # xxx this should create a new RestServiceTransaction for every
    # call since that's how it's invoked, clear out state in tx_cursor
    def test_rest_transaction(self):
        rest_tx = RestServiceTransaction.create_tx(
            self.storage, 'host')
        self.assertIsNotNone(rest_tx)
        rest_resp = rest_tx.start(
            TransactionMetadata(mail_from=Mailbox('alice')).to_json())
        rest_id = rest_tx.tx_rest_id()
        self.assertTrue(self.storage.wait_created(None, timeout=1))

        rest_tx = RestServiceTransaction.load_tx(self.storage, rest_id)
        self.assertIsNotNone(rest_tx)
        rest_resp = rest_tx.get({})
        self.assertEqual(rest_resp.status_code, 200)
        resp_json = rest_resp.get_json()
        self.assertIsNotNone(resp_json)
        self.assertEqual(resp_json.get('mail_response', None), {})
        self.assertNotIn('rcpt_response', resp_json)
        self.assertNotIn('data_response', resp_json)

        rest_tx = RestServiceTransaction.load_tx(self.storage, rest_id)
        self.assertIsNotNone(rest_tx)
        rest_resp = rest_tx.patch(
            TransactionMetadata(rcpt_to=[Mailbox('bob')]).to_json())
        self.assertEqual(rest_resp.status_code, 200)
        resp_json = rest_resp.get_json()
        self.assertIsNotNone(resp_json)
        self.assertEqual(resp_json.get('mail_response', None), {})
        self.assertEqual(resp_json.get('rcpt_response', None), [{}])
        self.assertNotIn('data_response', resp_json)

        rest_tx = RestServiceTransaction.load_tx(self.storage, rest_id)
        self.assertIsNotNone(rest_tx)
        rest_resp = rest_tx.get({})
        self.assertEqual(rest_resp.status_code, 200)
        resp_json = rest_resp.get_json()
        self.assertIsNotNone(resp_json)
        self.assertEqual(resp_json.get('mail_response'), {})
        self.assertEqual(resp_json.get('rcpt_response'), [{}])
        self.assertIsNone(resp_json.get('data_response', None))

        rest_tx = RestServiceTransaction.load_tx(self.storage, rest_id)
        self.assertIsNotNone(rest_tx)
        rest_resp = rest_tx.patch({
            'body': '' })
        self.assertEqual(rest_resp.status_code, 200)
        resp_json = rest_resp.get_json()
        self.assertIsNotNone(resp_json)
        self.assertIn('body', resp_json)
        blob_uri = resp_json['body']

        d = b'world!'
        put_req = FlaskRequest({})  # wsgiref.types.WSGIEnvironment
        put_req.method = 'PUT'
        put_req.data = d
        put_req.content_length = len(put_req.data)
        #self.dump_db()
        blob_tx = RestServiceTransaction.load_blob(self.storage, blob_uri)
        self.assertIsNotNone(blob_tx)
        range = ContentRange('bytes', 0, len(d), len(d))
        rest_resp = blob_tx.put_blob(put_req, range, True)
        logging.info('%d %s', rest_resp.status_code, str(rest_resp.data))
        self.assertEqual(rest_resp.status_code, 200)

        rest_tx = RestServiceTransaction.load_tx(self.storage, rest_id)
        self.assertIsNotNone(rest_tx)
        rest_resp = rest_tx.patch({'max_attempts': 100})
        self.assertEqual(rest_resp.status_code, 200)

    # test resue blob


    def put(self, blob_tx : RestServiceTransaction, offset, d, overall=None,
            expected_http_code=None, expected_resp_content=None,
            expected_length=None, expected_last=None):
        range = ContentRange('bytes', offset, offset + len(d), overall)
        put_req = FlaskRequest({})  # wsgiref.types.WSGIEnvironment
        put_req.method = 'PUT'
        put_req.data = d
        put_req.content_length = len(put_req.data)
        rest_resp = blob_tx.put_blob(put_req, range, True)
        logging.info('put off=%d code=%d resp=%s range: %s', offset,
                     rest_resp.status_code, rest_resp.data,
                     rest_resp.content_range)
        self.assertEqual(rest_resp.status_code, expected_http_code)
        if expected_resp_content:
            self.assertEqual(rest_resp.data, expected_resp_content)

        range = rest_resp.content_range
        self.assertIsNotNone(range)
        self.assertEqual(0, range.start)
        self.assertEqual(expected_length, range.stop)
        self.assertEqual(expected_length if expected_last else None,
                         range.length)
        return expected_length


    def test_rest_blob_ranges(self):
        rest_tx = RestServiceTransaction.create_tx(self.storage, 'host')
        self.assertIsNotNone(rest_tx)
        rest_tx.start(TransactionMetadata(mail_from=Mailbox('alice'),
                                          rcpt_to=[Mailbox('bob')]).to_json())
        rest_id = rest_tx.tx_rest_id()

        self.assertTrue(self.storage.wait_created(None, timeout=1))

        rest_tx = RestServiceTransaction.load_tx(self.storage, rest_id)
        self.assertIsNotNone(rest_tx)

        rest_resp = rest_tx.patch({
            'body': '' })
        self.assertEqual(rest_resp.status_code, 200)
        resp_json = rest_resp.get_json()
        self.assertIsNotNone(resp_json)
        self.assertIn('body', resp_json)
        blob_uri = resp_json['body']

        # bad uri
        self.assertIsNone(RestServiceTransaction.load_blob(self.storage, 'xyz'))

        blob_tx = RestServiceTransaction.load_blob(self.storage, blob_uri)
        d = b'deadbeef'
        length = self.put(blob_tx, 0, d, None, 200, None,
                          len(d), False)

        # same one again: noop
        self.put(blob_tx, 0, d, None, 200, b'noop (range)',
                 length, False)

        # underrun: noop
        self.put(blob_tx, 1, b'eadbee', None, 200, b'noop (range)',
                 length, False)

        # past the end: 400
        self.put(blob_tx, 10, b'deadbeef', None, 400,
                 b'range start past the end',
                 length, False)

        # overlap the end: ok
        d = b'f01234567'
        length = self.put(blob_tx, length, d, None,
                          200, None,
                          length + len(d), False)

        # append exactly at the end: ok
        d = b'feed'
        length = self.put(blob_tx, length, d, None,
                          200, None,
                          length + len(d), False)

        # last
        d = b'EOF'
        self.put(blob_tx, length, d, length + len(d),
                 200, None,
                 length + len(d), True)

        # last noop
        self.put(blob_tx, length, d, length + len(d),
                 200, b'noop (range)',
                 length + len(d), True)

        # !last after last
        self.put(blob_tx, length, d, None,
                 400, b'append or !last after last',
                 length + len(d), True)
        length += len(d)

        # append after last
        self.put(blob_tx, length, d, length+len(d),
                 400, b'append or !last after last',
                 length, True)

        # TODO more coverage of PUTs without content-range

if __name__ == '__main__':
    unittest.main()
