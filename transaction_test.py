
import unittest
import logging
from threading import Thread


from storage import Storage, Action
from response import Response
from transaction import RestServiceTransaction, cursor_to_endpoint

from fake_endpoints import SyncEndpoint

from flask import Request as FlaskRequest
from wsgiref.types import WSGIEnvironment
from werkzeug.datastructures import ContentRange


class TransactionTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

        self.storage = Storage()
        self.storage.connect(db=Storage.get_inmemory_for_test())

    def dump_db(self):
        for l in self.storage.db.iterdump():
            print(l)

    def test_rest_transaction(self):
        rest_tx = RestServiceTransaction.create(
            self.storage,
            {'mail_from': 'alice',
             'rcpt_to': 'bob'})
        rest_id = rest_tx.rest_id
        self.assertTrue(self.storage.wait_created(None, timeout=1))

        cursor = self.storage.load_one()
        self.assertIsNotNone(cursor)
        self.assertEqual(cursor.rest_id, rest_tx.rest_id)

        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.load(rest_id = rest_id)
        tx_cursor.append_action(Action.START, Response(234))

        rest_tx_cursor = self.storage.get_transaction_cursor()
        rest_tx_cursor.load(rest_id = rest_id)
        rest_tx = RestServiceTransaction(self.storage, rest_id, rest_tx_cursor)
        del rest_tx_cursor

        rest_resp = rest_tx.get({})
        self.assertEqual(rest_resp.status_code, 200)
        resp_json = rest_resp.get_json()
        self.assertIsNotNone(resp_json)
        self.assertIn('start_response', resp_json)
        start_resp = Response.from_json(resp_json['start_response'])
        self.assertEqual(234, start_resp.code)
        self.assertNotIn('final_status', resp_json)

        rest_resp = rest_tx.append({
            'd': 'hello, ',
            'last': False })
        self.assertEqual(rest_resp.status_code, 200)

        rest_resp = rest_tx.append({
            'uri': None })
        self.assertEqual(rest_resp.status_code, 200)
        resp_json = rest_resp.get_json()
        self.assertIsNotNone(resp_json)
        self.assertIn('uri', resp_json)
        blob_uri = resp_json['uri']

        d = b'world!'
        put_req = FlaskRequest(WSGIEnvironment())
        put_req.method = 'PUT'
        put_req.data = d
        put_req.content_length = len(put_req.data)
        rest_resp = rest_tx.put_blob(blob_uri, put_req)
        logging.info('%d %s', rest_resp.status_code, str(rest_resp.data))
        self.assertEqual(rest_resp.status_code, 200)

        # re-append the previous blob
        rest_resp = rest_tx.append({
            'uri': blob_uri,
            'last': True })
        self.assertEqual(rest_resp.status_code, 200)
        resp_json = rest_resp.get_json()
        self.assertIsNotNone(resp_json)
        self.assertNotIn('uri', resp_json)

        reader = self.storage.get_blob_reader()
        self.assertIsNotNone(reader.start(rest_id=blob_uri))
        self.assertEqual(reader.length, len(d))
        self.assertEqual(reader.contents(), d)

        tx_cursor.append_action(Action.DELIVERED, Response(245))
        rest_resp = rest_tx.get({})
        self.assertEqual(rest_resp.status_code, 200)
        resp_json = rest_resp.get_json()
        self.assertIsNotNone(resp_json)
        self.assertIn('start_response', resp_json)
        self.assertIn('final_status', resp_json)
        final_resp = Response.from_json(resp_json['final_status'])
        self.assertEqual(245, final_resp.code)

        #self.dump_db()


    def put(self, rest_tx, blob_uri, offset, d, overall=None,
            expected_http_code=None, expected_resp_content=None,
            expected_length=None, expected_last=None):
        range = ContentRange('bytes', offset, offset + len(d), overall)
        put_req = FlaskRequest(
            WSGIEnvironment({'HTTP_CONTENT_RANGE': range.to_header()}))
        put_req.method = 'PUT'
        put_req.data = d
        put_req.content_length = len(put_req.data)
        rest_resp = rest_tx.put_blob(blob_uri, put_req)
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
        self.assertEqual(expected_length if expected_last else None, range.length)
        return expected_length


    def test_rest_blob_ranges(self):
        rest_tx = RestServiceTransaction.create(
            self.storage,
            {'mail_from': 'alice',
             'rcpt_to': 'bob'})
        rest_id = rest_tx.rest_id
        self.assertTrue(self.storage.wait_created(None, timeout=1))

        cursor = self.storage.load_one()
        self.assertIsNotNone(cursor)
        self.assertEqual(cursor.rest_id, rest_tx.rest_id)

        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.load(rest_id = rest_id)
        tx_cursor.append_action(Action.START, Response(234))

        rest_tx_cursor = self.storage.get_transaction_cursor()
        rest_tx_cursor.load(rest_id = rest_id)
        rest_tx = RestServiceTransaction(self.storage, rest_id, rest_tx_cursor)
        del rest_tx_cursor

        rest_resp = rest_tx.append({
            'uri': None })
        self.assertEqual(rest_resp.status_code, 200)
        resp_json = rest_resp.get_json()
        self.assertIsNotNone(resp_json)
        self.assertIn('uri', resp_json)
        blob_uri = resp_json['uri']

        # bad uri
        put_req = FlaskRequest(WSGIEnvironment())
        put_req.method = 'PUT'
        put_req.data = 'deadbeef'
        put_req.content_length = len(put_req.data)
        rest_resp = rest_tx.put_blob('xyz', put_req)
        self.assertEqual(rest_resp.status_code, 404)

        # invalid content-range header
        put_req = FlaskRequest(
            WSGIEnvironment({'HTTP_CONTENT_RANGE': 'quux'}))
        put_req.method = 'PUT'
        put_req.data = 'deadbeef'
        put_req.content_length = len(put_req.data)
        rest_resp = rest_tx.put_blob(blob_uri, put_req)
        self.assertEqual(rest_resp.status_code, 400)
        self.assertEqual(rest_resp.data, b'bad range')


        d = b'deadbeef'
        length = self.put(rest_tx, blob_uri, 0, d, None, 200, None,
                          len(d), False)

        # same one again: noop
        self.put(rest_tx, blob_uri, 0, d, None, 200, b'noop (range)',
                 length, False)

        # underrun: noop
        self.put(rest_tx, blob_uri, 1, b'eadbee', None, 200, b'noop (range)',
                 length, False)

        # past the end: 400
        self.put(rest_tx, blob_uri, 10, b'deadbeef', None, 400,
                 b'range start past the end',
                 length, False)

        # overlap the end: ok
        d = b'f01234567'
        length = self.put(rest_tx, blob_uri, length, d, None,
                          200, None,
                          length + len(d), False)

        # append exactly at the end: ok
        d = b'feed'
        length = self.put(rest_tx, blob_uri, length, d, None,
                          200, None,
                          length + len(d), False)

        # last
        d = b'EOF'
        self.put(rest_tx, blob_uri, length, d, length + len(d),
                 200, None,
                 length + len(d), True)

        # last noop
        self.put(rest_tx, blob_uri, length, d, length + len(d),
                 200, b'noop (range)',
                 length + len(d), True)

        # !last after last
        self.put(rest_tx, blob_uri, length, d, None,
                 400, b'append or !last after last',
                 length + len(d), True)
        length += len(d)

        # append after last
        self.put(rest_tx, blob_uri, length, d, length+len(d),
                 400, b'append or !last after last',
                 length, True)

        # TODO more coverage of PUTs without content-range

    def test_cursor_to_endpoint(self):
        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.create('xyz')
        tx_cursor.write_envelope(
            None, None, 'alice', None, 'bob', None, None)
        tx_cursor.append_data(b'hello, ')

        blob_writer = self.storage.get_blob_writer()
        blob_rest_id = 'blob_rest_id'
        blob_writer.start(blob_rest_id)
        d = b'world!'
        for i,ch in enumerate(d):
            blob_writer.append_data(ch.to_bytes(), len(d))

        tx_cursor.append_blob(blob_writer.id, blob_writer.length)
        tx_cursor.finalize_payload()
        del tx_cursor

        self.dump_db()

        endpoint = SyncEndpoint()
        endpoint.set_start_response(Response(234))
        endpoint.add_data_response(None)  # XXX Response(245))
        endpoint.add_data_response(Response(256))

        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.load(rest_id='xyz')
        cursor_to_endpoint(tx_cursor, endpoint)

        self.assertEqual(endpoint.mail_from, 'alice')
        self.assertEqual(endpoint.rcpt_to, 'bob')
        self.assertEqual(len(endpoint.blobs), 2)
        self.assertEqual(endpoint.blobs[0].contents(), b'hello, ')
        self.assertEqual(endpoint.blobs[1].contents(), b'world!')

        reader = self.storage.get_transaction_cursor()
        reader.load(rest_id='xyz')
        actions = reader.load_last_action(1)
        time, action, resp = actions[0]
        self.assertEqual(action, Action.DELIVERED)
        self.assertEqual(resp.code, 256)

    def disabled_test_integrated(self):
        pass

if __name__ == '__main__':
    unittest.main()
