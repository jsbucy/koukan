
import unittest
import logging
from threading import Thread
import time

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

    # xxx this should create a new RestServiceTransaction for every
    # call since that's how it's invoked, clear out state in storage
    # tx
    def test_rest_transaction(self):
        rest_tx = RestServiceTransaction.create_tx(
            self.storage, 'host')
        self.assertIsNotNone(rest_tx)
        rest_id = rest_tx.tx_rest_id()
        rest_tx.start({'mail_from': 'alice',
                       'rcpt_to': 'bob'})
        self.assertTrue(self.storage.wait_created(None, timeout=1))

#        cursor = self.storage.load_one()
#        self.assertIsNotNone(cursor)
#        self.assertEqual(cursor.rest_id, rest_tx.rest_id)

#        tx_cursor = self.storage.get_transaction_cursor()
#        tx_cursor.load(rest_id = rest_id)
#        tx_cursor.append_action(Action.START, Response(234))

#        rest_tx_cursor = self.storage.get_transaction_cursor()
#        rest_tx_cursor.load(rest_id = rest_id)
#        rest_tx = RestServiceTransaction(self.storage, rest_id, rest_tx_cursor)
#        del rest_tx_cursor

        rest_resp = rest_tx.get({})
        self.assertEqual(rest_resp.status_code, 200)
        resp_json = rest_resp.get_json()
        self.assertIsNotNone(resp_json)
#        self.assertIn('start_response', resp_json)
#        start_resp = Response.from_json(resp_json['start_response'])
#        self.assertEqual(234, start_resp.code)
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
        #self.dump_db()
        blob_tx = RestServiceTransaction.load_blob(self.storage, blob_uri)
        self.assertIsNotNone(blob_tx)
        rest_resp = blob_tx.put_blob(put_req)
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


        rest_resp = rest_tx.set_durable({})
        self.assertEqual(rest_resp.status_code, 200)

        # reader = self.storage.get_blob_reader()
        # self.assertIsNotNone(reader.start(rest_id=blob_uri))
        # self.assertEqual(reader.length, len(d))
        # self.assertEqual(reader.contents(), d)

        # tx_cursor.append_action(Action.DELIVERED, Response(245))
        # rest_resp = rest_tx.get({})
        # self.assertEqual(rest_resp.status_code, 200)
        # resp_json = rest_resp.get_json()
        # self.assertIsNotNone(resp_json)
        # self.assertIn('start_response', resp_json)
        # self.assertIn('final_status', resp_json)
        # final_resp = Response.from_json(resp_json['final_status'])
        # self.assertEqual(245, final_resp.code)

        #self.dump_db()


    def put(self, blob_tx : RestServiceTransaction, offset, d, overall=None,
            expected_http_code=None, expected_resp_content=None,
            expected_length=None, expected_last=None):
        range = ContentRange('bytes', offset, offset + len(d), overall)
        put_req = FlaskRequest(
            WSGIEnvironment({'HTTP_CONTENT_RANGE': range.to_header()}))
        put_req.method = 'PUT'
        put_req.data = d
        put_req.content_length = len(put_req.data)
        rest_resp = blob_tx.put_blob(put_req)
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
        rest_tx = RestServiceTransaction.create_tx(self.storage, 'host')
        self.assertIsNotNone(rest_tx)
        rest_id = rest_tx.tx_rest_id()
        rest_tx.start({'mail_from': 'alice',
                       'rcpt_to': 'bob'})

        self.assertTrue(self.storage.wait_created(None, timeout=1))

#        cursor = self.storage.load_one()
#        self.assertIsNotNone(cursor)
#        self.assertEqual(cursor.rest_id, rest_tx.rest_id)

#        tx_cursor = self.storage.get_transaction_cursor()
#        tx_cursor.load(rest_id = rest_id)
#        tx_cursor.append_action(Action.START, Response(234))

#        rest_tx_cursor = self.storage.get_transaction_cursor()
#        rest_tx_cursor.load(rest_id = rest_id)
#        rest_tx = RestServiceTransaction(self.storage, rest_id, rest_tx_cursor)
#        del rest_tx_cursor
        rest_tx = RestServiceTransaction.load_tx(self.storage, rest_id)
        self.assertIsNotNone(rest_tx)

        rest_resp = rest_tx.append({
            'uri': None })
        self.assertEqual(rest_resp.status_code, 200)
        resp_json = rest_resp.get_json()
        self.assertIsNotNone(resp_json)
        self.assertIn('uri', resp_json)
        blob_uri = resp_json['uri']



        # bad uri
        self.assertIsNone(RestServiceTransaction.load_blob(self.storage, 'xyz'))

        # invalid content-range header
        # XXX move this check to frontline flask handler?
        put_req = FlaskRequest(
            WSGIEnvironment({'HTTP_CONTENT_RANGE': 'quux'}))
        put_req.method = 'PUT'
        put_req.data = 'deadbeef'
        put_req.content_length = len(put_req.data)
        blob_tx = RestServiceTransaction.load_blob(self.storage, blob_uri)
        self.assertIsNotNone(blob_tx)
        rest_resp = blob_tx.put_blob(put_req)
        self.assertEqual(rest_resp.status_code, 400)
        self.assertEqual(rest_resp.data, b'bad range')

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

    def test_cursor_to_endpoint(self):
        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.create('rest_tx_id')
        #tx_cursor = self.storage.load_one()

        tx_cursor.write_envelope(
            mail_from='alice', rcpt_to='bob')
        tx_cursor.append_data(b'hello, ', last=False)

        blob_writer = self.storage.get_blob_writer()
        blob_rest_id = 'blob_rest_id'
        blob_writer.start(blob_rest_id)
        d = b'world!'
        for i in range(0,len(d)):
            blob_writer.append_data(d[i:i+1], len(d))

        tx_cursor.append_blob(blob_rest_id, last=True)

        self.assertTrue(tx_cursor.append_action(Action.SET_DURABLE))
        del tx_cursor

        endpoint = SyncEndpoint()
        endpoint.set_start_response(Response(234))
        endpoint.add_data_response(None)
        endpoint.add_data_response(Response(256))

        tx_cursor = self.storage.load_one()
        self.assertIsNotNone(tx_cursor)
        self.assertEqual(tx_cursor.rest_id, 'rest_tx_id')
        cursor_to_endpoint(tx_cursor, endpoint)

        self.assertEqual(endpoint.mail_from, 'alice')
        self.assertEqual(endpoint.rcpt_to, 'bob')
        self.assertEqual(len(endpoint.blobs), 2)
        self.assertEqual(endpoint.blobs[0].contents(), b'hello, ')
        self.assertEqual(endpoint.blobs[1].contents(), b'world!')

        reader = self.storage.get_transaction_cursor()
        reader.load(rest_id='rest_tx_id')
        actions = reader.load_last_action(1)
        time, action, resp = actions[0]
        self.assertEqual(action, Action.DELIVERED)
        self.assertIsNotNone(resp)
        self.assertEqual(resp.code, 256)

    def output(self, rest_id, endpoint):
        tx_cursor = self.storage.load_one()
        self.assertEqual(tx_cursor.rest_id, rest_id)
        cursor_to_endpoint(tx_cursor, endpoint)

    def test_integrated(self):
        rest_tx = RestServiceTransaction.create_tx(
            self.storage, 'host')
        self.assertIsNotNone(rest_tx)
        rest_id : str = rest_tx.tx_rest_id()
        rest_tx.start({'mail_from': 'alice',
                       'rcpt_to': 'bob'})
        self.assertTrue(self.storage.wait_created(None, timeout=1))

        endpoint = SyncEndpoint()
        endpoint.set_start_response(Response(234))

        t = Thread(target=lambda: self.output(rest_id, endpoint),
                   daemon=True)
        t.start()

        # poll for start response
        # handler should wait a little if it's inflight but client
        # needs to be prepared to retry regardless
        for i in range(0,5):
            rest_tx = RestServiceTransaction.load_tx(self.storage, rest_id)
            self.assertIsNotNone(rest_tx)
            rest_resp = rest_tx.get({})
            self.assertEqual(rest_resp.status_code, 200)
            resp_json = rest_resp.get_json()
            self.assertIsNotNone(resp_json)
            self.assertNotIn('final_status', resp_json)
            if 'start_response' in resp_json:
                start_resp = Response.from_json(resp_json['start_response'])
                self.assertEqual(234, start_resp.code)
                break
            time.sleep(0.2)
        else:
            self.fail('no start result')


        rest_tx = RestServiceTransaction.load_tx(self.storage, rest_id)
        self.assertIsNotNone(rest_tx)
        rest_resp = rest_tx.append({
            'd': 'hello, '})

        endpoint.add_data_response(None)

        rest_resp = rest_tx.append({
            'uri': None,
            'last': True })
        blob_rest_id = rest_resp.json['uri']


        d = b'world!'
        for i in range(0, len(d)):
            blob_tx = RestServiceTransaction.load_blob(
                self.storage, blob_rest_id)
            self.assertIsNotNone(blob_tx)
            put_req = FlaskRequest(WSGIEnvironment())
            put_req.method = 'PUT'
            b = d[i:i+1]
            put_req.data = b
            put_req.content_length = len(b)

            put_resp = blob_tx.put_blob(put_req)
            self.assertEqual(200, put_resp.status_code)

        endpoint.add_data_response(Response(245))

        for i in range(0,5):
            cursor = self.storage.get_transaction_cursor()
            cursor.load(rest_id=rest_id)
            rest_tx = RestServiceTransaction.load_tx(
                self.storage, rest_id)
            self.assertIsNotNone(rest_tx)
            rest_resp = rest_tx.get({})
            self.assertEqual(rest_resp.status_code, 200)
            resp_json = rest_resp.get_json()
            self.assertIsNotNone(resp_json)
            if 'final_status' in resp_json:
                resp = Response.from_json(resp_json['final_status'])
                self.assertEqual(245, resp.code)
                break
            time.sleep(0.2)
        else:
            self.fail('no final result')

        t.join(timeout=1)
        self.assertFalse(t.is_alive())

        #self.dump_db()


if __name__ == '__main__':
    unittest.main()
