import unittest
import logging
from threading import Thread
import time

from flask import Request as FlaskRequest
from werkzeug.datastructures import ContentRange

from storage import Storage, TransactionCursor
from response import Response
from transaction import RestServiceTransaction, cursor_to_endpoint
from fake_endpoints import SyncEndpoint
from filter import Mailbox, TransactionMetadata


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
    # call since that's how it's invoked, clear out state in tx_cursor
    def test_rest_transaction(self):
        rest_tx = RestServiceTransaction.create_tx(
            self.storage, 'host')
        self.assertIsNotNone(rest_tx)
        rest_id = rest_tx.tx_rest_id()
        rest_resp = rest_tx.start(
            TransactionMetadata(mail_from=Mailbox('alice')).to_json())
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
        rest_resp = rest_tx.append({
            'd': 'hello, ',
            'last': False })
        self.assertEqual(rest_resp.status_code, 200)

        rest_tx = RestServiceTransaction.load_tx(self.storage, rest_id)
        self.assertIsNotNone(rest_tx)
        rest_resp = rest_tx.append({
            'uri': None })
        self.assertEqual(rest_resp.status_code, 200)
        resp_json = rest_resp.get_json()
        self.assertIsNotNone(resp_json)
        self.assertIn('uri', resp_json)
        blob_uri = resp_json['uri']

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

        # re-append the previous blob
        rest_tx = RestServiceTransaction.load_tx(self.storage, rest_id)
        self.assertIsNotNone(rest_tx)
        rest_resp = rest_tx.append({
            'uri': blob_uri,
            'last': True })
        self.assertEqual(rest_resp.status_code, 200)
        resp_json = rest_resp.get_json()
        self.assertIsNotNone(resp_json)
        self.assertNotIn('uri', resp_json)

        rest_tx = RestServiceTransaction.load_tx(self.storage, rest_id)
        self.assertIsNotNone(rest_tx)
        rest_resp = rest_tx.patch({'max_attempts': 100})
        self.assertEqual(rest_resp.status_code, 200)


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
        self.assertEqual(expected_length if expected_last else None, range.length)
        return expected_length


    def test_rest_blob_ranges(self):
        rest_tx = RestServiceTransaction.create_tx(self.storage, 'host')
        self.assertIsNotNone(rest_tx)
        rest_id = rest_tx.tx_rest_id()
        rest_tx.start(TransactionMetadata(mail_from=Mailbox('alice'),
                                          rcpt_to=[Mailbox('bob')]).to_json())

        self.assertTrue(self.storage.wait_created(None, timeout=1))

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
            TransactionMetadata(
                mail_from=Mailbox('alice'), rcpt_to=[Mailbox('bob')],
                host='outbound'))
        tx_cursor.append_blob(d=b'hello, ', last=False)

        blob_writer = self.storage.get_blob_writer()
        blob_rest_id = 'blob_rest_id'
        blob_writer.create(blob_rest_id)
        d = b'world!'
        for i in range(0,len(d)):
            blob_writer.append_data(d[i:i+1], len(d))

        tx_cursor.append_blob(blob_rest_id=blob_rest_id, last=True)

        tx_cursor.set_max_attempts(100)
        del tx_cursor

        endpoint = SyncEndpoint()
        endpoint.set_mail_response(Response())
        endpoint.add_rcpt_response(Response(234))
        endpoint.add_data_response(None)
        endpoint.add_data_response(Response(256))

        tx_cursor = self.storage.load_one()
        self.assertIsNotNone(tx_cursor)
        self.assertEqual(tx_cursor.rest_id, 'rest_tx_id')
        cursor_to_endpoint(tx_cursor, endpoint)

        self.assertEqual(endpoint.tx.mail_from.mailbox, 'alice')
        self.assertEqual(endpoint.tx.rcpt_to[0].mailbox, 'bob')
        self.assertEqual(len(endpoint.blobs), 2)
        self.assertEqual(endpoint.blobs[0].contents(), b'hello, ')
        self.assertEqual(endpoint.blobs[1].contents(), b'world!')

        reader = self.storage.get_transaction_cursor()
        reader.load(rest_id='rest_tx_id')

    def output(self, rest_id, endpoint):
        tx_cursor = self.storage.load_one()
        self.assertEqual(tx_cursor.rest_id, rest_id)
        cursor_to_endpoint(tx_cursor, endpoint)

    def test_integrated(self):
        rest_tx = RestServiceTransaction.create_tx(
            self.storage, 'host')
        self.assertIsNotNone(rest_tx)
        rest_id : str = rest_tx.tx_rest_id()
        rest_tx.start(TransactionMetadata(mail_from=Mailbox('alice'),
                                          rcpt_to=[Mailbox('bob')]).to_json())
        self.assertTrue(self.storage.wait_created(None, timeout=1))

        endpoint = SyncEndpoint()
        endpoint.set_mail_response(Response())
        endpoint.add_rcpt_response(Response(234))

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
            logging.info('test_integrated start resp %s', resp_json)
            self.assertIsNotNone(resp_json)
            self.assertNotIn('data_response', resp_json)
            if resp_json['rcpt_response']:
                rcpt_resp = Response.from_json(resp_json['rcpt_response'][0])
                self.assertEqual(234, rcpt_resp.code)
                break
            time.sleep(0.2)
        else:
            self.fail('no rcpt result')

        self.assertEqual(endpoint.tx.mail_from.mailbox, 'alice')
        self.assertEqual(endpoint.tx.rcpt_to[0].mailbox, 'bob')

        rest_tx = RestServiceTransaction.load_tx(self.storage, rest_id)
        self.assertIsNotNone(rest_tx)
        rest_resp = rest_tx.append({
            'd': 'hello, '})

        endpoint.add_data_response(None)

        rest_resp = rest_tx.append({
            'uri': None,
            'last': True })
        self.assertEqual(rest_resp.status_code, 200)
        blob_rest_id = rest_resp.json['uri']


        d = b'world!'
        for i in range(0, len(d)):
            blob_tx = RestServiceTransaction.load_blob(
                self.storage, blob_rest_id)
            self.assertIsNotNone(blob_tx)
            put_req = FlaskRequest({})  # wsgiref.types.WSGIEnvironment
            put_req.method = 'PUT'
            b = d[i:i+1]
            self.assertEqual(len(b), 1)
            put_req.data = b
            put_req.content_length = 1

            put_resp = blob_tx.put_blob(
                put_req, ContentRange('bytes', i, i+1, len(d)), True)
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
            if resp_json.get('data_response', None):
                resp = Response.from_json(resp_json['data_response'])
                self.assertEqual(245, resp.code)
                break
            time.sleep(0.5)
        else:
            self.fail('no final result')

        t.join(timeout=1)
        self.assertFalse(t.is_alive())

        self.assertEqual(len(endpoint.blobs), 2)
        self.assertEqual(endpoint.blobs[0].contents(), b'hello, ')
        self.assertEqual(endpoint.blobs[1].contents(), b'world!')
        self.assertTrue(endpoint.last)


if __name__ == '__main__':
    unittest.main()
