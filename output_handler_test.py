import unittest
import logging
from threading import Thread
import time

from flask import Request as FlaskRequest
from werkzeug.datastructures import ContentRange

from storage import Storage, TransactionCursor
from storage_schema import VersionConflictException
from response import Response
from output_handler import cursor_to_endpoint
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

    def test_cursor_to_endpoint(self):
        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.create('rest_tx_id', TransactionMetadata(host='outbound'))
        #tx_cursor = self.storage.load_one()

        tx_cursor.write_envelope(
            TransactionMetadata(
                mail_from=Mailbox('alice'), rcpt_to=[Mailbox('bob')]))
        tx_cursor.write_envelope(TransactionMetadata(),
                                 create_body_rest_id='blob_rest_id')

        blob_writer = self.storage.get_blob_writer()
        blob_writer.load(rest_id='blob_rest_id')
        d = b'hello, world!'
        for i in range(0,len(d)):
            blob_writer.append_data(d[i:i+1], len(d))

        tx_cursor.load()
        tx_cursor.set_max_attempts(100)
        del tx_cursor

        endpoint = SyncEndpoint()
        endpoint.set_mail_response(Response())
        endpoint.add_rcpt_response(Response(234))
        #endpoint.add_data_response(None)
        endpoint.add_data_response(Response(256))

        tx_cursor = self.storage.load_one()
        self.assertIsNotNone(tx_cursor)
        self.assertEqual(tx_cursor.rest_id, 'rest_tx_id')
        cursor_to_endpoint(tx_cursor, endpoint)

        self.assertEqual(endpoint.mail_from.mailbox, 'alice')
        self.assertEqual(endpoint.rcpt_to[0].mailbox, 'bob')
        self.assertEqual(endpoint.body_blob.read(0), b'hello, world!')

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
        rest_tx.start(TransactionMetadata(mail_from=Mailbox('alice'),
                                          rcpt_to=[Mailbox('bob')]).to_json())
        rest_id : str = rest_tx.tx_rest_id()
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
            responses = [Response.from_json(r)
                         for r in resp_json.get('rcpt_response', [])]
            response_codes = [r.code if r else None for r in responses]
            if response_codes == [234]:
                break
            time.sleep(0.2)
        else:
            self.fail('no rcpt result')

        self.assertEqual(endpoint.tx.mail_from.mailbox, 'alice')
        self.assertEqual(endpoint.tx.rcpt_to[0].mailbox, 'bob')

        rest_tx = RestServiceTransaction.load_tx(self.storage, rest_id)
        self.assertIsNotNone(rest_tx)
        rest_resp = rest_tx.patch({'body': ''})
        self.assertEqual(rest_resp.status_code, 200)
        blob_rest_id = rest_resp.json['body']

        d = b'world!'
        for i in range(0, len(d)):
            logging.info('test_integrated put blob %d', i)
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

        self.assertEqual(endpoint.body_blob.read(0), b'world!')

if __name__ == '__main__':
    unittest.main()
