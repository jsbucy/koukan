import unittest
import logging
from executor import Executor
import time

from flask import Request as FlaskRequest
from werkzeug.datastructures import ContentRange

from storage import Storage, TransactionCursor
from storage_schema import VersionConflictException
from response import Response
from output_handler import OutputHandler
from transaction import RestServiceTransaction
from fake_endpoints import SyncEndpoint
from filter import Mailbox, TransactionMetadata


class OutputHandlerTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

        self.storage = Storage.get_sqlite_inmemory_for_test()
        self.executor = Executor(inflight_limit=10, watchdog_timeout=10)

    def tearDown(self):
        self.executor.shutdown(timeout=5)

    def dump_db(self):
        with self.storage.conn() as conn:
            for l in conn.connection.iterdump():
                logging.debug(l)

    def test_cursor_to_endpoint(self):
        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.create('rest_tx_id', TransactionMetadata(host='outbound'))

        tx_meta = TransactionMetadata(
            mail_from=Mailbox('alice'), rcpt_to=[Mailbox('bob')])
        tx_cursor.write_envelope(tx_meta)
        blob_writer = self.storage.get_blob_writer()
        blob_writer.create(rest_id='blob_rest_id')
        d = b'hello, world!'
        for i in range(0,len(d)):
            blob_writer.append_data(d[i:i+1], len(d))

        tx_cursor.write_envelope(
            TransactionMetadata(
                body='blob_rest_id',
                retry = {'max_attempts': 100}),
            reuse_blob_rest_id=['blob_rest_id'])
        del tx_cursor

        endpoint = SyncEndpoint()
        endpoint.set_mail_response(Response())
        endpoint.add_rcpt_response(Response(234))
        #endpoint.add_data_response(None)
        endpoint.add_data_response(Response(256))

        tx_cursor = self.storage.load_one()
        self.assertIsNotNone(tx_cursor)
        self.assertEqual(tx_cursor.rest_id, 'rest_tx_id')
        handler = OutputHandler(tx_cursor, endpoint,
                                downstream_env_timeout=1,
                                downstream_data_timeout=1)
        handler.cursor_to_endpoint()

        self.assertEqual(endpoint.mail_from.mailbox, 'alice')
        self.assertEqual(endpoint.rcpt_to[0].mailbox, 'bob')
        self.assertEqual(endpoint.body_blob.read(0), b'hello, world!')

        reader = self.storage.get_transaction_cursor()
        reader.load(rest_id='rest_tx_id')

    def write_envelope(self, cursor, tx, reuse_blob_rest_id=None):
        for i in range(1,5):
            try:
                cursor.write_envelope(tx, reuse_blob_rest_id=reuse_blob_rest_id)
                break
            except VersionConflictException:
                cursor.load()
        else:
            self.fail('couldn\'t write envelope')

    def test_no_valid_rcpt(self):
        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.create('rest_tx_id', TransactionMetadata(host='outbound'))
        tx_meta = TransactionMetadata(
            mail_from=Mailbox('alice'), rcpt_to=[Mailbox('bob1')])
        tx_cursor.write_envelope(tx_meta)

        endpoint = SyncEndpoint()
        endpoint.set_mail_response(Response())
        endpoint.add_rcpt_response(Response(401))

        fut = self.executor.submit(lambda: self.output('rest_tx_id', endpoint))
        self.assertIsNotNone(fut)


        self.write_envelope(tx_cursor,
                            TransactionMetadata(rcpt_to=[Mailbox('bob2')]))

        endpoint.add_rcpt_response(Response(402))


        blob_writer = self.storage.get_blob_writer()
        self.assertIsNotNone(blob_writer.create('rest_blob_id'))
        d = b'hello, world!'
        blob_writer.append_data(d, len(d))

        self.write_envelope(tx_cursor,
                            TransactionMetadata(body='rest_blob_id'),
                            reuse_blob_rest_id=['rest_blob_id'])

        fut.result(timeout=5)

    # incomplete transactions i.e. downstream timeout shouldn't be retried
    def test_downstream_timeout(self):
        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.create('rest_tx_id', TransactionMetadata(host='outbound'))
        tx_meta = TransactionMetadata(
            mail_from=Mailbox('alice'), rcpt_to=[Mailbox('bob1')])
        tx_cursor.write_envelope(tx_meta)

        endpoint = SyncEndpoint()
        endpoint.set_mail_response(Response(201))
        endpoint.add_rcpt_response(Response(202))

        reader = self.storage.load_one()
        self.assertEqual(tx_cursor.id, reader.id)
        retry_params = {'backoff_factor': 0,
                        'min_attempt_time': 0}
        handler = OutputHandler(
            reader, endpoint,
            downstream_env_timeout=1,
            downstream_data_timeout=1,
            retry_params=retry_params)
        handler.cursor_to_endpoint()

        reader = self.storage.load_one()
        self.assertIsNone(reader)

    def output(self, rest_id, endpoint):
        tx_cursor = self.storage.load_one()
        self.assertEqual(tx_cursor.rest_id, rest_id)
        handler = OutputHandler(tx_cursor, endpoint,
                                downstream_env_timeout=1,
                                downstream_data_timeout=1)
        handler.cursor_to_endpoint()

    def test_integrated(self):
        rest_tx = RestServiceTransaction.create_tx(
            self.storage, 'host', rest_id_factory=lambda: str(time.time()))
        self.assertIsNotNone(rest_tx)
        rest_tx.start(TransactionMetadata(mail_from=Mailbox('alice'),
                                          rcpt_to=[Mailbox('bob')]).to_json())
        rest_id : str = rest_tx.tx_rest_id()
        self.assertTrue(self.storage.wait_created(None, timeout=1))

        endpoint = SyncEndpoint()
        endpoint.set_mail_response(Response())
        endpoint.add_rcpt_response(Response(234))

        fut = self.executor.submit(lambda: self.output(rest_id, endpoint))

        # poll for start response
        # handler should wait a little if it's inflight but client
        # needs to be prepared to retry regardless
        for i in range(0,5):
            rest_tx = RestServiceTransaction.load_tx(self.storage, rest_id)
            self.assertIsNotNone(rest_tx)
            rest_resp = rest_tx.get({}, timeout=1)
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

        create_blob_handler = RestServiceTransaction.create_blob_handler(
            self.storage, rest_id_factory=lambda: str(time.time()))
        create_blob_resp = create_blob_handler.create_blob(FlaskRequest({}))
        self.assertEqual(create_blob_resp.status_code, 201)
        blob_uri = create_blob_handler.blob_rest_id()

        d = b'world!'
        for i in range(0, len(d)):
            logging.info('test_integrated put blob %d', i)
            blob_tx = RestServiceTransaction.load_blob(
                self.storage, blob_uri)
            self.assertIsNotNone(blob_tx)
            put_req = FlaskRequest({})  # wsgiref.types.WSGIEnvironment
            put_req.method = 'PUT'
            b = d[i:i+1]
            self.assertEqual(len(b), 1)
            put_req.data = b
            put_req.content_length = 1

            put_resp = blob_tx.put_blob(
                put_req, ContentRange('bytes', i, i+1, len(d)))
            self.assertEqual(200, put_resp.status_code)

        rest_resp = rest_tx.patch({'body': blob_uri})
        self.assertEqual(rest_resp.status_code, 200)

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
                self.assertIsNotNone(resp)
                self.assertEqual(245, resp.code)
                break
            time.sleep(0.5)
        else:
            self.fail('no final result')

        fut.result(timeout=1)

        self.assertEqual(endpoint.body_blob.read(0), b'world!')

    def test_next_attempt_time(self):
        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.create('rest_tx_id', TransactionMetadata(
            host='outbound',
            notification = {'host': 'smtp-out'},
            retry={}))

        tx_cursor = self.storage.load_one()
        retry_params = {}
        handler = OutputHandler(
            tx_cursor, endpoint=None,
            notification_factory=None,
            retry_params=retry_params)


        retry_params['min_attempt_time'] = 0
        retry_params['backoff_factor'] = 2
        now = tx_cursor.creation + 1
        final_attempt_reason, next_attempt_time = handler._next_attempt_time(
            now)
        self.assertIsNone(final_attempt_reason)
        self.assertEqual(next_attempt_time - now, 2)

        retry_params['min_attempt_time'] = 7
        now = tx_cursor.creation
        final_attempt_reason, next_attempt_time = handler._next_attempt_time(
            now)
        self.assertIsNone(final_attempt_reason)
        self.assertEqual(next_attempt_time - now, 7)

        retry_params['max_attempt_time'] = 17
        retry_params['backoff_factor'] = 100
        now = tx_cursor.creation + 1
        final_attempt_reason, next_attempt_time = handler._next_attempt_time(
            now)
        self.assertIsNone(final_attempt_reason)
        self.assertEqual(next_attempt_time - now, 17)

        retry_params['max_attempts'] = 1
        final_attempt_reason, next_attempt_time = handler._next_attempt_time(
            now)
        self.assertEqual(final_attempt_reason, 'retry policy max attempts')
        self.assertIsNone(next_attempt_time)

        retry_params['deadline'] = 0
        retry_params['max_attempts'] = 2
        final_attempt_reason, next_attempt_time = handler._next_attempt_time(
            now)
        self.assertEqual(final_attempt_reason, 'retry policy deadline')
        self.assertIsNone(next_attempt_time)


    def test_notification(self):
        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.create('rest_tx_id', TransactionMetadata(host='outbound'))

        tx_meta = TransactionMetadata(
            mail_from=Mailbox('alice'), rcpt_to=[Mailbox('bob')])
        tx_cursor.write_envelope(tx_meta)
        blob_writer = self.storage.get_blob_writer()
        blob_writer.create(rest_id='blob_rest_id')
        d = (b'from: alice\r\n'
             b'to: bob\r\n'
             b'message-id: <abc@xyz>\r\n'
             b'\r\n'
             b'hello\r\n')
        blob_writer.append_data(d, len(d))

        tx_cursor.write_envelope(
            TransactionMetadata(
                body='blob_rest_id',
                retry={'max_attempts': 1},
                notification = {'host': 'smtp-out'}),
            reuse_blob_rest_id=['blob_rest_id'])
        del tx_cursor

        endpoint = SyncEndpoint()
        endpoint.set_mail_response(Response())
        endpoint.add_rcpt_response(Response(450))

        notification_endpoint = SyncEndpoint()
        notification_endpoint.set_mail_response(Response())
        notification_endpoint.add_rcpt_response(Response())
        notification_endpoint.add_data_response(Response())

        tx_cursor = self.storage.load_one()
        self.assertIsNotNone(tx_cursor)
        self.assertEqual(tx_cursor.rest_id, 'rest_tx_id')
        handler = OutputHandler(
            tx_cursor, endpoint,
            downstream_env_timeout=1,
            downstream_data_timeout=1,
            notification_factory=lambda: notification_endpoint,
            mailer_daemon_mailbox='mailer-daemon@example.com')
        handler.cursor_to_endpoint()

        self.assertEqual(endpoint.mail_from.mailbox, 'alice')
        self.assertEqual(endpoint.rcpt_to[0].mailbox, 'bob')

        reader = self.storage.get_transaction_cursor()
        reader.load(rest_id='rest_tx_id')

        self.assertEqual(notification_endpoint.mail_from.mailbox, '')
        self.assertEqual(notification_endpoint.rcpt_to[0].mailbox, 'alice')

        dsn = notification_endpoint.body_blob.read(0).decode('utf-8')
        logging.debug(dsn)
        self.assertIn('subject: Delivery Status Notification', dsn)

if __name__ == '__main__':
    unittest.main()
