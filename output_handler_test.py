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
from fake_endpoints import FakeAsyncEndpoint, SyncEndpoint
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
            blob_writer.append_data(i, d[i:i+1], len(d))

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
        blob_writer.append_data(0, d, len(d))

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
        blob_writer.append_data(0, d, len(d))

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

        notification_endpoint = FakeAsyncEndpoint(rest_id='rest-id')
        notification_endpoint.merge(
            TransactionMetadata(
                mail_response=Response(),
                rcpt_response=[Response()],
                data_response=Response()))

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

        self.assertEqual(notification_endpoint.tx.mail_from.mailbox, '')
        self.assertEqual(notification_endpoint.tx.rcpt_to[0].mailbox, 'alice')

        dsn = notification_endpoint.tx.body_blob.read(0).decode('utf-8')
        logging.debug(dsn)
        self.assertIn('subject: Delivery Status Notification', dsn)

if __name__ == '__main__':
    unittest.main()
