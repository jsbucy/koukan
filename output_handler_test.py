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
from fake_endpoints import FakeAsyncEndpoint, FakeSyncFilter
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
        with self.storage.begin_transaction() as db_tx:
            for l in db_tx.connection.iterdump():
                logging.debug(l)

    def output(self, rest_id, endpoint):
        tx_cursor = self.storage.load_one()
        self.assertEqual(tx_cursor.rest_id, rest_id)
        handler = OutputHandler(tx_cursor, endpoint,
                                downstream_env_timeout=1,
                                downstream_data_timeout=1)
        handler.cursor_to_endpoint()

    # oneshot ~rest
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

        endpoint = FakeSyncFilter()

        def exp(tx, tx_delta):
            self.assertEqual(tx.mail_from.mailbox, 'alice')
            self.assertEqual(tx.rcpt_to[0].mailbox, 'bob')
            self.assertEqual(tx.body_blob.read(0), b'hello, world!')

            upstream_delta = TransactionMetadata(
                mail_response = Response(),
                rcpt_response = [Response(234)],
                data_response = Response(256))
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        endpoint.add_expectation(exp)

        tx_cursor = self.storage.load_one()
        self.assertIsNotNone(tx_cursor)
        self.assertEqual(tx_cursor.rest_id, 'rest_tx_id')
        handler = OutputHandler(tx_cursor, endpoint,
                                downstream_env_timeout=1,
                                downstream_data_timeout=1)
        handler.cursor_to_endpoint()

        reader = self.storage.get_transaction_cursor()
        reader.load(rest_id='rest_tx_id')

    # multiple roundtrips ~smtp
    def test_cursor_to_endpoint_multi(self):
        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.create('rest_tx_id', TransactionMetadata(host='outbound'))

        endpoint = FakeSyncFilter()

        def exp_mail(tx, tx_delta):
            self.assertEqual(tx.mail_from.mailbox, 'alice')
            self.assertEqual(tx_delta.mail_from.mailbox, 'alice')
            upstream_delta = TransactionMetadata(
                mail_response = Response(201))
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        endpoint.add_expectation(exp_mail)

        def exp_rcpt1(tx, tx_delta):
            self.assertEqual(tx.mail_from.mailbox, 'alice')
            self.assertEqual(tx.rcpt_to[0].mailbox, 'bob1')
            self.assertEqual(tx_delta.rcpt_to[0].mailbox, 'bob1')

            upstream_delta = TransactionMetadata(
                rcpt_response = [Response(202)])
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        endpoint.add_expectation(exp_rcpt1)

        def exp_rcpt2(tx, tx_delta):
            self.assertEqual(tx.mail_from.mailbox, 'alice')
            self.assertEqual([m.mailbox for m in tx.rcpt_to], ['bob1', 'bob2'])
            self.assertEqual(tx_delta.rcpt_to[0].mailbox, 'bob2')

            updated_tx = tx.copy()
            updated_tx.rcpt_response.append(Response(203))
            upstream_delta = tx.delta(updated_tx)
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        endpoint.add_expectation(exp_rcpt2)

        def run_handler():
            tx_cursor = self.storage.load_one()
            self.assertIsNotNone(tx_cursor)
            self.assertEqual(tx_cursor.rest_id, 'rest_tx_id')
            handler = OutputHandler(tx_cursor, endpoint,
                                    downstream_env_timeout=1,
                                    downstream_data_timeout=1)
            handler.cursor_to_endpoint()
        fut = self.executor.submit(run_handler)
        self.assertIsNotNone(fut)

        # write mail
        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        #, rcpt_to=[Mailbox('bob')])
        tx_cursor.load()
        tx_cursor.write_envelope(tx)

        # read mail resp
        for i in range(0,3):
            tx_cursor.load()
            if tx_cursor.tx.mail_response is not None and tx_cursor.tx.mail_response.code == 201:
                break
            time.sleep(0.1)
        else:
            self.fail('failed to read mail response')

        rcpt = ['bob1', 'bob2']
        rcpt_resp = [202, 203]
        for i in range(0,2):
            # write rcpt
            updated_tx = tx.copy()
            updated_tx.rcpt_to.append(Mailbox(rcpt[i]))
            delta = tx.delta(updated_tx)
            tx = updated_tx
            tx_cursor.load()
            tx_cursor.write_envelope(delta)

            # read rcpt resp
            for j in range(0,3):
                tx_cursor.load()
                if ([r.code for r in tx_cursor.tx.rcpt_response] ==
                    rcpt_resp[0:i+1]):
                    break
                time.sleep(0.1)
            else:
                self.fail('failed to read ' + rcpt[i] + ' response')


        # write & patch blob

        blob_writer = self.storage.get_blob_writer()
        blob_writer.create(rest_id='blob_rest_id')
        body = b'hello, world!'
        blob_writer.append_data(0, body, len(body))

        def exp_body(tx, tx_delta):
            self.assertEqual(tx.mail_from.mailbox, 'alice')
            self.assertEqual([m.mailbox for m in tx.rcpt_to], ['bob1', 'bob2'])
            self.assertEqual(tx_delta.body_blob.read(0), body)
            upstream_delta = TransactionMetadata(
                data_response = Response(204))
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        endpoint.add_expectation(exp_body)


        tx_cursor.write_envelope(
            TransactionMetadata(
                body='blob_rest_id',
                retry = {'max_attempts': 100}),
            reuse_blob_rest_id=['blob_rest_id'])

        # read data resp
        for j in range(0,3):
            tx_cursor.load()
            if tx_cursor.tx.data_response and tx_cursor.tx.data_response.code == 204:
                break
            time.sleep(0.1)
        else:
            self.fail('failed to read data response')


        fut.result(timeout=5)



    def write_envelope(self, cursor, tx, reuse_blob_rest_id=None):
        for i in range(1,3):
            try:
                cursor.write_envelope(tx, reuse_blob_rest_id=reuse_blob_rest_id)
                break
            except VersionConflictException:
                time.sleep(0.1)
                cursor.load()
        else:
            self.fail('couldn\'t write envelope')

    def test_no_valid_rcpt(self):
        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.create('rest_tx_id', TransactionMetadata(host='outbound'))
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'), rcpt_to=[Mailbox('bob1')],
            retry={})
        tx_cursor.write_envelope(tx)

        endpoint = FakeSyncFilter()

        def exp_rcpt1(tx, tx_delta):
            self.assertEqual(tx.mail_from.mailbox, 'alice')
            self.assertEqual([m.mailbox for m in tx.rcpt_to], ['bob1'])
            updated_tx = tx.copy()
            updated_tx.mail_response = Response(201)
            updated_tx.rcpt_response.append(Response(402))
            upstream_delta = tx.delta(updated_tx)
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        endpoint.add_expectation(exp_rcpt1)

        fut = self.executor.submit(lambda: self.output('rest_tx_id', endpoint))

        def exp_rcpt2(tx, tx_delta):
            self.assertEqual([m.mailbox for m in tx_delta.rcpt_to], ['bob2'])
            updated_tx = tx.copy()
            updated_tx.rcpt_response.append(Response(403))
            upstream_delta = tx.delta(updated_tx)
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        endpoint.add_expectation(exp_rcpt2)

        updated_tx = tx.copy()
        updated_tx.rcpt_to.append(Mailbox('bob2'))

        self.write_envelope(tx_cursor, tx.delta(updated_tx))


        # no additional expectation on endpoint, should not send blob
        # upstream since all rcpts failed

        blob_writer = self.storage.get_blob_writer()
        self.assertIsNotNone(blob_writer.create('rest_blob_id'))
        d = b'hello, world!'
        blob_writer.append_data(0, d, len(d))

        self.write_envelope(tx_cursor,
                            TransactionMetadata(body='rest_blob_id'),
                            reuse_blob_rest_id=['rest_blob_id'])

        fut.result(timeout=5)

        tx_cursor.load()
        self.assertIsNone(tx_cursor.final_attempt_reason)
        self.assertEqual(tx_cursor.tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx_cursor.tx.rcpt_response], [402,403])
        self.assertIsNone(tx_cursor.tx.data_response)


    # incomplete transactions i.e. downstream timeout shouldn't be retried
    def test_downstream_timeout(self):
        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.create('rest_tx_id', TransactionMetadata(host='outbound'))
        tx_meta = TransactionMetadata(
            mail_from=Mailbox('alice'), rcpt_to=[Mailbox('bob')])
        tx_cursor.write_envelope(tx_meta)

        endpoint = FakeSyncFilter()
        def exp_rcpt(tx, tx_delta):
            self.assertEqual(tx.mail_from.mailbox, 'alice')
            self.assertEqual([m.mailbox for m in tx.rcpt_to], ['bob'])
            upstream_delta = TransactionMetadata(
                mail_response = Response(201),
                rcpt_response = [Response(202)])
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        endpoint.add_expectation(exp_rcpt)


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

        endpoint = FakeSyncFilter()
        def exp_rcpt(tx, tx_delta):
            self.assertEqual(tx.mail_from.mailbox, 'alice')
            self.assertEqual([m.mailbox for m in tx.rcpt_to], ['bob'])
            upstream_delta = TransactionMetadata(
                mail_response = Response(201),
                rcpt_response = [Response(402)])
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        endpoint.add_expectation(exp_rcpt)


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

        reader = self.storage.get_transaction_cursor()
        reader.load(rest_id='rest_tx_id')

        self.assertEqual(notification_endpoint.tx.mail_from.mailbox, '')
        self.assertEqual(notification_endpoint.tx.rcpt_to[0].mailbox, 'alice')

        dsn = notification_endpoint.tx.body_blob.read(0).decode('utf-8')
        logging.debug(dsn)
        self.assertIn('subject: Delivery Status Notification', dsn)

if __name__ == '__main__':
    unittest.main()
