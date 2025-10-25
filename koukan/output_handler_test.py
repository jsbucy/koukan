# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import unittest
import logging
from koukan.executor import Executor
import time
from functools import partial

from koukan.storage import Storage, TransactionCursor
from koukan.storage_schema import BlobSpec, VersionConflictException
from koukan.response import Response
from koukan.output_handler import OutputHandler
from koukan.fake_endpoints import FakeFilter, MockAsyncFilter
from koukan.filter import Mailbox, TransactionMetadata
from koukan.filter_chain import FilterChain
from koukan.rest_schema import BlobUri
import koukan.sqlite_test_utils as sqlite_test_utils
from koukan.message_builder import MessageBuilderSpec

class OutputHandlerTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s [%(thread)d] '
                            '%(filename)s:%(lineno)d %(message)s')
        self.db_dir, self.db_url = sqlite_test_utils.create_temp_sqlite_for_test()
        self.storage = Storage.connect(self.db_url, 'http://output_handler_test')
        self.executor = Executor(inflight_limit=10, watchdog_timeout=10)

    def tearDown(self):
        self.executor.shutdown(timeout=5)
        self.db_dir.cleanup()

    def dump_db(self):
        with self.storage.begin_transaction() as db_tx:
            for l in db_tx.connection.iterdump():
                logging.debug(l)

    def output(self, rest_id, chain):
        tx_cursor = self.storage.load_one()
        self.assertEqual(tx_cursor.rest_id, rest_id)
        handler = OutputHandler(tx_cursor, chain,
                                downstream_timeout=3,
                                upstream_refresh=1)
        handler.handle()

    # ~rest
    def test_oneshot(self):
        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.create('rest_tx_id', TransactionMetadata(host='outbound'))

        tx_meta = TransactionMetadata(
            mail_from=Mailbox('alice'), rcpt_to=[Mailbox('bob')],
            body=BlobSpec(create_tx_body=True))
        tx_cursor.write_envelope(tx_meta)
        blob_uri = BlobUri(
            tx_id='rest_tx_id', tx_body=True, blob='blob_rest_id')
        blob_writer = tx_cursor.get_blob_for_append(blob_uri)
        d = b'hello, world!'
        for i in range(0,len(d)):
            blob_writer.append_data(i, d[i:i+1], len(d))

        del tx_cursor

        endpoint = FakeFilter()
        chain = FilterChain([endpoint])

        def exp(tx, tx_delta):
            self.assertEqual(tx.mail_from.mailbox, 'alice')
            self.assertEqual(tx.rcpt_to[0].mailbox, 'bob')
            self.assertEqual(tx.body.pread(0), b'hello, world!')

            upstream_delta = TransactionMetadata(
                mail_response = Response(201),
                rcpt_response = [Response(202)],
                data_response = Response(203))
            tx.merge_from(upstream_delta)
            return upstream_delta
        endpoint.add_expectation(exp)

        tx_cursor = self.storage.load_one()
        assert tx_cursor.in_attempt
        self.assertIsNotNone(tx_cursor)
        self.assertEqual(tx_cursor.rest_id, 'rest_tx_id')
        handler = OutputHandler(tx_cursor, chain,
                                downstream_timeout=2,
                                upstream_refresh=1)
        handler.handle()

        reader = self.storage.get_transaction_cursor()
        reader.load(rest_id='rest_tx_id')
        self.assertEqual(201, reader.tx.mail_response.code)
        self.assertEqual([202], [r.code for r in reader.tx.rcpt_response])
        self.assertEqual(203, reader.tx.data_response.code)

    # ~interactive smtp/exploder
    def test_interactive(self):
        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.create(
            'rest_tx_id',
            TransactionMetadata(host='outbound',
                                mail_from=Mailbox('alice')),
            create_leased=True)
        tx_id = tx_cursor.db_id

        endpoint = FakeFilter()
        chain = FilterChain([endpoint])

        def exp_mail(tx, tx_delta):
            logging.debug(tx)
            self.assertEqual(tx.mail_from.mailbox, 'alice')
            self.assertEqual(tx_delta.mail_from.mailbox, 'alice')
            upstream_delta = TransactionMetadata(
                mail_response = Response(201))
            tx.merge_from(upstream_delta)
            return upstream_delta
        endpoint.add_expectation(exp_mail)

        def exp_rcpt(tx, tx_delta):
            logging.debug(tx)
            self.assertEqual(tx.mail_from.mailbox, 'alice')
            for i in range(0, len(tx.rcpt_to)):
                self.assertEqual(tx.rcpt_to[i].mailbox, 'bob%d' % (i + 1))
            upstream_delta = TransactionMetadata()
            upstream_delta.rcpt_response_list_offset = len(tx.rcpt_response)
            for i in range(len(tx.rcpt_response), len(tx.rcpt_to)):
                upstream_delta.rcpt_response.append(Response(202 + i))

            tx.merge_from(upstream_delta)
            logging.debug(tx)
            return upstream_delta
        for i in range(0,6):
            endpoint.add_expectation(exp_rcpt)

        def run_handler(tx_cursor):
            self.assertTrue(tx_cursor.start_attempt())
            self.assertEqual(tx_cursor.rest_id, 'rest_tx_id')
            handler = OutputHandler(tx_cursor, chain,
                                    downstream_timeout=5,
                                    upstream_refresh=1)
            handler.handle()
        fut = self.executor.submit(partial(run_handler, tx_cursor))
        self.assertIsNotNone(fut)

        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.load(db_id=tx_id)

        # read mail resp
        for i in range(0,5):
            tx = tx_cursor.load().copy()
            if tx.mail_response is not None and tx.mail_response.code == 201:
                break
            time.sleep(1)
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
            while True:
                tx_cursor.load()
                try:
                    tx_cursor.write_envelope(delta)
                    break
                except VersionConflictException:
                    pass

            # read rcpt resp
            for j in range(0,5):
                tx_cursor.load()
                if ([r.code for r in tx_cursor.tx.rcpt_response] ==
                    rcpt_resp[0:i+1]):
                    break
                logging.debug('test_handle_multi rcpt_response %s',
                              tx_cursor.tx.rcpt_response)
                time.sleep(1)
            else:
                self.fail('failed to read ' + rcpt[i] + ' response')

        endpoint.expectation = []

        body = b'hello, world!'
        def exp_body(tx, tx_delta):
            logging.debug(tx)
            self.assertEqual(tx.mail_from.mailbox, 'alice')
            self.assertEqual([m.mailbox for m in tx.rcpt_to], ['bob1', 'bob2'])
            if tx.body is None or not tx.body.finalized():
                return TransactionMetadata()
            self.assertEqual(tx_delta.body.pread(0), body)
            upstream_delta = TransactionMetadata(
                data_response = Response(204))
            tx.merge_from(upstream_delta)
            return upstream_delta
        endpoint.add_expectation(exp_body)
        endpoint.add_expectation(exp_body)

        # write blob
        for i in range(0, 3):
            try:
                tx_cursor.write_envelope(
                    TransactionMetadata(body=BlobSpec(create_tx_body=True)))
                break
            except VersionConflictException:
                time.sleep(1)
        blob_writer = tx_cursor.get_blob_for_append(
            BlobUri(tx_id='rest_tx_id', tx_body=True, blob='blob_rest_id'))
        blob_writer.append_data(0, body, last=True)

        # read data resp
        for j in range(0,5):
            tx_cursor.load()
            if tx_cursor.tx.data_response:
                break
            time.sleep(1)
        else:
            self.fail('failed to read data response')
        self.assertEqual(204, tx_cursor.tx.data_response.code)

        fut.result(timeout=5)



    def write_envelope(self, cursor, tx):
        for i in range(1,3):
            try:
                cursor.write_envelope(tx)
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

        endpoint = FakeFilter()
        chain = FilterChain([endpoint])

        def exp_rcpt1(tx, tx_delta):
            logging.debug(tx)
            self.assertEqual(tx.mail_from.mailbox, 'alice')
            self.assertEqual([m.mailbox for m in tx.rcpt_to], ['bob1'])
            updated_tx = tx.copy()
            updated_tx.mail_response = Response(201)
            updated_tx.rcpt_response.append(Response(402))
            upstream_delta = tx.delta(updated_tx)
            tx.merge_from(upstream_delta)
            return upstream_delta
        endpoint.add_expectation(exp_rcpt1)

        fut = self.executor.submit(lambda: self.output('rest_tx_id', chain))

        while endpoint.expectation:
            time.sleep(0.1)

        def exp_rcpt2(tx, tx_delta):
            logging.debug(tx)
            upstream_delta = TransactionMetadata()
            if len(tx.rcpt_to) == 2 and len(tx.rcpt_response) == 1:
                upstream_delta.rcpt_response_list_offset = 1
                upstream_delta.rcpt_response = [Response(403)]
            tx.merge_from(upstream_delta)
            return upstream_delta
        for i in range(0,2):
            endpoint.add_expectation(exp_rcpt2)

        updated_tx = tx.copy()
        updated_tx.rcpt_to.append(Mailbox('bob2'))

        self.write_envelope(tx_cursor, tx.delta(updated_tx))

        while tx_cursor.tx.req_inflight():
            tx_cursor.wait(0.3)
            tx_cursor.load()
        time.sleep(1)
        self.assertEqual([402, 403], [r.code for r in tx_cursor.tx.rcpt_response])
        endpoint.expectation = []

        def exp_body(tx, tx_delta):
            logging.debug(tx)
            upstream_delta = TransactionMetadata()
            if tx.body and tx.body.finalized():
                tx.fill_inflight_responses(Response(450, 'cancelled'), upstream_delta)
            tx.merge_from(upstream_delta)
            return upstream_delta
        for i in range(0,3):
            endpoint.add_expectation(exp_body)


        for i in range(0,5):
            try:
                tx_cursor.load()
                tx_cursor.write_envelope(
                    TransactionMetadata(body=BlobSpec(create_tx_body=True)))
                break
            except VersionConflictException:
                time.sleep(0.2)
        else:
            self.fail('conflict')
        blob_writer = tx_cursor.get_blob_for_append(
            BlobUri(tx_id='rest_tx_id', tx_body=True, blob='rest_blob_id'))
        d = b'hello, world!'
        logging.debug('finalize body')
        blob_writer.append_data(0, d, last=True)

        fut.result(timeout=5)

        while tx_cursor.final_attempt_reason is None:
            tx_cursor.wait(0.3)
            tx_cursor.load()

        self.assertIn('oneshot', tx_cursor.final_attempt_reason)
        self.assertEqual(tx_cursor.tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx_cursor.tx.rcpt_response],
                         [402, 403])
        self.assertEqual(503, tx_cursor.tx.data_response.code)
        self.assertIn('precondition', tx_cursor.tx.data_response.message)


    # incomplete transactions i.e. downstream timeout shouldn't be retried
    def test_downstream_timeout(self):
        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.create('rest_tx_id', TransactionMetadata(host='outbound'))
        tx_meta = TransactionMetadata(
            mail_from=Mailbox('alice'), rcpt_to=[Mailbox('bob')])
        tx_cursor.write_envelope(tx_meta)

        endpoint = FakeFilter()
        chain = FilterChain([endpoint])

        def exp_rcpt(tx, tx_delta):
            self.assertEqual(tx.mail_from.mailbox, 'alice')
            self.assertEqual([m.mailbox for m in tx.rcpt_to], ['bob'])
            upstream_delta = TransactionMetadata(
                mail_response = Response(201),
                rcpt_response = [Response(202)])
            tx.merge_from(upstream_delta)
            return upstream_delta
        endpoint.add_expectation(exp_rcpt)
        def exp(tx, delta):
            pass
        for i in range(0,3):
            endpoint.add_expectation(exp)

        reader = self.storage.load_one()
        self.assertEqual(tx_cursor.db_id, reader.db_id)
        retry_params = {'backoff_factor': 0,
                        'min_attempt_time': 0}
        handler = OutputHandler(
            reader, chain,
            downstream_timeout=2,
            upstream_refresh=1,
            retry_params=retry_params)
        handler.handle()

        reader = self.storage.load_one()
        self.assertIsNone(reader)

        tx_cursor.load()
        self.assertEqual('downstream timeout', tx_cursor.final_attempt_reason)

    def test_downstream_cancel(self):
        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.create('rest_tx_id', TransactionMetadata(host='outbound'))
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'))
        tx_cursor.write_envelope(tx)

        endpoint = FakeFilter()
        chain = FilterChain([endpoint])

        started = False
        def exp_rcpt(tx, tx_delta):
            nonlocal started
            started = True
            logging.debug(tx)
            time.sleep(1)
            self.assertEqual(tx.mail_from.mailbox, 'alice')
            prev = tx.copy()
            if tx.mail_response is None:
                tx.mail_response = Response(201)
            return prev.delta(tx)
        for i in range(0,3):
            endpoint.add_expectation(exp_rcpt)

        fut = self.executor.submit(lambda: self.output('rest_tx_id', chain))

        while not started:
            logging.debug(started)
            time.sleep(0.1)

        for i in range(0,2):
            try:
                tx_cursor.write_envelope(
                    TransactionMetadata(cancelled=True),
                    final_attempt_reason='downstream cancelled')
                break
            except VersionConflictException:
                tx_cursor.load()

        fut.result()  # wait/propagate exceptions

        tx_cursor.load()
        self.assertTrue(tx_cursor.no_final_notification)
        self.assertIsNone(tx_cursor.tx.notification)
        self.assertEqual('downstream cancelled', tx_cursor.final_attempt_reason)
        self.assertIsNone(self.storage.load_one())

    def test_next_attempt_time(self):
        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.create('rest_tx_id', TransactionMetadata(
            host='outbound',
            notification = {'host': 'smtp-out'},
            retry={}))

        tx_cursor = self.storage.load_one()
        retry_params = {}
        handler = OutputHandler(
            tx_cursor, filter_chain=None,
            notification_endpoint_factory=None,
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



    # 1: handle w/notification request set that permfails
    def test_notification(self):
        tx = TransactionMetadata(
            host='outbound',
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')],
            body=BlobSpec(create_tx_body=True))
        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.create('rest_tx_id', tx)
        blob_uri = BlobUri(tx_id='rest_tx_id', tx_body=True)
        blob_writer = tx_cursor.get_blob_for_append(blob_uri)
        d = (b'from: alice\r\n'
             b'to: bob\r\n'
             b'message-id: <abc@xyz>\r\n'
             b'\r\n'
             b'hello\r\n')
        blob_writer.append_data(0, d, len(d))


        endpoint = FakeFilter()
        chain = FilterChain([endpoint])

        def exp(tx, tx_delta):
            upstream_delta = TransactionMetadata(
                mail_response = Response(201),
                rcpt_response = [Response(202)],
                data_response = Response(550))
            tx.merge_from(upstream_delta)
            return upstream_delta
        endpoint.add_expectation(exp)

        notification_endpoint = MockAsyncFilter()

        def exp_notification(tx, tx_delta):
            self.assertEqual(tx.mail_from.mailbox, '')
            self.assertEqual(tx.rcpt_to[0].mailbox, 'alice')
            dsn = tx.body.pread(0).decode('utf-8')
            logging.debug(dsn)
            self.assertIn('subject: Delivery Status Notification', dsn)

            upstream_delta = TransactionMetadata(
                mail_response=Response(),
                rcpt_response=[Response()],
                data_response=Response())
            tx.merge_from(upstream_delta)
            return upstream_delta, 1
        notification_endpoint.expect_update(exp_notification)

        tx_cursor = self.storage.load_one()
        self.assertIsNotNone(tx_cursor)
        self.assertEqual(tx_cursor.rest_id, 'rest_tx_id')
        handler = OutputHandler(
            tx_cursor, chain,
            notification_endpoint_factory=lambda: notification_endpoint,
            mailer_daemon_mailbox='mailer-daemon@example.com',
            downstream_timeout=2,
            upstream_refresh=1,
            retry_params={'max_attempts': 1},
            notification_params = {'host': 'smtp-out'})

        handler.handle()
        self.assertFalse(notification_endpoint.update_expectation)

        reader = self.storage.get_transaction_cursor()
        reader.load(rest_id='rest_tx_id')


    def test_notification_message_builder(self):
        body = MessageBuilderSpec({
                'headers': [
                    ["from", [{"display_name": "alice a",
                               "address": "alice@example.com"}]],
                    ["to", [{"address": "bob@example.com"}]],
                    ["subject", "hello"],
                    ["date", {"unix_secs": 1709750551, "tz_offset": -28800}],
                    ["message-id", ["abc@xyz"]]],
                'text_body': [{
                    "content_type": "text/plain",
                    "content": {"inline": "hello, world!"}
                }]
            })
        body.parse_blob_specs()

        tx = TransactionMetadata(
            host='outbound',
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')],
            body = body )

        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.create('rest_tx_id', tx)

        endpoint = FakeFilter()
        chain = FilterChain([endpoint])

        def exp(tx, tx_delta):
            logging.debug('test_notification_message_builder exp %s', tx)
            upstream_delta = TransactionMetadata(
                mail_response = Response(201),
                rcpt_response = [Response(202)],
                data_response = Response(550))
            tx.merge_from(upstream_delta)
            return upstream_delta
        endpoint.add_expectation(exp)

        notification_endpoint = MockAsyncFilter()

        def exp_notification(tx, tx_delta):
            self.assertEqual(tx.mail_from.mailbox, '')
            self.assertEqual(tx.rcpt_to[0].mailbox, 'alice')
            dsn = tx.body.pread(0).decode('utf-8')
            logging.debug(dsn)
            self.assertIn('subject: Delivery Status Notification', dsn)
            orig_headers = [
                'from: alice a <alice@example.com>',
                'to: bob@example.com',
                'subject: hello',
                'date: Wed, 06 Mar 2024 10:42:31 -0800',
                'message-id: <abc@xyz>']
            dsn = tx.body.pread(0).decode('utf-8')
            logging.debug(dsn)
            self.assertIn('subject: Delivery Status Notification', dsn)

            for h in orig_headers:
                self.assertIn(h, dsn)

            upstream_delta = TransactionMetadata(
                mail_response=Response(),
                rcpt_response=[Response()],
                data_response=Response())
            tx.merge_from(upstream_delta)
            return upstream_delta, 1
        notification_endpoint.expect_update(exp_notification)


        tx_cursor = self.storage.load_one()
        self.assertIsNotNone(tx_cursor)
        self.assertEqual(tx_cursor.rest_id, 'rest_tx_id')
        handler = OutputHandler(
            tx_cursor, chain,
            notification_endpoint_factory=lambda: notification_endpoint,
            mailer_daemon_mailbox='mailer-daemon@example.com',
            downstream_timeout=2,
            upstream_refresh=1,
            retry_params={'max_attempts': 1},
            notification_params={'host': 'smtp-out'})

        handler.handle()
        self.assertFalse(notification_endpoint.update_expectation)

        reader = self.storage.get_transaction_cursor()
        reader.load(rest_id='rest_tx_id')


    # 2: handle w/o notification that permfails,
    # enable notifications after handler done,
    # recover, handle -> dsn
    def test_notification_post_facto(self):
        tx = TransactionMetadata(
            host='outbound',
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')],
            body=BlobSpec(create_tx_body=True))
        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.create('rest_tx_id', tx)
        tx_id = tx_cursor.db_id
        blob_writer = tx_cursor.get_blob_for_append(
            BlobUri(tx_id='rest_tx_id', tx_body=True))
        d = (b'from: alice\r\n'
             b'to: bob\r\n'
             b'message-id: <abc@xyz>\r\n'
             b'\r\n'
             b'hello\r\n')
        blob_writer.append_data(0, d, len(d))

        endpoint = FakeFilter()
        chain = FilterChain([endpoint])

        def exp(tx, tx_delta):
            upstream_delta = TransactionMetadata(
                mail_response = Response(201),
                rcpt_response = [Response(202)],
                data_response = Response(550))
            tx.merge_from(upstream_delta)
            return upstream_delta
        endpoint.add_expectation(exp)

        # no notification_endpoint_factory since not requested
        tx_cursor = self.storage.load_one()
        handler = OutputHandler(
            tx_cursor, chain,
            mailer_daemon_mailbox='mailer-daemon@example.com',
            downstream_timeout=2,
            upstream_refresh=1,
            retry_params={'max_attempts': 1,
                          'mode': 'per_request'},
            notification_params={'host': 'notify-out',
                                 'mode': 'per_request'})
        handler.handle()

        # tx should not be loadable
        self.assertIsNone(self.storage.load_one())

        # now enable notifications on the tx
        tx_cursor = self.storage.get_transaction_cursor()
        tx_cursor.load(db_id=tx_id)
        tx_cursor.write_envelope(TransactionMetadata(
            retry={},
            notification={}))

        notification_endpoint = MockAsyncFilter()

        def exp_notification(tx, tx_delta):
            self.assertEqual(tx.mail_from.mailbox, '')
            self.assertEqual(tx.rcpt_to[0].mailbox, 'alice')
            dsn = tx.body.pread(0).decode('utf-8')
            logging.debug(dsn)
            self.assertIn('subject: Delivery Status Notification', dsn)
            upstream_delta = TransactionMetadata(
                mail_response=Response(),
                rcpt_response=[Response()],
                data_response=Response())
            tx.merge_from(upstream_delta)
            return upstream_delta, 1
        notification_endpoint.expect_update(exp_notification)

        endpoint = FakeFilter()
        chain = FilterChain([endpoint])

        tx_cursor = self.storage.load_one()
        self.assertIsNotNone(tx_cursor)
        handler = OutputHandler(
            tx_cursor, chain,
            notification_endpoint_factory=lambda: notification_endpoint,
            mailer_daemon_mailbox='mailer-daemon@example.com',
            downstream_timeout=2,
            upstream_refresh=1,
            retry_params={'max_attempts': 1},
            notification_params={'host': 'notify-out'})

        logging.debug('handle() for notification')
        handler.handle()
        self.assertFalse(notification_endpoint.update_expectation)


if __name__ == '__main__':
    unittest.main()
