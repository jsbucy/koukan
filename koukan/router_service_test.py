from typing import Any, List, Optional, Tuple
from threading import Lock, Condition
import logging
import unittest
import socketserver
import time
from functools import partial

from requests.exceptions import ConnectionError

import koukan.postgres_test_utils as postgres_test_utils
from koukan.router_service import Service
from koukan.rest_endpoint import RestEndpoint
from koukan.response import Response
from koukan.blob import CompositeBlob, InlineBlob
from koukan.config import Config
from koukan.fake_endpoints import FakeSyncFilter
from koukan.filter import (
    HostPort,
    Mailbox,
    SyncFilter,
    TransactionMetadata,
    WhichJson )
from koukan.executor import Executor

import koukan.sqlite_test_utils as sqlite_test_utils

def setUpModule():
    postgres_test_utils.setUpModule()

def tearDownModule():
    postgres_test_utils.tearDownModule()


root_yaml = {
    'global': {
        'tx_idle_timeout': 5,
        'gc_interval': None,
        # XXX only the notification tests still micromanage this, it
        # should be enabled for the rest
        'dequeue': False,
        'mailer_daemon_mailbox': 'mailer-daemon@d',
        'rest_id_entropy': 2
    },
    'rest_listener': {
    },
    'endpoint': [
        {
            'name': 'smtp-msa',
            'msa': True,
            'output_handler': {
                'downstream_env_timeout': 10,
                'downstream_data_timeout': 10,
                'retry_params': {
                    'max_attempts': 3,
                    'min_attempt_time': 1,
                    'max_attempt_time': 1,
                    'backoff_factor': 0,
                    'deadline': 300,
                }
            },
            'chain': [{'filter': 'exploder',
                       'output_chain': 'submission',
                       'msa': True,
                       'rcpt_timeout': 10,
                       'data_timeout': 10,
                       'default_notification': {
                           'host': 'submission'
                       }}]
        },
        {
            'name': 'submission',
            'msa': True,
            'output_handler': {
                'downstream_env_timeout': 10,
                'downstream_data_timeout': 10,
                'retry_params': {
                    'max_attempts': 3,
                    'min_attempt_time': 1,
                    'max_attempt_time': 1,
                    'backoff_factor': 0,
                    'deadline': 300,
                }
            },
            'chain': [
                {'filter': 'message_builder'},
                {'filter': 'sync'}
            ]
        },
        {
            'name': 'smtp-in',
            'msa': True,
            'output_handler': {
                'downstream_env_timeout': 1,
                'downstream_data_timeout': 1,
            },
            'chain': [{'filter': 'exploder',
                       'output_chain': 'inbound-gw',
                       'msa': False,
                       'default_notification': {
                           'host': 'inbound-gw'
                       }}]
        },
        {
            'name': 'inbound-gw',
            'msa': True,
            'output_handler': {
                'downstream_env_timeout': 1,
                'downstream_data_timeout': 1
            },
            'chain': [
                {'filter': 'router',
                 'policy': {
                     'name': 'address_list',
                     'domains': ['d'],
                     #endpoint: https://localhost:8001
                     'destination': {'options': { 'receive_parsing': {}}}
                     }
                },
                {'filter': 'router',
                 'policy': { 'name': 'address_list' }},
                {'filter': 'message_parser'},
                {'filter': 'sync'} ]
        },
    ],
    'storage': {
        'session_refresh_interval': 1,

        'gc_ttl': 0,
        'gc_interval': None,  # don't start, we'll invoke in the tests
    },
    'executor': {
        'max_inflight': 10,
        'watchdog_timeout': 10,
        'testonly_debug_futures': True
    }
}

class RouterServiceTest(unittest.TestCase):
    lock : Lock
    cv : Condition
    endpoints : List[FakeSyncFilter]
    use_postgres = True

    def get_endpoint(self):
        logging.debug('RouterServiceTest.get_endpoint')
        with self.lock:
            self.cv.wait_for(lambda: bool(self.endpoints))
            return self.endpoints.pop(0)

    def add_endpoint(self, endpoint):
        with self.lock:
            self.endpoints.append(endpoint)
            self.cv.notify_all()

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s [%(thread)d] %(message)s')

        self.lock = Lock()
        self.cv = Condition(self.lock)

        if self.use_postgres:
            root_yaml['storage']['engine'] = 'postgres'
            self.pg = postgres_test_utils.setup_postgres(root_yaml['storage'])
        else:
            self.dir, self.db_filename = sqlite_test_utils.create_temp_sqlite_for_test()
            root_yaml['storage']['engine'] = 'sqlite'
            root_yaml['storage']['sqlite_db_filename'] = self.db_filename

        # find a free port
        with socketserver.TCPServer(("localhost", 0), lambda x,y,z: None) as s:
            self.port = s.server_address[1]
        root_yaml['rest_listener']['addr'] = ('127.0.0.1', self.port)
        root_yaml['rest_listener']['use_fastapi'] = self.use_fastapi
        self.router_url = 'http://localhost:%d' % self.port
        self.endpoints = []
        self.config = Config()
        self.config.inject_yaml(root_yaml)
        self.config.inject_filter(
            'sync', lambda yaml, next: self.get_endpoint(), SyncFilter)
        self.service = Service(config=self.config)
        self.service.start_main()

        self.assertTrue(self.service.wait_started(5))

        # probe for startup
        def exp(tx, tx_delta):
            upstream_delta = TransactionMetadata(
                mail_response = Response(299, 'probe mail ok'))
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        def exp_cancel(tx, tx_delta):
            return TransactionMetadata()

        for i in range(0,10):
            logging.info('RouterServiceTest.setUp probe %d', i)

            upstream = FakeSyncFilter()
            upstream.add_expectation(exp)
            upstream.add_expectation(exp_cancel)
            self.add_endpoint(upstream)

            rest_endpoint = RestEndpoint(
                static_base_url=self.router_url,
                static_http_host='submission',
                timeout_start=1, timeout_data=1)
            tx = TransactionMetadata(
                mail_from = Mailbox('probe-from%d' % i))
            rest_endpoint.on_update(tx, tx.copy(), 5)
            delta = TransactionMetadata(cancelled=True)
            tx.merge_from(delta)
            rest_endpoint.on_update(tx, delta)
            logging.info('RouterServiceTest.setUp %s', tx.mail_response)
            if tx.mail_response.ok():
                break
            # we may have gotten http error before we got this far
            self.endpoints = []
            time.sleep(1)
        else:
            self.fail('service not ready')

        self.assertFalse(self.endpoints)

        # gc the probe request so it doesn't cause confusion later
        self.assertTrue(self.service._gc(0))

    def tearDown(self):
        # TODO this should verify that there are no open tx attempts in storage
        # e.g. some exception path failed to tx_cursor.finalize_attempt()
        self.assertTrue(self.service.shutdown())

    def dump_db(self):
        with self.service.storage.begin_transaction() as db_tx:
            for l in db_tx.connection.iterdump():
                logging.debug(l)

    def assertRcptCodesEqual(self, responses : List[Optional[Response]],
                             expected_codes):
        self.assertEqual([r.code if r else None for r in responses],
                         expected_codes)

    def assertRcptJsonCodesEqual(self, resp_json, expected_codes):
        self.assertRcptCodesEqual(
            [Response.from_json(r) for r in resp_json.get('rcpt_response', [])],
            expected_codes)

    def _dequeue(self, n=1):
        deq = 0
        for i in range(0,100):
            if self.service._dequeue():
                deq += 1
                logging.debug('RouterServiceTest._dequeue %d', deq)
            if deq == n:
                return
            time.sleep(0.01)
        else:
            self.fail('failed to dequeue')

    # simplest possible case: native rest/inline body -> upstream success
    def test_rest_smoke(self):
        logging.debug('RouterServiceTest.test_rest_smoke')
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, static_http_host='submission',
            timeout_start=5, timeout_data=5)
        body = 'hello, world!'
        tx = TransactionMetadata(
            #retry={},
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')],
            inline_body=body)

        def exp(tx, tx_delta):
            upstream_delta=TransactionMetadata(
                mail_response=Response(201),
                rcpt_response=[Response(202)],
                data_response=Response(203))
            self.assertTrue(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream_endpoint = FakeSyncFilter()
        upstream_endpoint.add_expectation(exp)
        self.add_endpoint(upstream_endpoint)

        rest_endpoint.on_update(tx, tx.copy())

        for i in range(0,5):
            tx_json = rest_endpoint.get_json(timeout=2)
            logging.debug('RouterServiceTest.test_rest_smoke create %s',
                          tx_json)
            if 'attempt_count' in tx_json:
                del tx_json['attempt_count']
            if tx_json == {
                #'attempt_count': 1,
                #'retry': {},
                'mail_from': {},
                'rcpt_to': [{}],
                'body': {},
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'}],
                'data_response': {'code': 203, 'message': 'ok'},
                'final_attempt_reason': 'upstream response success'}:
                break
        else:
            self.fail('didn\'t get expected transaction %s' % tx_json)

        self.service.daemon_executor.submit(
            partial(self.service.gc, self.service.daemon_executor))

        for i in range(0,5):
            if rest_endpoint.get_json(timeout=2) is None:
                break
            time.sleep(1)
        else:
            self.fail('expected 404 after gc')

    def test_rest_body(self):
        logging.debug('RouterServiceTest.test_rest_body')
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, static_http_host='submission',
            timeout_start=5, timeout_data=5)
        body = 'hello, world!'
        tx = TransactionMetadata(
            #retry={},
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')],
            body_blob=InlineBlob(body))

        def exp(tx, tx_delta):
            upstream_delta=TransactionMetadata(
                mail_response=Response(201),
                rcpt_response=[Response(202)],
                data_response=Response(203))
            self.assertTrue(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream_endpoint = FakeSyncFilter()
        upstream_endpoint.add_expectation(exp)
        self.add_endpoint(upstream_endpoint)

        rest_endpoint.on_update(tx, tx.copy())
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])
        self.assertEqual(tx.data_response.code, 203)

        # the previous on_update() waited on req_inflight() so this
        # just does a point read
        tx_json = rest_endpoint.get_json()
        logging.debug('RouterServiceTest.test_rest_body create %s',
                      tx_json)

        if 'attempt_count' in tx_json:
            del tx_json['attempt_count']
        self.assertEqual(tx_json, {
            #'attempt_count': 1,
            #'retry': {},
            'mail_from': {},
            'rcpt_to': [{}],
            'body': {},
            'mail_response': {'code': 201, 'message': 'ok'},
            'rcpt_response': [{'code': 202, 'message': 'ok'}],
            'data_response': {'code': 203, 'message': 'ok'},
            'final_attempt_reason': 'upstream response success'
        })


    def test_reuse_body(self):
        logging.debug('RouterServiceTest.test_reuse_body')
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, static_http_host='submission',
            timeout_start=5, timeout_data=5)
        body = 'hello, world!'
        body_utf8 = body.encode('utf-8')
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')],
            inline_body=body)

        upstream_endpoint = FakeSyncFilter()
        def exp_env(tx, tx_delta):
            upstream_delta = TransactionMetadata(
                mail_response = Response(201),
                rcpt_response = [Response(202)])
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        def exp_body(tx, tx_delta):
            self.assertEqual(tx.body_blob.read(0), body_utf8)
            upstream_delta = TransactionMetadata(
                data_response = Response(203))
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        upstream_endpoint.add_expectation(exp_env)
        upstream_endpoint.add_expectation(exp_body)
        self.add_endpoint(upstream_endpoint)

        rest_endpoint.on_update(tx, tx.copy())

        tx_json = rest_endpoint.get_json()
        logging.debug('RouterServiceTest.test_reuse_body create %s',
                      tx_json)

        tx_url = rest_endpoint.transaction_path

        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, static_http_host='submission',
            timeout_start=5, timeout_data=5)
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob2')],
            body=tx_url + '/body')

        upstream_endpoint = FakeSyncFilter()
        def exp2(tx, tx_delta):
            upstream_delta = TransactionMetadata(
                mail_response = Response(201),
                rcpt_response = [Response(202)],
            data_response = Response(203))
            self.assertEqual(tx.body_blob.read(0), body_utf8)
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        upstream_endpoint.add_expectation(exp2)
        self.add_endpoint(upstream_endpoint)

        rest_endpoint.on_update(tx, tx.copy())
        tx_json = rest_endpoint.get_json()



    # xxx need non-exploder test w/filter api, problems in
    # post-exploder/upstream tx won't be reported out synchronously,
    # would potentially bounce
    # and/or get ahold of those tx IDs and verify the status directly


    def test_exploder_multi_rcpt(self):
        logging.info('RouterServiceTest.test_exploder_multi_rcpt')
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, static_http_host='smtp-msa',
            timeout_start=5, timeout_data=5)

        logging.info('testExploderMultiRcpt start tx')
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            remote_host=HostPort('1.2.3.4', 12345))
        rest_endpoint.on_update(tx, tx.copy())

        # no rcpt -> buffered
        self.assertEqual(tx.mail_response.code, 250)
        self.assertIn('exploder noop', tx.mail_response.message)

        # upstream tx #1
        upstream_endpoint = FakeSyncFilter()
        self.add_endpoint(upstream_endpoint)
        def exp_rcpt1(tx, tx_delta):
            # set upstream responses so output (retry) succeeds
            # upstream success, retry succeeds, propagates down to rest
            updated_tx = tx.copy()
            updated_tx.mail_response = Response(201)
            updated_tx.rcpt_response.append(Response(202, 'upstream rcpt 1'))
            upstream_delta = tx.delta(updated_tx)
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        upstream_endpoint.add_expectation(exp_rcpt1)

        logging.info('testExploderMultiRcpt patch rcpt1')
        updated_tx = tx.copy()
        updated_tx.rcpt_to.append(Mailbox('bob'))
        tx_delta = tx.delta(updated_tx)
        tx = updated_tx
        rest_endpoint.on_update(tx, tx_delta)
        self.assertRcptCodesEqual(tx.rcpt_response, [202])
        self.assertEqual(tx.rcpt_response[0].message, 'upstream rcpt 1')

        # upstream tx #2
        upstream_endpoint2 = FakeSyncFilter()
        self.add_endpoint(upstream_endpoint2)

        def exp_rcpt2(tx, tx_delta):
            # set upstream responses so output (retry) succeeds
            # upstream success, retry succeeds, propagates down to rest
            updated_tx = tx.copy()
            updated_tx.mail_response = Response(203)
            updated_tx.rcpt_response.append(Response(204, 'upstream rcpt 2'))
            upstream_delta = tx.delta(updated_tx)
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        upstream_endpoint2.add_expectation(exp_rcpt2)

        logging.info('testExploderMultiRcpt patch rcpt2')
        updated_tx = tx.copy()
        updated_tx.rcpt_to.append(Mailbox('bob2'))
        tx_delta = tx.delta(updated_tx)
        tx = updated_tx
        rest_endpoint.on_update(tx, tx_delta)

        self.assertRcptCodesEqual(tx.rcpt_response, [202, 204])
        self.assertEqual(tx.rcpt_response[1].message, 'upstream rcpt 2')

        def exp_body(i, tx, tx_delta):
            self.assertEqual(tx.body_blob.read(0),
                             b'Hello, World!')
            updated_tx = tx.copy()
            updated_tx.data_response = Response(205 + i, 'upstream data %d' % i)
            upstream_delta = tx.delta(updated_tx)
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta

        for i,upstream in enumerate([upstream_endpoint, upstream_endpoint2]):
            upstream.add_expectation(partial(exp_body, i))

        logging.info('testExploderMultiRcpt patch body_blob')

        tx_delta = TransactionMetadata(
            body_blob=InlineBlob(b'Hello, World!'))
        self.assertIsNotNone(tx.merge_from(tx_delta))
        rest_endpoint.on_update(tx, tx_delta)
        logging.debug('test_exploder_multi_rcpt %s', tx)
        self.assertEqual(tx.data_response.code, 205)
        self.assertEqual(tx.data_response.message,
                         'exploder same status: upstream data 0')


    def test_notification_retry_timeout(self):
        logging.info('RouterServiceTest.test_notification_retry_timeout')
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, static_http_host='smtp-msa')

        def exp(tx, tx_delta):
            self.assertEqual(tx.mail_from.mailbox, 'alice')
            self.assertEqual([m.mailbox for m in tx.rcpt_to], ['bob'])
            # set upstream responses so output (retry) succeeds
            # upstream success, retry succeeds, propagates down to rest
            upstream_delta = TransactionMetadata(
                mail_response = Response(201),
            rcpt_response = [Response(402)])
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        upstream_endpoint = FakeSyncFilter()
        upstream_endpoint.add_expectation(exp)
        self.add_endpoint(upstream_endpoint)

        def exp_cancel(tx, tx_delta):
            self.assertTrue(tx_delta.cancelled)
            return TransactionMetadata()
        upstream_endpoint.add_expectation(exp_cancel)

        logging.info('test_notification start tx')
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')],
            body_blob=InlineBlob(b'Hello, World!'),
            remote_host=HostPort('1.2.3.4', 12345))
        rest_endpoint.on_update(tx, tx.copy(), 10)
        logging.debug('RouterServiceTest.test_notification after update %s',
                      tx)

        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual(tx.mail_response.message, 'ok')
        self.assertEqual([r.code for r in tx.rcpt_response], [250])
        self.assertIn('exploder store&forward RCPT',
                      tx.rcpt_response[0].message)

        for i in range(0,2):
            logging.debug('test_notification upstream tx %d', i)
            upstream_endpoint = FakeSyncFilter()
            def exp(tx, tx_delta):
                self.assertEqual(tx.mail_from.mailbox, 'alice')
                self.assertEqual([m.mailbox for m in tx.rcpt_to], ['bob'])
                # set upstream responses so output (retry) succeeds
                # upstream success, retry succeeds, propagates down to rest
                upstream_delta = TransactionMetadata(
                    mail_response = Response(201),
                    rcpt_response = [Response(402)])
                assert tx.merge_from(upstream_delta) is not None
                return upstream_delta
            upstream_endpoint.add_expectation(exp)
            upstream_endpoint.add_expectation(exp_cancel)

            self.add_endpoint(upstream_endpoint)

            if i == 1:
                dsn_endpoint = FakeSyncFilter()
                def exp_dsn(tx, tx_delta):
                    self.assertEqual(tx.mail_from.mailbox, '')
                    self.assertEqual([m.mailbox for m in tx.rcpt_to], ['alice'])
                    dsn = tx.body_blob.read(0)
                    logging.debug('test_notification %s', dsn)
                    self.assertIn(b'subject: Delivery Status Notification', dsn)

                    upstream_delta = TransactionMetadata(
                        mail_response = Response(203),
                        rcpt_response = [Response(204)],
                        data_response = Response(205))
                    assert tx.merge_from(upstream_delta) is not None
                    return upstream_delta
                dsn_endpoint.add_expectation(exp_dsn)

                self.add_endpoint(dsn_endpoint)
                self._dequeue()


    def test_notification_fast_perm(self):
        logging.info('RouterServiceTest.test_notification_fast_perm')
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, static_http_host='smtp-msa')

        logging.info('test_notification start tx')
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob1'),
                     Mailbox('bob2')],
            body_blob=InlineBlob(b'Hello, World!'),
            remote_host=HostPort('1.2.3.4', 12345))

        def exp_rcpt(tx, tx_delta):
            rcpt = tx_delta.rcpt_to[0].mailbox
            self.assertIn(rcpt, ['bob1', 'bob2'])
            data_resp = 550 if rcpt == 'bob2' else 250
            upstream_delta=TransactionMetadata(
                mail_response=Response(201),
                rcpt_response=[Response(202)],
                data_response=Response(data_resp))
            tx.merge_from(upstream_delta)
            return upstream_delta

        # output for each of rcpt1, rcpt2
        for j in range(0,2):
            upstream = FakeSyncFilter()
            upstream.add_expectation(exp_rcpt)
            self.add_endpoint(upstream)

        rest_endpoint.on_update(tx, tx.copy(), 10)
        logging.debug('RouterServiceTest.test_notification after update %s',
                      tx)
        # we get the upstream MAIL response since exploder got the
        # whole env in the same call
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual(tx.mail_response.message, 'ok')
        self.assertEqual(len(tx.rcpt_response), 2)
        for r in tx.rcpt_response:
            self.assertEqual(r.code, 202)
            self.assertEqual(r.message, 'ok')
        self.assertEqual(tx.data_response.code, 250)
        self.assertIn('exploder store&forward DATA', tx.data_response.message)

        # no_final_notification OutputHandler handler for both rcpts
        for i in range(0,2):
            unused_upstream_endpoint = FakeSyncFilter()
            self.add_endpoint(unused_upstream_endpoint)

        dsn_endpoint = FakeSyncFilter()
        def exp_dsn_env(tx, tx_delta):
            self.assertEqual(tx.mail_from.mailbox, '')
            self.assertEqual(tx.rcpt_to[0].mailbox, 'alice')
            upstream_delta=TransactionMetadata(
                mail_response=Response(250),
                rcpt_response=[Response(250)])
            tx.merge_from(upstream_delta)
            return upstream_delta

        def exp_dsn_body(tx, tx_delta):
            dsn = tx.body_blob.read(0)
            logging.debug('test_notification %s', dsn)
            self.assertIn(b'subject: Delivery Status Notification', dsn)

            upstream_delta=TransactionMetadata(
                data_response=Response(250))
            tx.merge_from(upstream_delta)
            return upstream_delta
        dsn_endpoint.add_expectation(exp_dsn_env)
        dsn_endpoint.add_expectation(exp_dsn_body)
        self.add_endpoint(dsn_endpoint)
        # no_final_notification x2 + dsn output
        self._dequeue(3)

        # xxx kludge, fix SyncEndpoint to wait on this
        for i in range(0,5):
            if not dsn_endpoint.expectation:
                break
            time.sleep(0.1)
        else:
            self.fail('didn\'t get dsn')


    # message builder is a rest submission feature; first-class rest
    # never uses the exploder
    def test_message_builder(self):
        logging.info('RouterServiceTest.test_message_builder')
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, static_http_host='submission')

        upstream_endpoint = FakeSyncFilter()
        self.add_endpoint(upstream_endpoint)

        logging.info('test_notification start tx')
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob1')],
            remote_host=HostPort('1.2.3.4', 12345))
        message_builder_spec = {
            "headers": [
                ["from", [{"display_name": "alice a",
                           "address": "alice@example.com"}]],
                ["to", [{"address": "bob@example.com"}]],
                ["subject", "hello"],
                ["date", {"unix_secs": 1709750551, "tz_offset": -28800}],
                ["message-id", ["abc@xyz"]],
            ],
            "text_body": [{
                "content_type": "text/plain",
                "content_uri": "my_plain_body"
            }]
        }
        tx.message_builder = message_builder_spec
        def exp_env(tx, tx_delta):
            updated_tx = tx.copy()
            updated_tx.mail_response = Response(201)
            updated_tx.rcpt_response.append(Response(202))
            upstream_delta = tx.delta(updated_tx)
            self.assertIsNotNone(tx.merge_from(upstream_delta))
            return upstream_delta

        upstream_endpoint.add_expectation(exp_env)
        rest_endpoint.on_update(tx, tx.copy(), 1)

        def exp_wakeup(tx, tx_delta):
            logging.debug('test_message_builder.exp_wakeup %s', tx)
            return TransactionMetadata()
        upstream_endpoint.add_expectation(exp_wakeup)


        b = b"hello, world!"
        blob = InlineBlob(b)
        blob_path = rest_endpoint.transaction_path + '/blob/' + 'my_plain_body'
        rest_endpoint.blob_url = rest_endpoint.transaction_url + '/blob/' + 'my_plain_body'
        blob_resp = rest_endpoint._put_blob(blob, non_body_blob=True)
        self.assertEqual(blob_resp.code, 200)


        def exp_body(tx, tx_delta):
            logging.debug('test_message_builder.exp_body %s', tx)
            body = tx.body_blob.read(0)
            logging.debug(body)
            self.assertIn(b'subject: hello\r\n', body)
            #self.assertIn(b, body)  # base64
            updated_tx = tx.copy()
            updated_tx.data_response = Response(203)
            upstream_delta = tx.delta(updated_tx)
            self.assertIsNotNone(tx.merge_from(upstream_delta))
            return upstream_delta

        upstream_endpoint.add_expectation(exp_body)

        message_builder_spec['text_body'][0]['content_uri'] = blob_path

        # send another tx with the same spec to exercise blob reuse
        logging.info('RouterServiceTest.test_message_builder start tx #2')
        tx2 = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob2')],
            remote_host=HostPort('1.2.3.4', 12345),
            message_builder=message_builder_spec)

        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, static_http_host='submission')

        upstream_endpoint = FakeSyncFilter()
        self.add_endpoint(upstream_endpoint)

        def exp2(tx, tx_delta):
            logging.debug('test_message_builder.exp2 %s', tx)
            self.assertEqual(tx.mail_from.mailbox, 'alice')
            self.assertEqual([m.mailbox for m in tx.rcpt_to], ['bob2'])
            return TransactionMetadata(
                mail_response = Response(204),
                rcpt_response = [Response(205)])

        def exp2_body(tx, tx_delta):
            logging.debug('test_message_builder.exp2_body %s', tx)
            body = tx.body_blob.read(0)
            logging.debug(body)
            self.assertIn(b'subject: hello\r\n', body)

            upstream_delta = TransactionMetadata(
                data_response = Response(206))

            self.assertIsNotNone(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream_endpoint.add_expectation(exp2)
        upstream_endpoint.add_expectation(exp2_body)

        rest_endpoint.on_update(tx2, tx2.copy(), 10)


    def test_receive_parsing(self):
        logging.info('RouterServiceTest.test_receive_parsing')
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, static_http_host='smtp-in',
            timeout_start=5, timeout_data=5)
        with open('testdata/multipart.msg', 'rb') as f:
            body = f.read()
        tx = TransactionMetadata(
            #retry={},
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob@d')],
            inline_body=body.decode('ascii'))

        def exp(tx, tx_delta):
            logging.debug('test_receive_parsing %s', tx)
            upstream_delta=TransactionMetadata()
            if tx_delta.mail_from:
                upstream_delta.mail_response=Response(201)
            if tx_delta.rcpt_to:
                upstream_delta.rcpt_response=[Response(202)]
            if tx_delta.body_blob:
                logging.debug('test_receive_parsing json %s',
                              tx_delta.parsed_json)
                logging.debug('test_receive_parsing blobs %s',
                              tx_delta.parsed_blobs)
                logging.debug('test_receive_parsing body %s',
                              tx_delta.body_blob)

                self.assertIn(['subject', 'hello'],
                              tx_delta.parsed_json['parts']['headers'])
                # NOTE: I can't figure out how to get email.parser to
                # leave the line endings alone
                logging.debug('test_receive_parsing exp parsed_blobs %s',
                              tx_delta.parsed_blobs)
                self.assertEqual(tx_delta.parsed_blobs[0].read(0),
                                 b'yolocat')
                self.assertEqual(tx_delta.parsed_blobs[1].read(0),
                                 b'yolocat2')
                self.assertIsNotNone(tx_delta.parsed_json)
                self.assertIsNotNone(tx_delta.parsed_blobs)
                upstream_delta.data_response=Response(203)
            self.assertTrue(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream_endpoint = FakeSyncFilter()
        upstream_endpoint.add_expectation(exp)
        upstream_endpoint.add_expectation(exp)
        self.add_endpoint(upstream_endpoint)

        rest_endpoint.on_update(tx, tx.copy())
        self.assertEqual(tx.mail_response.code, 201)
        self.assertRcptCodesEqual(tx.rcpt_response, [202])
        self.assertEqual(tx.data_response.code, 203)


class RouterServiceTestFlask(RouterServiceTest):
    use_fastapi = False

class RouterServiceTestFastApi(RouterServiceTest):
    use_fastapi = True

class RouterServiceTestSqlite(RouterServiceTest):
    use_fastapi = True
    use_postgres = False

if __name__ == '__main__':
    unittest.removeHandler()
    unittest.main(catchbreak=False)
