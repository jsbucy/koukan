from typing import Any, List, Optional, Tuple
from threading import Lock, Condition
import logging
import unittest
import socketserver
import time
from functools import partial

from requests.exceptions import ConnectionError

import psycopg
import psycopg.errors
import testing.postgresql

from router_service import Service
from rest_endpoint import RestEndpoint
from response import Response
from blob import CompositeBlob, InlineBlob
from config import Config
from fake_endpoints import FakeSyncFilter
from filter import (
    HostPort,
    Mailbox,
    SyncFilter,
    TransactionMetadata,
    WhichJson )
from executor import Executor



def setUpModule():
    global pg_factory
    pg_factory = testing.postgresql.PostgresqlFactory(cache_initialized_db=True)


def tearDownModule():
    global pg_factory
    pg_factory.clear_cache()


root_yaml = {
    'global': {
        'use_gunicorn': False,
        'use_hypercorn': False,
        'tx_idle_timeout': 5,
        'gc_interval': None,
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
                'downstream_env_timeout': 1,
                'downstream_data_timeout': 1,
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
                       'rcpt_timeout': 2,
                       'data_timeout': 2,
                       'default_notification': {
                           'host': 'submission'
                       }}]
        },
        {
            'name': 'submission',
            'msa': True,
            'output_handler': {
                'downstream_env_timeout': 2,
                'downstream_data_timeout': 2,
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
            'chain': [{'filter': 'sync'}]
        },
    ],
    'storage': {
        'engine': 'postgres',  #'sqlite_memory'
        'postgres_db_name': 'storage_test',
    }
}

class RouterServiceTest(unittest.TestCase):
    lock : Lock
    cv : Condition
    endpoints : List[FakeSyncFilter]
    executor : Executor

    def get_endpoint(self):
        logging.debug('RouterServiceTest.get_endpoint')
        with self.lock:
            self.cv.wait_for(lambda: bool(self.endpoints))
            return self.endpoints.pop(0)

    def add_endpoint(self, endpoint):
        with self.lock:
            self.endpoints.append(endpoint)
            self.cv.notify_all()

    def postgres_url(self, unix_socket_dir, port, db):
        url = 'postgresql://postgres@/' + db + '?'
        url += ('host=' + unix_socket_dir)
        url += ('&port=%d' % port)
        return url

    def setupPostgres(self):
        global pg_factory
        self.pg = pg_factory()
        unix_socket_dir = self.pg.base_dir + '/tmp'
        port = self.pg.dsn()['port']
        root_yaml['storage']['unix_socket_dir'] = unix_socket_dir
        root_yaml['storage']['port'] = port
        root_yaml['storage']['postgres_user'] = 'postgres'
        url = self.postgres_url(unix_socket_dir, port, 'postgres')
        logging.info('StorageTest setup_postgres %s', url)

        with psycopg.connect(url) as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                try:
                    cursor.execute('drop database storage_test;')
                except psycopg.errors.InvalidCatalogName:
                    pass
                cursor.execute('create database storage_test;')
                conn.commit()

        url = self.postgres_url(unix_socket_dir, port, 'storage_test')
        with psycopg.connect(url) as conn:
            with open('init_storage_postgres.sql', 'r') as f:
                with conn.cursor() as cursor:
                    cursor.execute(f.read())


    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s [%(thread)d] %(message)s')

        self.lock = Lock()
        self.cv = Condition(self.lock)

        self.setupPostgres()

        self.executor = Executor(inflight_limit=10, watchdog_timeout=300,
                                 debug_futures=True)

        # find a free port
        with socketserver.TCPServer(("localhost", 0), lambda x,y,z: None) as s:
            self.port = s.server_address[1]
        root_yaml['rest_listener']['addr'] = ('127.0.0.1', self.port)
        self.router_url = 'http://localhost:%d' % self.port
        self.endpoints = []
        self.config = Config()
        self.config.inject_yaml(root_yaml)
        self.config.inject_filter(
            'sync', lambda yaml, next: self.get_endpoint(), SyncFilter)
        self.service = Service(config=self.config,
                               executor = self.executor)
        self.executor.submit(lambda: self.service.main())

        self.assertTrue(self.service.wait_started(1))

        # probe for startup
        upstream = FakeSyncFilter()
        def exp(tx, tx_delta):
            upstream_delta = TransactionMetadata(
                mail_response = Response())
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        upstream.add_expectation(exp)
        self.add_endpoint(upstream)

        for i in range(1,3):
            logging.info('RouterServiceTest.setUp probe %d', i)
            rest_endpoint = RestEndpoint(
                static_base_url=self.router_url,
                http_host='submission',
                timeout_start=1, timeout_data=1)
            tx = TransactionMetadata(
                mail_from = Mailbox('probe-from%d' % i))
            t = self.start_tx_update(rest_endpoint, tx, tx.copy())
            #self.service._dequeue()
            self.join_tx_update(t)
            logging.info('RouterServiceTest.setUp %s', tx.mail_response)
            if tx.mail_response.ok():
                break
            time.sleep(0.1)
        else:
            self.fail('service not ready')

        self.assertFalse(self.endpoints)

        # gc the probe request so it doesn't cause confusion later
        self.assertTrue(self.service._gc(0))

    def tearDown(self):
        # TODO this should verify that there are no open tx attempts in storage
        # e.g. some exception path failed to tx_cursor.finalize_attempt()
        self.service.shutdown()
        self.executor.shutdown(timeout=5)

    def _update_tx(self, endpoint, tx, tx_delta, timeout=None):
        logging.debug('RouterServiceTest._update_tx')
        endpoint.on_update(tx, tx_delta, timeout)

    def start_tx_update(self, rest_endpoint, tx, tx_delta, timeout=5):
        return self.executor.submit(
            lambda: self._update_tx(rest_endpoint, tx, tx_delta, timeout))

    def join_tx_update(self, t, timeout=5):
        logging.debug('RouterServiceTest.join_tx_update')
        try:
            t.result(timeout)
        except Exception:
            logging.exception('RouterServiceTest.join_tx_update exception')
        logging.debug('RouterServiceTest.join_tx_update done')

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
        else:
            self.fail('failed to dequeue')

    # simplest possible case: native rest/inline body -> upstream success
    def test_rest_smoke(self):
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, http_host='submission',
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

        t = self.start_tx_update(rest_endpoint, tx, tx.copy())
#        self._dequeue()
        self.join_tx_update(t)

        tx_json = rest_endpoint.get_json()
        logging.debug('RouterServiceTest.test_rest_smoke create %s',
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
        })


    def test_reuse_body(self):
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, http_host='submission',
            timeout_start=5, timeout_data=5)
        body = 'hello, world!'
        body_utf8 = body.encode('utf-8')
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')],
            inline_body=body)

        upstream_endpoint = FakeSyncFilter()
        self.add_endpoint(upstream_endpoint)
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

        t = self.start_tx_update(rest_endpoint, tx, tx.copy())

        self._dequeue()
        self.join_tx_update(t)

        tx_json = rest_endpoint.get_json()
        logging.debug('RouterServiceTest.test_reuse_body create %s',
                      tx_json)
        tx_url = rest_endpoint.transaction_path


        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, http_host='submission',
            timeout_start=5, timeout_data=5)
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')],
            body=tx_url + '/body')

        upstream_endpoint = FakeSyncFilter()
        self.add_endpoint(upstream_endpoint)
        def exp2(tx, tx_delta):
            upstream_delta = TransactionMetadata(
                mail_response = Response(201),
                rcpt_response = [Response(202)],
            data_response = Response(203))
            self.assertEqual(tx.body_blob.read(0), body_utf8)
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        upstream_endpoint.add_expectation(exp2)

        t = self.start_tx_update(rest_endpoint, tx, tx.copy())

        self._dequeue()
        self.join_tx_update(t)
        tx_json = rest_endpoint.get_json()



    # xxx need non-exploder test w/filter api, problems in
    # post-exploder/upstream tx won't be reported out synchronously,
    # would potentially bounce
    # and/or get ahold of those tx IDs and verify the status directly


    def test_exploder_multi_rcpt(self):
        logging.info('testExploderMultiRcpt')
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, http_host='smtp-msa')

        logging.info('testExploderMultiRcpt start tx')
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            remote_host=HostPort('1.2.3.4', 12345))
        t = self.start_tx_update(rest_endpoint, tx, tx.copy())

        # exploder tx
        #self._dequeue()

        self.join_tx_update(t)

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
        t = self.start_tx_update(rest_endpoint, tx, tx_delta)

        #self._dequeue()

        self.join_tx_update(t)
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
        t = self.start_tx_update(rest_endpoint, tx, tx_delta)
        #self._dequeue()

        self.join_tx_update(t)
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
        self.assertEqual(tx.data_response.code, 205)
        self.assertEqual(tx.data_response.message,
                         'exploder same status: upstream data 0')


    def test_notification_retry_timeout(self):
        logging.info('test_notification')
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, http_host='smtp-msa')

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
        t = self.start_tx_update(rest_endpoint, tx, tx.copy(), 10)

        # exploder tx and upstream tx
        self._dequeue(2)

        self.join_tx_update(t, 10)
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

            exp_deq = 1

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
                exp_deq += 1

            self._dequeue(exp_deq)


    def test_notification_fast_perm(self):
        logging.info('test_notification_fast_perm')
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, http_host='smtp-msa')

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

        t = self.start_tx_update(rest_endpoint, tx, tx.copy(), 10)
        # exploder tx and 2 upstream tx
        logging.info('test_notification_fast_perm dequeue exp+2 upstream')
        self._dequeue(3)
        logging.info('test_notification_fast_perm dequeue exp+2 upstream done')

        self.join_tx_update(t, 10)
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
        def exp_dsn(tx, tx_delta):
            self.assertEqual(tx.mail_from.mailbox, '')
            self.assertEqual(tx.rcpt_to[0].mailbox, 'alice')
            dsn = tx.body_blob.read(0)
            logging.debug('test_notification %s', dsn)
            self.assertIn(b'subject: Delivery Status Notification', dsn)

            upstream_delta=TransactionMetadata(
                mail_response=Response(250),
                rcpt_response=[Response(250)],
                data_response=Response(250))
            tx.merge_from(upstream_delta)
            return upstream_delta
        dsn_endpoint.add_expectation(exp_dsn)
        self.add_endpoint(dsn_endpoint)
        # dsn output
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
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, http_host='submission')

        upstream_endpoint = FakeSyncFilter()
        self.add_endpoint(upstream_endpoint)

        logging.info('test_notification start tx')
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob1')],
            remote_host=HostPort('1.2.3.4', 12345))
        t = self.start_tx_update(rest_endpoint, tx, tx.copy(), 10)

        def exp_env(tx, tx_delta):
            updated_tx = tx.copy()
            updated_tx.mail_response = Response(201)
            updated_tx.rcpt_response.append(Response(202))
            upstream_delta = tx.delta(updated_tx)
            self.assertIsNotNone(tx.merge_from(upstream_delta))
            return upstream_delta

        upstream_endpoint.add_expectation(exp_env)

        self._dequeue()

        self.join_tx_update(t, 10)

        blob = InlineBlob("hello, world!")
        #rest_endpoint.blob_url = rest_endpoint.full_blob_url = None
        blob_resp = rest_endpoint._put_blob(
            blob, testonly_non_body_blob=True)
        self.assertEqual(blob_resp.code, 200)

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
                "content_uri": rest_endpoint.blob_path
            }]
        }

        def exp_body(tx, tx_delta):
            body = tx.body_blob.read(0)
            logging.debug(body)
            self.assertIn(b'subject: hello\r\n', body)
            updated_tx = tx.copy()
            updated_tx.data_response = Response(203)
            upstream_delta = tx.delta(updated_tx)
            self.assertIsNotNone(tx.merge_from(upstream_delta))
            return upstream_delta

        upstream_endpoint.add_expectation(exp_body)

        logging.debug('RouterServiceTest.test_message_builder tx before '
                      'patch message_builder spec %s',
                      rest_endpoint.get_json(timeout=1))
        rest_resp = rest_endpoint.client.post(
            rest_endpoint.transaction_url + '/message_builder',
            json=message_builder_spec,
            headers={'if-match': rest_endpoint.etag})
        self.assertEqual(rest_resp.status_code, 200)

        for i in range(0,3):
            json = rest_endpoint.get_json()
            tx = TransactionMetadata.from_json(json, WhichJson.REST_READ)
            if tx.data_response and tx.data_response.code == 203:
                break
            time.sleep(0.1)
        else:
            self.fail('data response')

        # send another tx with the same spec to exercise blob reuse
        logging.info('test_notification start tx #2')
        tx2 = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob2')],
            remote_host=HostPort('1.2.3.4', 12345),
            message_builder=message_builder_spec)

        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, http_host='submission')

        upstream_endpoint = FakeSyncFilter()
        self.add_endpoint(upstream_endpoint)

        def exp2(tx, tx_delta):
            self.assertEqual(tx.mail_from.mailbox, 'alice')
            self.assertEqual([m.mailbox for m in tx.rcpt_to], ['bob2'])
            body = tx.body_blob.read(0)
            logging.debug(body)
            self.assertIn(b'subject: hello\r\n', body)

            upstream_delta = TransactionMetadata(
                mail_response = Response(204),
                rcpt_response = [Response(205)],
                data_response = Response(206))

            self.assertIsNotNone(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream_endpoint.add_expectation(exp2)

        t = self.start_tx_update(rest_endpoint, tx2, tx2.copy(), 10)
        self.service._dequeue()
        self.join_tx_update(t, 10)



if __name__ == '__main__':
    unittest.removeHandler()
    unittest.main(catchbreak=False)
