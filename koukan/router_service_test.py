# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Any, List, Optional, Tuple
from threading import Lock, Condition
import logging
import logging.config
import unittest
import socketserver
import time
from functools import partial
from urllib.parse import urljoin
from datetime import timedelta
import copy

from werkzeug.datastructures import ContentRange
import werkzeug.http

from koukan.rest_schema import BlobUri, parse_blob_uri
import koukan.postgres_test_utils as postgres_test_utils
from koukan.router_service import Service
from koukan.rest_endpoint import RestEndpoint
from koukan.response import Response
from koukan.blob import Blob, CompositeBlob, InlineBlob
from koukan.fake_endpoints import FakeSyncFilter
from koukan.filter import (
    HostPort,
    Mailbox,
    SyncFilter,
    TransactionMetadata,
    WhichJson )
from koukan.executor import Executor

import koukan.sqlite_test_utils as sqlite_test_utils

from koukan.message_builder import MessageBuilderSpec

from koukan.storage_schema import BlobSpec

def setUpModule():
    postgres_test_utils.setUpModule()

def tearDownModule():
    postgres_test_utils.tearDownModule()


root_yaml_template = {
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
                    'bug_retry': 1,
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
                       'rcpt_timeout': 10,
                       'data_timeout': 10,
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
                     'domains': ['example.com'],
                     #endpoint: https://localhost:8001
                     'destination': {'options': { 'receive_parsing': {}}}
                     }
                },
                {'filter': 'router',
                 'policy': { 'name': 'address_list' }},
                {'filter': 'message_parser'},
                {'filter': 'sync'} ]
        },
        {
            'name': 'submission-sync-sor',
            'chain': [
                {'filter': 'add_route',
                 'output_chain': 'sor',
                 # store_and_forward
                },
                {'filter': 'sync'}
            ]
        },
        {
            'name': 'submission-sf-sor',
            'chain': [
                {'filter': 'add_route',
                 'output_chain': 'sor',
                 'store_and_forward': True
                },
                {'filter': 'sync'}
            ]
        },
        {
            'name': 'sor',
            'chain': [
                {'filter': 'sync'}
            ]
        }
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
    },
    'modules': {
        'sync_filter': {
            'hello': 'koukan.hello_filter'
        }
    }
}

class RouterServiceTest(unittest.TestCase):
    lock : Lock
    cv : Condition
    endpoints : List[FakeSyncFilter]
    use_postgres = True

    def __init__(self, *args, **kwargs):
        super(RouterServiceTest, self).__init__(*args, **kwargs)

        self.endpoints = []

    def get_endpoint(self):
        logging.debug('RouterServiceTest.get_endpoint')
        with self.lock:
            self.cv.wait_for(lambda: bool(self.endpoints))
            return self.endpoints.pop(0)

    def add_endpoint(self, endpoint):
        with self.lock:
            self.endpoints.append(endpoint)
            self.cv.notify_all()

    def _setup_router(self):
        self.root_yaml = root_yaml = copy.deepcopy(root_yaml_template)
        root_yaml['storage']['url'] = self.storage_url

        # find a free port
        with socketserver.TCPServer(("localhost", 0), lambda x,y,z: None) as s:
            self.port = s.server_address[1]
        root_yaml['rest_listener']['addr'] = ('127.0.0.1', self.port)
        root_yaml['rest_listener']['session_uri'] = 'http://localhost:%d' % self.port
        router_url = 'http://localhost:%d' % self.port
        service = Service(root_yaml=root_yaml)

        service.start_main()
        self.assertTrue(service.wait_started(5))
        # XXX
        service.filter_chain_factory.inject_filter(
            'sync', lambda yaml, next: self.get_endpoint())

        return router_url, service

    def setUp(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
            '%(message)s')
        logging.config.dictConfig({'version': 1,
             'loggers': {
                 'hpack': {
                     'level': 'INFO'
                     }
                 }
             })
        self.lock = Lock()
        self.cv = Condition(self.lock)

        if self.use_postgres:
            self.pg, self.storage_url = postgres_test_utils.setup_postgres()
        else:
            self.dir, self.storage_url = sqlite_test_utils.create_temp_sqlite_for_test()

        # find a free port
        self.router_url, self.service = self._setup_router()

        # probe for startup
        def exp(tx, tx_delta):
            upstream_delta = TransactionMetadata(
                mail_response = Response(201, 'probe mail ok'),
                rcpt_response = [Response(202)])
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        def exp_cancel(tx, tx_delta):
            upstream_delta = TransactionMetadata()
            tx.fill_inflight_responses(Response(450), upstream_delta)
            tx.merge_from(upstream_delta)
            return upstream_delta

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
                mail_from = Mailbox('probe-from%d' % i),
                rcpt_to = [Mailbox('probe-to%d' % i)])
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
        self.assertTrue(self.service._gc(timedelta(seconds=0)))

    def tearDown(self):
        # TODO this should verify that there are no open tx attempts in storage
        # e.g. some exception path failed to tx_cursor.finalize_attempt()
        self.assertTrue(self.service.shutdown())

    def dump_db(self):
        with self.service.storage.begin_transaction() as db_tx:
            for l in db_tx.connection.iterdump():
                logging.debug(l)

    def assertRcptCodesEqual(self, expected_codes : List[int],
                             responses : List[Optional[Response]]):
        self.assertEqual(expected_codes,
                         [r.code if r else None for r in responses])

    # def assertRcptJsonCodesEqual(self, resp_json, expected_codes):
    #     self.assertRcptCodesEqual(
    #         expected_codes,
    #         [Response.from_json(r) for r in resp_json.get('rcpt_response', [])])

    def _dequeue(self, n=1):
        deq = 0
        for i in range(0,10):
            if self.service._dequeue():
                deq += 1
                logging.debug('RouterServiceTest._dequeue %d', deq)
            if deq == n:
                return
            time.sleep(0.3)
        else:
            self.fail('failed to dequeue')

    # simplest possible case: native rest/inline body -> upstream success
    def test_rest_smoke(self):
        logging.debug('RouterServiceTest.test_rest_smoke')
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, static_http_host='submission',
            timeout_start=5, timeout_data=5)
        body = b'hello, world!'
        tx = TransactionMetadata(
            #retry={},
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob@example.com')],
            body=InlineBlob(body, last=True))

        def exp(tx, tx_delta):
            logging.debug(tx)
            # xxx verify mail/rcpt
            upstream_delta=TransactionMetadata()
            if tx_delta.mail_from:
                upstream_delta.mail_response=Response(201)
            if tx_delta.rcpt_to:
                upstream_delta.rcpt_response=[Response(202)]
            if tx.body and tx.body.finalized():
                self.assertIn(body, tx.body.pread(0))
                upstream_delta.data_response=Response(203)

            self.assertTrue(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream_endpoint = FakeSyncFilter()
        upstream_endpoint.add_expectation(exp)
        upstream_endpoint.add_expectation(exp)
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
            logging.debug(tx_json)
        else:
            logging.debug(self.service.storage.debug_dump())
            self.fail('didn\'t get expected transaction %s' % tx_json)

        self.root_yaml['storage']['gc_interval'] = 1
        self.service.daemon_executor.submit(
            partial(self.service.gc, self.service.daemon_executor))

        for i in range(0,5):
            if rest_endpoint.get_json(timeout=2) is None:
                break
            time.sleep(1)
        else:
            self.fail('expected 404 after gc')

    # repro/regtest for spurious wakeup bug in VersionCache
    def test_rest_hanging_get(self):
        logging.debug('RouterServiceTest.test_rest_hanging_get')
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, static_http_host='submission',
            timeout_start=5, timeout_data=5)
        body = 'hello, world!'
        tx = TransactionMetadata(
            #retry={},
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob@example.com')])
            #inline_body=body)

        def exp(tx, tx_delta):
            time.sleep(1)
            upstream_delta=TransactionMetadata(
                mail_response=Response(201),
                rcpt_response=[Response(202)],
                data_response=Response(203))
            self.assertTrue(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream_endpoint = FakeSyncFilter()
        upstream_endpoint.add_expectation(exp)
        self.add_endpoint(upstream_endpoint)

        post_resp = rest_endpoint.client.post(
            rest_endpoint.base_url + '/transactions',
            json = tx.to_json(WhichJson.REST_CREATE),
            headers={'host': 'submission'})
        self.assertEqual(201, post_resp.status_code)
        tx_url = rest_endpoint._maybe_qualify_url(post_resp.headers['location'])[0]
        logging.debug(tx_url)

        post_body_resp = rest_endpoint.client.put(
            tx_url + '/body', data=body)
        self.assertEqual(200, post_body_resp.status_code)

        # GET without if-none-match does a point read
        logging.debug('get tx')
        get_resp = rest_endpoint.client.get(tx_url)
        self.assertEqual(200, get_resp.status_code)
        self.assertNotEqual(get_resp.headers['etag'], post_resp.headers['etag'])
        logging.debug(get_resp.json())

        # GET again with previous etag, should wait for change
        logging.debug('get tx 2')
        get_resp2 = rest_endpoint.client.get(
            tx_url,
            headers={'if-none-match': get_resp.headers['etag'],
                     'request-timeout': '5'})
        logging.debug(get_resp2)
        logging.debug(get_resp2.json())
        self.assertEqual(200, get_resp.status_code)
        self.assertNotEqual(get_resp2.headers['etag'], get_resp.headers['etag'])
        tx_json = get_resp2.json()
        # depending on timing, we may get the tx before or after the
        # OH completion that sets final_attempt_reason but we should
        # get all the responses
        self.assertEqual(
            tx_json['mail_response'], {'code': 201, 'message': 'ok'})
        self.assertEqual(
            tx_json['rcpt_response'], [{'code': 202, 'message': 'ok'}])
        self.assertEqual(
            tx_json['data_response'], {'code': 203, 'message': 'ok'})


    def test_rest_body(self):
        logging.debug('RouterServiceTest.test_rest_body')
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, static_http_host='submission',
            timeout_start=5, timeout_data=5)
        body = b'hello, world!'
        tx = TransactionMetadata(
            #retry={},
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob@example.com')],
            body=InlineBlob(body, last=True))

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
            'mail_from': {},
            'rcpt_to': [{}],
            'body': {},
            'mail_response': {'code': 201, 'message': 'ok'},
            'rcpt_response': [{'code': 202, 'message': 'ok'}],
            'data_response': {'code': 203, 'message': 'ok'},
            'final_attempt_reason': 'upstream response success'
        })


    def test_rest_body_http_retry(self):
        logging.debug('RouterServiceTest.test_rest_body_chunked')
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, static_http_host='submission',
            timeout_start=5, timeout_data=5)
        body = b'hello, world!'
        tx = TransactionMetadata(
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob@example.com')])

        def exp(tx, tx_delta):
            upstream_delta=TransactionMetadata(
                mail_response=Response(201),
                rcpt_response=[Response(202)])
            self.assertTrue(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream_endpoint = FakeSyncFilter()
        upstream_endpoint.add_expectation(exp)
        self.add_endpoint(upstream_endpoint)

        rest_endpoint.on_update(tx, tx.copy())
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])

        def exp(tx, tx_delta):
            upstream_delta=TransactionMetadata(
                data_response=Response(203))
            self.assertTrue(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream_endpoint.add_expectation(exp)


        body = 200000 * b'hello, '
        def data():
            yield body
            raise EOFError()  # simulate request timeout

        try:
            resp = rest_endpoint.client.put(
                rest_endpoint._maybe_qualify_url(
                    rest_endpoint.transaction_path + '/body')[0],
                content = data())
        except:
            pass

        # xxx some race
        time.sleep(1)

        logging.debug('retry')

        body += b'world!'
        resp = rest_endpoint.client.put(
            rest_endpoint._maybe_qualify_url(
                rest_endpoint.transaction_path + '/body')[0],
            content = body)
        logging.debug(resp)
        logging.debug(resp.headers)
        self.assertEqual(416, resp.status_code)

        # xxx some race
        time.sleep(1)

        range = werkzeug.http.parse_content_range_header(
            resp.headers.get('content-range'))
        range.start = range.stop
        range.stop = len(body)
        range.length = len(body)
        resp = rest_endpoint.client.put(
            rest_endpoint._maybe_qualify_url(
                rest_endpoint.transaction_path + '/body')[0],
            headers={'content-range': range.to_header()},
            content = body[range.start:])
        logging.debug(resp)
        logging.debug(resp.headers)
        self.assertEqual(200, resp.status_code)

        # xxx get tx

    def test_rest_body_chunked(self):
        logging.debug('RouterServiceTest.test_rest_body_chunked')
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, static_http_host='submission',
            timeout_start=5, timeout_data=5)
        body = b'hello, world!'
        tx = TransactionMetadata(
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob@example.com')])

        def exp(tx, tx_delta):
            upstream_delta=TransactionMetadata(
                mail_response=Response(201),
                rcpt_response=[Response(202)])
            self.assertTrue(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream_endpoint = FakeSyncFilter()
        upstream_endpoint.add_expectation(exp)
        self.add_endpoint(upstream_endpoint)

        rest_endpoint.on_update(tx, tx.copy())
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])

        def exp(tx, tx_delta):
            upstream_delta=TransactionMetadata(
                data_response=Response(203))
            self.assertTrue(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream_endpoint.add_expectation(exp)

        def data():
            yield b'hello, '
            yield b'world!'

        resp = rest_endpoint.client.put(
            rest_endpoint._maybe_qualify_url(
                rest_endpoint.transaction_path + '/body')[0],
            content = data())
        logging.debug(resp)

        for i in range(0,3):
            tx_json = rest_endpoint.get_json()
            logging.debug('RouterServiceTest.test_rest_body create %s',
                          tx_json)

            if 'attempt_count' in tx_json:
                del tx_json['attempt_count']
            if tx_json == {
                'mail_from': {},
                'rcpt_to': [{}],
                'body': {},
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'}],
                'data_response': {'code': 203, 'message': 'ok'},
                'final_attempt_reason': 'upstream response success' }:
                break
            time.sleep(1)
        else:
            self.fail('expected tx')

    # upstream temp during submission before input done
    # downstream should complete uploading the body -> input_done -> reload
    def test_submission_retry(self):
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, static_http_host='submission',
            timeout_start=5, timeout_data=5)
        body = b'hello, world!'
        tx = TransactionMetadata(
            retry={},
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob@example.com')])

        def exp(tx, tx_delta):
            upstream_delta=TransactionMetadata(
                mail_response=Response(201),
                rcpt_response=[Response(402)])
            self.assertTrue(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream_endpoint = FakeSyncFilter()
        upstream_endpoint.add_expectation(exp)
        self.add_endpoint(upstream_endpoint)

        rest_endpoint.on_update(tx, tx.copy())
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [402])

        def exp_body(tx, tx_delta):
            self.assertIsNotNone(tx.body)
            upstream_delta = TransactionMetadata()
            tx.fill_inflight_responses(Response(550), upstream_delta)
            tx.merge_from(upstream_delta)
            return upstream_delta

        upstream_endpoint.add_expectation(exp_body)

        body_url = rest_endpoint._maybe_qualify_url(
            rest_endpoint.transaction_path + '/body')[0]

        data = b'hello, world!\r\n'  # 15B
        chunk1 = 7

        resp = rest_endpoint.client.put(
            body_url,
            headers={'content-range': 'bytes 0-%d/%d' % (chunk1-1, len(data))},
            content = data[0:chunk1])
        logging.debug('%d %s', resp.status_code, resp.content)
        self.assertEqual(200, resp.status_code)

        time.sleep(1)

        resp = rest_endpoint.client.put(
            body_url,
            headers={'content-range': 'bytes %d-%d/%d' % (
                chunk1, len(data) - 1, len(data))},
            content = data[chunk1:])
        logging.debug('%d %s', resp.status_code, resp.content)
        logging.debug(self.service.storage.debug_dump())
        self.assertEqual(200, resp.status_code)


        def exp_body(tx, tx_delta):
            self.assertEqual(data, tx.body.pread(0))
            upstream_delta=TransactionMetadata(
                mail_response=Response(201),
                rcpt_response=[Response(202)],
                data_response=Response(203))
            self.assertTrue(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream_endpoint = FakeSyncFilter()
        upstream_endpoint.add_expectation(exp_body)
        self.add_endpoint(upstream_endpoint)

        self._dequeue()

        logging.debug(self.service.storage.debug_dump())
        tx_json = rest_endpoint.get_json()
        tx = TransactionMetadata.from_json(tx_json, WhichJson.REST_READ)
        self.assertEqual(2, tx.attempt_count)
        self.assertEqual(201, tx.mail_response.code)
        self.assertEqual([202], [r.code for r in tx.rcpt_response])
        self.assertEqual(203, tx.data_response.code)
        self.assertEqual('upstream response success', tx.final_attempt_reason)

    def test_reuse_body(self):
        logging.debug('RouterServiceTest.test_reuse_body')
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, static_http_host='submission',
            timeout_start=5, timeout_data=5)
        body = 'hello, world!'
        body_utf8 = body.encode('utf-8')
        tx = TransactionMetadata(
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob@example.com')],
            body=InlineBlob(body_utf8, last=True))

        upstream_endpoint = FakeSyncFilter()
        def exp_env(tx, tx_delta):
            upstream_delta = TransactionMetadata(
                mail_response = Response(201),
                rcpt_response = [Response(202)])
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        def exp_body(tx, tx_delta):
            if tx.body is None or not tx.body.finalized():
                return TransactionMetadata()
            self.assertEqual(tx.body.pread(0), body_utf8)
            upstream_delta = TransactionMetadata(
                data_response = Response(203))
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        upstream_endpoint.add_expectation(exp_env)
        upstream_endpoint.add_expectation(exp_body)
        upstream_endpoint.add_expectation(exp_body)
        self.add_endpoint(upstream_endpoint)

        rest_endpoint.on_update(tx, tx.copy())
        self.assertEqual(201, tx.mail_response.code)
        self.assertEqual([202], [r.code for r in tx.rcpt_response])
        self.assertEqual(203, tx.data_response.code)

        blob_uri = parse_blob_uri(rest_endpoint.transaction_path + '/body')
        logging.debug(blob_uri)

        tx_json = rest_endpoint.get_json()
        logging.debug('RouterServiceTest.test_reuse_body create %s',
                      tx_json)

        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, static_http_host='submission',
            timeout_start=5, timeout_data=5)
        tx = TransactionMetadata(
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob2@example.com')],
            body=BlobSpec(reuse_uri=blob_uri))

        upstream_endpoint = FakeSyncFilter()
        def exp2(tx, tx_delta):
            logging.debug(tx)
            upstream_delta = TransactionMetadata(
                mail_response = Response(201),
                rcpt_response = [Response(202)])

            if tx.body is not None:
                self.assertTrue(isinstance(tx.body, Blob))
                if tx.body.finalized():
                    self.assertEqual(tx.body.pread(0), body_utf8)
                    upstream_delta.data_response = Response(203)

            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        upstream_endpoint.add_expectation(exp2)
        upstream_endpoint.add_expectation(exp2)
        self.add_endpoint(upstream_endpoint)

        rest_endpoint.on_update(tx, tx.copy())
        tx_json = rest_endpoint.get_json()
        logging.debug(tx_json)
        self.assertEqual(
            {'mail_from': {},
             'mail_response': {'code': 201, 'message': 'ok'},
             'rcpt_to': [{}],
             'rcpt_response': [{'code': 202, 'message': 'ok'}],
             'body': {},
             'data_response': {'code': 203, 'message': 'ok'},
             'attempt_count': 1,
             'final_attempt_reason': 'upstream response success'},
            tx_json)

    # xxx need non-exploder test w/filter api, problems in
    # post-exploder/upstream tx won't be reported out synchronously,
    # would potentially bounce
    # and/or get ahold of those tx IDs and verify the status directly

    # TODO add exploder s&f test

    def test_exploder_multi_rcpt(self):
        logging.info('RouterServiceTest.test_exploder_multi_rcpt')
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, static_http_host='smtp-msa',
            timeout_start=5, timeout_data=5)

        logging.info('testExploderMultiRcpt start tx')
        tx = TransactionMetadata(
            mail_from=Mailbox('alice@example.com'),
            remote_host=HostPort('1.2.3.4', 12345))
        rest_endpoint.on_update(tx, tx.copy())

        # no rcpt -> buffered
        self.assertEqual(tx.mail_response.code, 250)
        self.assertIn('exploder noop', tx.mail_response.message)

        # upstream tx #1
        upstream_endpoint = FakeSyncFilter()
        self.add_endpoint(upstream_endpoint)
        def exp_rcpt1(tx, tx_delta):
            logging.debug(tx)
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
        updated_tx.rcpt_to.append(Mailbox('bob@example.com'))
        tx_delta = tx.delta(updated_tx)
        tx = updated_tx
        rest_endpoint.on_update(tx, tx_delta)
        self.assertRcptCodesEqual([202], tx.rcpt_response)
        self.assertEqual(tx.rcpt_response[0].message, 'upstream rcpt 1')

        # upstream tx #2
        upstream_endpoint2 = FakeSyncFilter()
        self.add_endpoint(upstream_endpoint2)

        def exp_rcpt2(tx, tx_delta):
            logging.debug(tx)
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
        updated_tx.rcpt_to.append(Mailbox('bob2@example.com'))
        tx_delta = tx.delta(updated_tx)
        tx = updated_tx
        rest_endpoint.on_update(tx, tx_delta)

        self.assertRcptCodesEqual([202, 204], tx.rcpt_response)
        self.assertEqual('upstream rcpt 2', tx.rcpt_response[1].message)
        logging.debug('env done')

        def exp_body(i, tx, tx_delta):
            logging.debug('%d %s', i, tx)
            self.assertEqual(tx.body.pread(0),
                             b'Hello, World!')
            updated_tx = tx.copy()
            # repro temp err here
            updated_tx.data_response = Response(205 + i, 'upstream data %d' % i)
            upstream_delta = tx.delta(updated_tx)
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta

        for i,upstream in enumerate([upstream_endpoint, upstream_endpoint2]):
            upstream.add_expectation(partial(exp_body, i))

        logging.info('testExploderMultiRcpt patch body')

        tx_delta = TransactionMetadata(
            body=InlineBlob(b'Hello, World!', last=True))
        self.assertIsNotNone(tx.merge_from(tx_delta))
        rest_endpoint.on_update(tx, tx_delta)
        logging.debug('test_exploder_multi_rcpt %s', tx)
        self.assertEqual(205, tx.data_response.code)
        self.assertEqual('upstream data 0 (Exploder same response)',
                         tx.data_response.message)


    def test_notification_retry_timeout(self):
        logging.info('RouterServiceTest.test_notification_retry_timeout')
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, static_http_host='smtp-msa')

        def exp(tx, tx_delta):
            self.assertEqual(tx.mail_from.mailbox, 'alice@example.com')
            self.assertEqual([m.mailbox for m in tx.rcpt_to],
                             ['bob@example.com'])
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
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob@example.com')],
            body=InlineBlob(b'Hello, World!', last=True),
            remote_host=HostPort('1.2.3.4', 12345))
        rest_endpoint.on_update(tx, tx.copy(), 10)
        logging.debug('RouterServiceTest.test_notification after update %s',
                      tx)

        self.assertEqual(250, tx.mail_response.code, 201)
        self.assertEqual('MAIL ok (exploder noop)', tx.mail_response.message)
        self.assertRcptCodesEqual([250], tx.rcpt_response)
        self.assertIn('RCPT ok (AsyncFilterWrapper store&forward)',
                      tx.rcpt_response[0].message)

        for i in range(0,2):
            logging.debug('test_notification upstream tx %d', i)
            upstream_endpoint = FakeSyncFilter()
            def exp(tx, tx_delta):
                self.assertEqual(tx.mail_from.mailbox, 'alice@example.com')
                self.assertEqual([m.mailbox for m in tx.rcpt_to],
                                 ['bob@example.com'])
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
                    self.assertEqual([m.mailbox for m in tx.rcpt_to],
                                     ['alice@example.com'])
                    dsn = tx.body.pread(0)
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
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob1@example.com'),
                     Mailbox('bob2@example.com')],
            body=InlineBlob(b'Hello, World!', last=True),
            remote_host=HostPort('1.2.3.4', 12345))

        def exp_rcpt(tx, tx_delta):
            logging.debug(tx)
            rcpt = tx_delta.rcpt_to[0].mailbox
            self.assertIn(rcpt, ['bob1@example.com', 'bob2@example.com'])
            # XXX if tx.body and tx.body.finalized()
            data_resp = 550 if rcpt == 'bob2@example.com' else 250
            upstream_delta=TransactionMetadata(
                mail_response=Response(201),
                rcpt_response=[Response(202)],
                data_response=Response(data_resp))
            tx.merge_from(upstream_delta)
            return upstream_delta

        # output for each of rcpt1, rcpt2
        for j in range(0,2):
            upstream = FakeSyncFilter()
            logging.debug(id(upstream))
            upstream.add_expectation(exp_rcpt)
            self.add_endpoint(upstream)

        rest_endpoint.on_update(tx, tx.copy(), 10)
        logging.debug('RouterServiceTest.test_notification after update %s',
                      tx)
        self.assertEqual(250, tx.mail_response.code)
        self.assertEqual('MAIL ok (exploder noop)', tx.mail_response.message)
        self.assertEqual(2, len(tx.rcpt_response))
        for r in tx.rcpt_response:
            self.assertEqual(202, r.code)
            self.assertEqual('ok', r.message)
        self.assertEqual(250, tx.data_response.code)
        self.assertIn('DATA ok (Exploder store&forward)',
                      tx.data_response.message)

        # no_final_notification OutputHandler handler for the rcpt that failed
        for i in range(0,1):
            unused_upstream_endpoint = FakeSyncFilter()
            logging.debug(id(unused_upstream_endpoint))
            self.add_endpoint(unused_upstream_endpoint)

        dsn_endpoint = FakeSyncFilter()
        logging.debug(id(dsn_endpoint))
        def exp_dsn(tx, tx_delta):
            logging.error(tx)
            self.assertEqual('', tx.mail_from.mailbox)
            self.assertEqual('alice@example.com', tx.rcpt_to[0].mailbox)
            upstream_delta=TransactionMetadata(
                mail_response=Response(250),
                rcpt_response=[Response(250)])
            tx.merge_from(upstream_delta)

            if tx.body and tx.body.finalized():
                dsn = tx.body.pread(0)
                logging.debug('test_notification %s', dsn)
                self.assertIn(b'subject: Delivery Status Notification', dsn)
                upstream_delta.data_response=Response(250)
            tx.merge_from(upstream_delta)
            return upstream_delta
        dsn_endpoint.add_expectation(exp_dsn)
        self.add_endpoint(dsn_endpoint)
        # no_final_notification(bob2) + dsn output
        self._dequeue(2)

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

        logging.info('test_message_builder start 1st tx')
        tx = TransactionMetadata(
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob1@example.com')],
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
                "content": {"create_id": "my_plain_body"}
            }]
        }
        b = b"hello, world!"
        blob = InlineBlob(b, rest_id='my_plain_body', last=True)
        tx.body = MessageBuilderSpec(message_builder_spec, blobs=[blob])
        tx.body.check_ids()
        def exp_env(tx, tx_delta):
            logging.debug('test_message_builder.exp_env %s', tx)
            self.assertEqual("alice@example.com", tx.mail_from.mailbox)
            self.assertEqual(["bob1@example.com"],
                              [m.mailbox for m in tx.rcpt_to])
            self.assertIsNone(tx.body)
            upstream_delta = TransactionMetadata(
                mail_response = Response(201),
                rcpt_response=[Response(202)])
            self.assertIsNotNone(tx.merge_from(upstream_delta))
            return upstream_delta

        upstream_endpoint.add_expectation(exp_env)

        def exp_body(tx, tx_delta):
            logging.debug('test_message_builder.exp_body %s', tx)
            if tx.body is None:
                return TransactionMetadata()
            body = tx.body.pread(0)
            logging.debug(body)
            self.assertIn(b'subject: hello\r\n', body)
            #self.assertIn(b, body)  # base64
            upstream_delta = TransactionMetadata(
                data_response = Response(203))
            self.assertIsNotNone(tx.merge_from(upstream_delta))
            return upstream_delta

        upstream_endpoint.add_expectation(exp_body)
        upstream_endpoint.add_expectation(exp_body)

        rest_endpoint.on_update(tx, tx.copy(), 1)
        logging.debug(tx)
        self.assertEqual(201, tx.mail_response.code)
        self.assertEqual([202], [r.code for r in tx.rcpt_response])
        self.assertEqual(203, tx.data_response.code)


        logging.debug('start second tx')

        message_builder_spec['text_body'][0]['content'] = {
            'reuse_uri': rest_endpoint.transaction_path + '/blob/my_plain_body'
        }

        # send another tx with the same spec to exercise blob reuse
        logging.info('RouterServiceTest.test_message_builder start tx #2')
        spec = MessageBuilderSpec(message_builder_spec)
        spec.check_ids()
        tx2 = TransactionMetadata(
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob2@example.com')],
            remote_host=HostPort('1.2.3.4', 12345),
            body=spec)

        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, static_http_host='submission')

        upstream_endpoint = FakeSyncFilter()
        self.add_endpoint(upstream_endpoint)

        def exp2(tx, tx_delta):
            logging.debug('test_message_builder.exp2 %s', tx)
            self.assertEqual('alice@example.com', tx.mail_from.mailbox)
            self.assertEqual(['bob2@example.com'],
                             [m.mailbox for m in tx.rcpt_to])
            upstream_delta = TransactionMetadata(
                mail_response = Response(204),
                rcpt_response = [Response(205)])
            self.assertTrue(tx_delta.body.finalized())
            body = tx.body.pread(0)
            logging.debug(body)
            self.assertIn(b'subject: hello\r\n', body)
            upstream_delta.data_response = Response(206)
            tx.merge_from(upstream_delta)
            return upstream_delta

        upstream_endpoint.add_expectation(exp2)

        rest_endpoint.on_update(tx2, tx2.copy(), 10)
        logging.debug(tx2)
        self.assertEqual(204, tx2.mail_response.code)
        self.assertEqual([205], [r.code for r in tx2.rcpt_response])
        self.assertEqual(206, tx2.data_response.code)


    def test_receive_parsing(self):
        logging.info('RouterServiceTest.test_receive_parsing')
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, static_http_host='smtp-in',
            timeout_start=5, timeout_data=5)
        with open('testdata/multipart.msg', 'rb') as f:
            body = f.read()
        tx = TransactionMetadata(
            #retry={},
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob@example.com')],
            body=InlineBlob(body, last=True))

        def exp(tx, tx_delta):
            logging.debug('test_receive_parsing %s', tx)
            upstream_delta=TransactionMetadata()
            if tx_delta.mail_from:
                upstream_delta.mail_response=Response(201)
            if tx_delta.rcpt_to:
                upstream_delta.rcpt_response=[Response(202)]
            if tx_delta.body:
                self.assertTrue(isinstance(tx_delta.body, MessageBuilderSpec))
                self.assertIn(['subject', 'hello'],
                              tx_delta.body.json['parts']['headers'])
                # NOTE: I can't figure out how to get email.parser to
                # leave the line endings alone
                self.assertEqual(2, len(tx_delta.body.blobs))
                self.assertEqual(tx_delta.body.blobs[0].pread(0),
                                 b'yolocat')
                self.assertEqual(tx_delta.body.blobs[1].pread(0),
                                 b'yolocat2')
                self.assertIsNotNone(tx_delta.body.json)
                upstream_delta.data_response=Response(203)
            self.assertTrue(tx.merge_from(upstream_delta))
            logging.debug(upstream_delta)
            return upstream_delta
        upstream_endpoint = FakeSyncFilter()
        upstream_endpoint.add_expectation(exp)
        upstream_endpoint.add_expectation(exp)
        self.add_endpoint(upstream_endpoint)

        rest_endpoint.on_update(tx, tx.copy())
        self.assertEqual(tx.mail_response.code, 250)
        self.assertRcptCodesEqual([202], tx.rcpt_response)
        self.assertEqual(tx.data_response.code, 203)


    def test_multi_node(self):
        url2, service2 = self._setup_router()

        # start a tx on self.service
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, static_http_host='smtp-msa',
            timeout_start=5, timeout_data=5)
        body = b'hello, world!'
        tx = TransactionMetadata(mail_from=Mailbox('alice@example.com'))

        upstream_endpoint = FakeSyncFilter()
        self.add_endpoint(upstream_endpoint)

        upstream_delta = rest_endpoint.on_update(tx, tx.copy(), timeout=1)
        logging.debug(rest_endpoint.transaction_path)

        # GET the tx from service2, point read should be serivced directly
        transaction_url=urljoin(url2, rest_endpoint.transaction_path)
        logging.debug(transaction_url)
        endpoint2 = RestEndpoint(
            transaction_url=transaction_url,
            static_http_host='submission',
            timeout_start=5, timeout_data=5)
        endpoint2.base_url = url2
        endpoint2.transaction_path = rest_endpoint.transaction_path
        tx_json = endpoint2.get_json()

        # hanging GET should be redirected to self.service
        tx_json = endpoint2._get_json(timeout = 2)
        logging.debug(tx_json)

        def exp_rcpt(tx, tx_delta):
            time.sleep(1)
            upstream_delta=TransactionMetadata(
                mail_response=Response(201),
                rcpt_response=[Response(202)])
            self.assertTrue(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream_endpoint.add_expectation(exp_rcpt)

        # PATCH the tx via service2, it does the update locally and
        # sends ping to self.service
        tx_delta = TransactionMetadata(rcpt_to=[Mailbox('bob@example.com')])
        tx.merge_from(tx_delta)
        endpoint2.on_update(tx, tx_delta, timeout=2)

        # send the body to service2
        def exp_body(tx, tx_delta):
            time.sleep(1)
            upstream_delta=TransactionMetadata(data_response=Response(203))
            self.assertTrue(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream_endpoint.add_expectation(exp_body)

        tx_delta = TransactionMetadata(
            body=InlineBlob(body, last=True))
        tx.merge_from(tx_delta)
        upstream_delta = endpoint2.on_update(tx, tx_delta)
        logging.debug('%s %s', tx, upstream_delta)

        tx_json = endpoint2.get_json()
        self.assertIn(('data_response', {'code': 203, 'message': 'ok'}),
                      tx_json.items())


        self.assertTrue(service2.shutdown())

    def test_add_route_sync_success(self):
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url,
            static_http_host='submission-sync-sor',
            timeout_start=5, timeout_data=5)
        body = b'hello, world!'
        tx = TransactionMetadata(
            #retry={},
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob@example.com')],
            body=InlineBlob(body, last=True))

        def exp(tx, tx_delta):
            logging.debug(tx)
            #self.assertEqual('sor', tx.host)
            upstream_delta=TransactionMetadata(
                mail_response=Response(201),
                rcpt_response=[Response(203)],
                data_response=Response(205))
            self.assertTrue(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream_endpoint = FakeSyncFilter()
        upstream_endpoint.add_expectation(exp)
        self.add_endpoint(upstream_endpoint)

        def exp_sor(tx, tx_delta):
            logging.debug(tx)
            #self.assertEqual('submission-sor', tx.host)
            upstream_delta=TransactionMetadata(
                mail_response=Response(202),
                rcpt_response=[Response(204)],
                data_response=Response(206))
            self.assertTrue(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream_endpoint = FakeSyncFilter()
        upstream_endpoint.add_expectation(exp_sor)
        self.add_endpoint(upstream_endpoint)

        rest_endpoint.on_update(tx, tx.copy())
        self.assertEqual(tx.mail_response.code, 201)
        self.assertRcptCodesEqual([203], tx.rcpt_response)
        self.assertEqual(tx.data_response.code, 205)


    def test_add_route_sf_success(self):
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url,
            static_http_host='submission-sf-sor',
            timeout_start=5, timeout_data=5)
        body = b'hello, world!'
        tx = TransactionMetadata(
            #retry={},
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob@example.com')],
            body=InlineBlob(body, last=True))

        def exp_upstream(tx, tx_delta):
            logging.debug(tx)
            self.assertEqual('submission-sf-sor', tx.host)
            upstream_delta=TransactionMetadata(
                mail_response=Response(201),
                rcpt_response=[Response(203)],
                data_response=Response(205))
            self.assertTrue(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream_endpoint = FakeSyncFilter()
        upstream_endpoint.add_expectation(exp_upstream)
        self.add_endpoint(upstream_endpoint)

        def exp_add_route(tx, tx_delta):
            logging.debug(tx)
            self.assertEqual('sor', tx.host)
            upstream_delta=TransactionMetadata(
                mail_response=Response(202),
                rcpt_response=[Response(204)],
                data_response=Response(206))
            self.assertTrue(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream_endpoint = FakeSyncFilter()
        upstream_endpoint.add_expectation(exp_add_route)
        self.add_endpoint(upstream_endpoint)

        rest_endpoint.on_update(tx, tx.copy())
        self.assertEqual(tx.mail_response.code, 201)
        self.assertRcptCodesEqual([203], tx.rcpt_response)
        self.assertEqual(tx.data_response.code, 205)


    def test_output_handler_exception(self):
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url,
            static_http_host='submission',
            timeout_start=5, timeout_data=5)
        body = b'hello, world!'
        tx = TransactionMetadata(
            retry={},
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob@example.com')],
            body=InlineBlob(body, last=True))

        def exp(tx, tx_delta):
            raise ValueError('bad')
        upstream_endpoint = FakeSyncFilter()
        upstream_endpoint.add_expectation(exp)
        self.add_endpoint(upstream_endpoint)
        rest_endpoint.on_update(tx, tx.copy())

        def check_response():
            for i in range(0,5):
                tx_json = rest_endpoint.get_json(timeout=2)
                logging.debug(
                    'RouterServiceTest.test_output_handler_exception %s',
                    tx_json)
                tx = TransactionMetadata.from_json(tx_json, WhichJson.REST_READ)
                if (tx.mail_response is None or not tx.rcpt_response):
                    # or tx.data_response is None):
                    # some ordering changed in this test,
                    # OH runs upstream with envelope before swf writes body
                    time.sleep(1)
                    continue
                self.assertEqual(tx.mail_response.code, 450)
                self.assertRcptCodesEqual([450], tx.rcpt_response)
                # self.assertEqual(tx.data_response.code, 450)
                break
            else:
                self.fail('no response')
        check_response()

        upstream_endpoint = FakeSyncFilter()
        upstream_endpoint.add_expectation(exp)
        self.add_endpoint(upstream_endpoint)
        self._dequeue(1)
        check_response()


class RouterServiceTestFastApi(RouterServiceTest):
    pass

class RouterServiceTestSqlite(RouterServiceTest):
    use_postgres = False

    # having multiple routers accessing the same sqlite causes
    # concurrency errors, probably not a useful configuration
    def test_multi_node(self):
        pass

if __name__ == '__main__':
    unittest.removeHandler()
    unittest.main(catchbreak=False)
