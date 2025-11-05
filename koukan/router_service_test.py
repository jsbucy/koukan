# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Any, List, Optional, Tuple
from threading import Lock, Condition, Thread
import logging
import logging.config
import unittest
import socketserver
import time
from functools import partial
from urllib.parse import urljoin, urlparse
from datetime import timedelta
import copy
import cProfile
from pstats import SortKey
import gc
import tempfile
import yaml

from httpx import Client
from werkzeug.datastructures import ContentRange
import werkzeug.http

from koukan.rest_schema import BlobUri, parse_blob_uri
import koukan.postgres_test_utils as postgres_test_utils
from koukan.router_service import Service
from koukan.rest_endpoint import RestEndpoint, RestEndpointClientProvider
from koukan.response import Response
from koukan.blob import Blob, CompositeBlob, InlineBlob
from koukan.fake_endpoints import FakeFilter
from koukan.filter import (
    HostPort,
    Mailbox,
    TransactionMetadata,
    WhichJson )
from koukan.executor import Executor
from koukan.deadline import Deadline

import koukan.sqlite_test_utils as sqlite_test_utils

from koukan.message_builder import MessageBuilderSpec

from koukan.storage_schema import BlobSpec
from koukan.sender import Sender

from examples.send_message.send_message import Sender as RestSender

def setUpModule():
    postgres_test_utils.setUpModule()

def tearDownModule():
    postgres_test_utils.tearDownModule()


class RouterServiceTest(unittest.TestCase):
    lock : Lock
    cv : Condition
    endpoints : List[FakeFilter]
    use_postgres = True
    client_provider : RestEndpointClientProvider

    def __init__(self, *args, **kwargs):
        super(RouterServiceTest, self).__init__(*args, **kwargs)

        self.endpoints = []

    def get_endpoint(self, yaml, sender : Sender):
        logging.debug('RouterServiceTest.get_endpoint')
        with self.lock:
            self.cv.wait_for(lambda: bool(self.endpoints))
            return self.endpoints.pop(0)

    def add_endpoint(self, endpoint):
        with self.lock:
            self.endpoints.append(endpoint)
            self.cv.notify_all()

    def _setup_router(self):
        with open('testdata/router_service_test-router.yaml', 'r') as yaml_file:
            self.root_yaml = root_yaml = yaml.load(
                yaml_file, Loader=yaml.CLoader)
        root_yaml['storage']['url'] = self.storage_url
        root_yaml['storage']['cache_ttl'] = 1

        # find a free port
        with socketserver.TCPServer(("localhost", 0), lambda x,y,z: None) as s:
            self.port = s.server_address[1]
        root_yaml['rest_listener']['addr'] = ('127.0.0.1', self.port)
        root_yaml['rest_listener']['session_uri'] = 'http://localhost:%d' % self.port
        router_url = 'http://localhost:%d/' % self.port
        service = Service(root_yaml=root_yaml)

        service.start_main()
        self.assertTrue(service.wait_started(5))
        # XXX
        service.filter_chain_factory.inject_filter(
            'sync', self.get_endpoint)

        return router_url, service

    def setUp(self):
        logging.basicConfig(
            level=logging.DEBUG,  # WARNING
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
        self.router_base_url, self.service = self._setup_router()
        self.router_url = urljoin(self.router_base_url, '/senders/submission/transactions')
        self.router_gw_url = urljoin(self.router_base_url, '/senders/ingress/transactions')

        self.client_provider = RestEndpointClientProvider()

        # probe for startup
        def exp(tx, tx_delta):
            logging.debug(tx)
            logging.debug(tx_delta)
            if tx_delta.cancelled:
                tx.fill_inflight_responses(Response(450))
            if tx.cancelled:
                return
            if tx_delta.mail_from:
                tx.mail_response = Response(201, 'probe mail ok')
            if tx_delta.rcpt_to:
                tx.rcpt_response = [Response(202)]

        for i in range(0,10):
            logging.info('RouterServiceTest.setUp probe %d', i)

            upstream = FakeFilter()
            upstream.add_expectation(exp)
            upstream.add_expectation(exp)
            upstream.add_expectation(exp)

            self.add_endpoint(upstream)

            rest_endpoint = self.create_endpoint(
                static_base_url=self.router_url,
                timeout_start=1, timeout_data=1)
            tx = rest_endpoint.downstream_tx
            delta = TransactionMetadata(
                mail_from = Mailbox('probe-from%d' % i),
                rcpt_to = [Mailbox('probe-to%d' % i)],
                sender=Sender('submission', 'probe'))
            tx.merge_from(delta)
            rest_endpoint.on_update(delta, 5)
            delta = TransactionMetadata(cancelled=True)
            tx.merge_from(delta)
            rest_endpoint.on_update(delta)
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
        self.client_provider.close()
        self.client_provider = None

        # TODO abort any inflight handlers here so we don't hang

        # TODO this should verify that there are no open tx attempts in storage
        # e.g. some exception path failed to tx_cursor.finalize_attempt()
        self.assertTrue(self.service.shutdown())

    def create_endpoint(self, **kwargs):
        endpoint = RestEndpoint(client_provider=self.client_provider, **kwargs)
        endpoint.wire_downstream(TransactionMetadata())
        return endpoint

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

    def _dequeue(self, n=1, service=None):
        if service is None:
            service = self.service
        deq = 0
        for i in range(0,10):
            if service.dequeue_one(service.output_executor):
                deq += 1
            logging.debug('RouterServiceTest._dequeue %d', deq)
            if deq == n:
                return
            time.sleep(0.3)
        else:
            self.fail('failed to dequeue')

    # simplest possible case: native rest/inline body -> upstream success
    def test_rest_smoke(self):
        prev_reads = self.service.storage._tx_reads
        logging.debug('RouterServiceTest.test_rest_smoke')
        rest_endpoint = self.create_endpoint(
            static_base_url=self.router_url,
            timeout_start=5, timeout_data=5)
        tx = rest_endpoint.downstream_tx
        body = b'hello, world!'
        delta = TransactionMetadata(
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob@example.com')],
            body=InlineBlob(body, last=True))
        tx.merge_from(delta)

        def exp(tx, tx_delta):
            logging.debug(tx)
            logging.debug(tx_delta)
            # xxx verify mail/rcpt
            if tx_delta.mail_from:
                tx.mail_response=Response(201)
            if tx_delta.rcpt_to:
                tx.rcpt_response=[Response(202)]
            if tx.body and tx.body.finalized():
                self.assertIn(body, tx.body.pread(0))
                tx.data_response=Response(203)

        upstream_endpoint = FakeFilter()
        upstream_endpoint.add_expectation(exp)
        upstream_endpoint.add_expectation(exp)
        upstream_endpoint.add_expectation(exp)
        self.add_endpoint(upstream_endpoint)

        rest_endpoint.on_update(delta)

        def done():
            logging.debug(tx)
            if tx.mail_response.code != 201:
                return False
            if [r.code for r in tx.rcpt_response] != [202]:
                return False
            if tx.data_response.code != 203:
                return False
            if tx.final_attempt_reason != 'upstream response success':
                return False
            return True
        deadline = Deadline(5)
        while deadline.remaining():
            if done():
                break
            rest_endpoint.get(deadline)
        else:
            self.fail('expected tx')
        self.assertEqual(0, self.service.storage._tx_reads - prev_reads)

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
        rest_endpoint = self.create_endpoint(
            static_base_url=self.router_url,
            timeout_start=5, timeout_data=5)
        body = 'hello, world!'
        tx = TransactionMetadata(
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob@example.com')])
            #inline_body=body)

        def exp(tx, tx_delta):
            time.sleep(1)
            upstream_delta=TransactionMetadata(
                mail_response=Response(201),
                rcpt_response=[Response(202)],
                data_response=Response(203))
            tx.merge_from(upstream_delta)
            return upstream_delta
        upstream_endpoint = FakeFilter()
        upstream_endpoint.add_expectation(exp)
        self.add_endpoint(upstream_endpoint)

        post_resp = rest_endpoint.client.post(
            rest_endpoint.base_url,
            json = tx.to_json(WhichJson.REST_CREATE),
            headers={'host': 'submission'})
        self.assertEqual(201, post_resp.status_code)
        tx_url = post_resp.headers['location']
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
        # this may see one more version update when the output handler
        # starts but should wait after that
        for i in range(0,2):
            logging.debug('get tx 2.%d', i)

            get_resp2 = rest_endpoint.client.get(
                tx_url,
                headers={'if-none-match': get_resp.headers['etag'],
                         'request-timeout': '5'})
            logging.debug(get_resp2)
            logging.debug(get_resp2.json())
            self.assertEqual(200, get_resp.status_code)
            self.assertNotEqual(get_resp2.headers['etag'],
                                get_resp.headers['etag'])
            tx_json = get_resp2.json()
            # depending on timing, we may get the tx before or after the
            # OH completion that sets final_attempt_reason but we should
            # get all the responses
            done = True
            for f in ['mail_response', 'rcpt_response', 'data_response']:
                if f not in tx_json:
                    done = False
                    break
            if not done:
                continue
            self.assertEqual(
                tx_json['mail_response'], {'code': 201, 'message': 'ok'})
            self.assertEqual(
                tx_json['rcpt_response'], [{'code': 202, 'message': 'ok'}])
            self.assertEqual(
                tx_json['data_response'], {'code': 203, 'message': 'ok'})


    def test_rest_body(self):
        logging.debug('RouterServiceTest.test_rest_body')
        rest_endpoint = self.create_endpoint(
            static_base_url=self.router_url,
            timeout_start=5, timeout_data=5)
        body = b'hello, world!'
        tx = TransactionMetadata()
        rest_endpoint.wire_downstream(tx)
        delta = TransactionMetadata(
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob@example.com')],
            body=InlineBlob(body, last=True))

        def exp(tx, tx_delta):
            upstream_delta=TransactionMetadata(
                mail_response=Response(201),
                rcpt_response=[Response(202)],
                data_response=Response(203))
            tx.merge_from(upstream_delta)
            return upstream_delta
        upstream_endpoint = FakeFilter()
        upstream_endpoint.add_expectation(exp)
        self.add_endpoint(upstream_endpoint)

        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
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
        self.assertIn('blob_status', tx_json['body'])
        self.assertIn('uri', tx_json['body']['blob_status'])
        del tx_json['body']['blob_status']['uri']
        self.assertEqual(tx_json, {
            'sender': {'name': 'submission'},
            'mail_from': {},
            'rcpt_to': [{}],
            'body': {'blob_status': {#'content_length': 13, 'length': 13,
                'finalized': True}},
            'retry': {},
            'mail_response': {'code': 201, 'message': 'ok'},
            'rcpt_response': [{'code': 202, 'message': 'ok'}],
            'data_response': {'code': 203, 'message': 'ok'},
            'final_attempt_reason': 'upstream response success'
        })


    def test_rest_body_http_retry(self):
        logging.debug('RouterServiceTest.test_rest_body_http_retry')
        rest_endpoint = self.create_endpoint(
            static_base_url=self.router_url,
            timeout_start=5, timeout_data=5)
        body = b'hello, world!'
        tx = TransactionMetadata()
        rest_endpoint.wire_downstream(tx)
        delta = TransactionMetadata(
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob@example.com')])

        def exp(tx, tx_delta):
            upstream_delta=TransactionMetadata(
                mail_response=Response(201),
                rcpt_response=[Response(202)])
            tx.merge_from(upstream_delta)
            return upstream_delta
        upstream_endpoint = FakeFilter()
        upstream_endpoint.add_expectation(exp)
        self.add_endpoint(upstream_endpoint)

        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])

        def exp(tx, tx_delta):
            upstream_delta=TransactionMetadata(
                data_response=Response(203))
            tx.merge_from(upstream_delta)
            return upstream_delta
        upstream_endpoint.add_expectation(exp)


        body = 200000 * b'hello, '
        chunk1 = len(body)
        def data():
            yield body
            raise EOFError()  # simulate request timeout

        try:
            resp = rest_endpoint.client.put(
                rest_endpoint.rest_upstream_tx.body.reuse_uri.parsed_uri,
                content = data())
            self.assertEqual(200, resp.status_code)
        except:
            pass

        logging.debug('retry')

        body += b'world!'
        resp = rest_endpoint.client.put(
            rest_endpoint.rest_upstream_tx.body.reuse_uri.parsed_uri,
            content = body)
        logging.debug(resp)
        logging.debug(resp.headers)
        self.assertEqual(200, resp.status_code)


    def test_rest_body_chunked(self):
        logging.debug('RouterServiceTest.test_rest_body_chunked')
        rest_endpoint = self.create_endpoint(
            static_base_url=self.router_url,
            timeout_start=5, timeout_data=5)
        body = b'hello, world!'
        tx = TransactionMetadata()
        rest_endpoint.wire_downstream(tx)
        delta = TransactionMetadata(
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob@example.com')])

        def exp(tx, tx_delta):
            upstream_delta=TransactionMetadata(
                mail_response=Response(201),
                rcpt_response=[Response(202)])
            tx.merge_from(upstream_delta)
            return upstream_delta
        upstream_endpoint = FakeFilter()
        upstream_endpoint.add_expectation(exp)
        self.add_endpoint(upstream_endpoint)

        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])

        def exp(tx, tx_delta):
            upstream_delta=TransactionMetadata(
                data_response=Response(203))
            tx.merge_from(upstream_delta)
            return upstream_delta
        upstream_endpoint.add_expectation(exp)

        def data():
            yield b'hello, '
            yield b'world!'

        resp = rest_endpoint.client.put(
            rest_endpoint.rest_upstream_tx.body.reuse_uri.parsed_uri,
            content = data())
        logging.debug(resp)

        for i in range(0,3):
            tx_json = rest_endpoint.get_json()
            logging.debug('RouterServiceTest.test_rest_body create %s',
                          tx_json)

            if 'attempt_count' in tx_json:
                del tx_json['attempt_count']
            if 'blob_status' not in tx_json['body']:
                continue
            if 'uri' not in tx_json['body']['blob_status']:
                continue
            del tx_json['body']['blob_status']['uri']

            if tx_json == {
                'sender': {'name': 'submission'},
                'mail_from': {},
                'rcpt_to': [{}],
                'body': {'blob_status': {#'content_length': 13, 'length': 13,
                    'finalized': True}},
                'retry': {},
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'}],
                'data_response': {'code': 203, 'message': 'ok'},
                'final_attempt_reason': 'upstream response success' }:
                break
            logging.debug(tx_json)
            time.sleep(1)
        else:
            self.fail('expected tx')

    # upstream temp during submission before input done
    # downstream should complete uploading the body -> input_done -> reload
    def test_submission_retry(self):
        rest_endpoint = self.create_endpoint(
            static_base_url=self.router_url,
            timeout_start=5, timeout_data=5)
        body = b'hello, world!'
        tx = TransactionMetadata()
        rest_endpoint.wire_downstream(tx)
        delta = TransactionMetadata(
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob@example.com')])

        def exp(tx, tx_delta):
            upstream_delta=TransactionMetadata(
                mail_response=Response(201),
                rcpt_response=[Response(402)])
            tx.merge_from(upstream_delta)
            return upstream_delta
        upstream_endpoint = FakeFilter()
        upstream_endpoint.add_expectation(exp)
        self.add_endpoint(upstream_endpoint)

        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [402])

        def exp_body(tx, tx_delta):
            self.assertIsNotNone(tx.body)
            upstream_delta = TransactionMetadata()
            tx.fill_inflight_responses(Response(550), upstream_delta)
            tx.merge_from(upstream_delta)
            return upstream_delta

        upstream_endpoint.add_expectation(exp_body)

        body_url = rest_endpoint.rest_upstream_tx.body.reuse_uri.parsed_uri

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
            tx.merge_from(upstream_delta)
            return upstream_delta
        upstream_endpoint = FakeFilter()
        upstream_endpoint.add_expectation(exp_body)
        self.add_endpoint(upstream_endpoint)

        logging.debug(self.service.storage.debug_dump())
        self._dequeue()

        for i in range(0, 5):
            tx_json = rest_endpoint.get_json()
            logging.debug(tx_json)

            if 'blob_status' not in tx_json['body'] or 'uri' not in tx_json['body']['blob_status']:
                continue
            del tx_json['body']['blob_status']['uri']

            if tx_json == {
                    'sender': {'name': 'submission'},
                    'mail_from': {},
                    'rcpt_to': [{}],
                    'body': {'blob_status': {'finalized': True}},
                    'retry': {},
                    'mail_response': {'code': 201, 'message': 'ok'},
                    'rcpt_response': [{'code': 202, 'message': 'ok'}],
                    'data_response': {'code': 203, 'message': 'ok'},
                    'attempt_count': 2,
                    'final_attempt_reason': 'upstream response success'}:
                break
            time.sleep(0.3)
        else:
            self.fail('expected tx')



    def test_reuse_body(self):
        logging.debug('RouterServiceTest.test_reuse_body')

        body_file = tempfile.NamedTemporaryFile()
        body = 'hello, world!'
        body_utf8 = body.encode('utf-8')
        body_file.write(body_utf8)
        body_file.flush()

        sender = RestSender(self.router_url,
                            'alice@example.com',
                            body_filename=body_file.name,
                            sender='submission')

        upstream_endpoint = FakeFilter()
        def exp_env(tx, tx_delta):
            upstream_delta = TransactionMetadata(
                mail_response = Response(201),
                rcpt_response = [Response(202)])
            tx.merge_from(upstream_delta)
            return upstream_delta
        def exp_body(tx, tx_delta):
            if tx.body is None or not tx.body.finalized():
                return TransactionMetadata()
            self.assertEqual(tx.body.pread(0), body_utf8)
            upstream_delta = TransactionMetadata(
                data_response = Response(203))
            tx.merge_from(upstream_delta)
            return upstream_delta
        upstream_endpoint.add_expectation(exp_env)
        upstream_endpoint.add_expectation(exp_body)
        upstream_endpoint.add_expectation(exp_body)
        self.add_endpoint(upstream_endpoint)


        sender.send('bob@example.com')

        self.assertEqual(201, sender.tx_json['mail_response']['code'])
        self.assertEqual(
            [202], [r['code'] for r in sender.tx_json['rcpt_response']])
        self.assertEqual(203, sender.tx_json['data_response']['code'])

        upstream_endpoint = FakeFilter()
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

            tx.merge_from(upstream_delta)
            return upstream_delta
        upstream_endpoint.add_expectation(exp2)
        upstream_endpoint.add_expectation(exp2)
        self.add_endpoint(upstream_endpoint)

        sender.force_reuse = True
        sender.send('bob2@example.com')
        self.assertEqual(201, sender.tx_json['mail_response']['code'])
        self.assertEqual(
            [202], [r['code'] for r in sender.tx_json['rcpt_response']])
        self.assertEqual(203, sender.tx_json['data_response']['code'])

    # xxx need non-exploder test w/filter api, problems in
    # post-exploder/upstream tx won't be reported out synchronously,
    # would potentially bounce
    # and/or get ahold of those tx IDs and verify the status directly

    # TODO add exploder s&f test

    def test_exploder_multi_rcpt(self):
        prev_reads = self.service.storage._tx_reads
        logging.info('RouterServiceTest.test_exploder_multi_rcpt')
        rest_endpoint = self.create_endpoint(
            static_base_url=self.router_url,
            timeout_start=5, timeout_data=5)
        tx = TransactionMetadata()
        rest_endpoint.wire_downstream(tx)

        logging.info('testExploderMultiRcpt start tx')
        delta = TransactionMetadata(
            sender=Sender(name='submission', tag='smtp-msa'),
            mail_from=Mailbox('alice@example.com'),
            remote_host=HostPort('1.2.3.4', 12345))
        tx.merge_from(delta)
        rest_endpoint.on_update(delta)

        # no rcpt -> buffered
        self.assertEqual(tx.mail_response.code, 250)
        self.assertIn('exploder noop', tx.mail_response.message)

        upstream_endpoint = FakeFilter()
        self.add_endpoint(upstream_endpoint)
        def exp_rcpt(i, tx, tx_delta):
            logging.debug(tx)
            if tx_delta.mail_from:
                tx.mail_response = Response(201 + i)
            if tx_delta.rcpt_to:
                tx.rcpt_response.append(
                    Response(203 + i, 'upstream rcpt %d' % i))
            if tx.body and tx.body.finalized():
                self.assertEqual(tx.body.pread(0), b'Hello, World!')
                tx.data_response = Response(
                    205 + i, 'upstream data %d' % i)

        for i in range(0,3):
            upstream_endpoint.add_expectation(partial(exp_rcpt, 0))

        logging.info('testExploderMultiRcpt patch rcpt1')
        prev = tx.copy()
        tx.rcpt_to.append(Mailbox('bob@example.com'))
        tx_delta = prev.delta(tx)
        rest_endpoint.on_update(tx_delta)
        self.assertRcptCodesEqual([203], tx.rcpt_response)
        self.assertEqual(tx.rcpt_response[0].message, 'upstream rcpt 0')

        # upstream tx #2
        upstream_endpoint2 = FakeFilter()
        self.add_endpoint(upstream_endpoint2)

        for i in range(0, 3):
            upstream_endpoint2.add_expectation(partial(exp_rcpt, 1))

        logging.info('testExploderMultiRcpt patch rcpt2')
        prev = tx.copy()
        tx.rcpt_to.append(Mailbox('bob2@example.com'))
        tx_delta = prev.delta(tx)
        rest_endpoint.on_update(tx_delta)

        self.assertRcptCodesEqual([203, 204], tx.rcpt_response)
        self.assertEqual('upstream rcpt 1', tx.rcpt_response[1].message)
        logging.debug('env done')

        logging.info('testExploderMultiRcpt patch body')

        tx_delta = TransactionMetadata(
            body=InlineBlob(b'Hello, World!', last=True))
        self.assertIsNotNone(tx.merge_from(tx_delta))
        rest_endpoint.on_update(tx_delta)
        logging.debug('test_exploder_multi_rcpt %s', tx)
        self.assertEqual(205, tx.data_response.code)
        self.assertEqual('upstream data 0 (Exploder same response)',
                         tx.data_response.message)
        self.assertEqual(0, self.service.storage._tx_reads - prev_reads)


    def _rest_smoke_micro(self):
        logging.debug('_rest_smoke_micro')
        rest_endpoint = self.create_endpoint(
            static_base_url=self.router_url,
            timeout_start=5, timeout_data=5)
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

            tx.merge_from(upstream_delta)
            return upstream_delta
        upstream_endpoint = FakeFilter()
        upstream_endpoint.add_expectation(exp)
        upstream_endpoint.add_expectation(exp)
        upstream_endpoint.add_expectation(exp)
        self.add_endpoint(upstream_endpoint)


        body = b'hello, world!'
        tx = rest_endpoint.downstream_tx
        delta = TransactionMetadata(
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob@example.com')],
            body=InlineBlob(body, last=True))
        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        logging.debug(tx)
        self.assertEqual(201, tx.mail_response.code)
        self.assertEqual([202], [r.code for r in tx.rcpt_response])
        self.assertEqual(203, tx.data_response.code)

        # match new hanging get in test_rest_smoke()?

    def _exploder_micro(self):
        upstream_endpoint = FakeFilter()
        self.add_endpoint(upstream_endpoint)
        def exp_rcpt(tx, tx_delta):
            logging.debug(tx)
            # set upstream responses so output (retry) succeeds
            # upstream success, retry succeeds, propagates down to rest
            updated_tx = tx.copy()
            if tx_delta.mail_from:
                updated_tx.mail_response = Response(201)
            if tx_delta.rcpt_to:
                updated_tx.rcpt_response.append(Response(202, 'upstream rcpt 1'))
            upstream_delta = tx.delta(updated_tx)
            tx.merge_from(upstream_delta)
            return upstream_delta
        upstream_endpoint.add_expectation(exp_rcpt)

        def exp_body(tx, tx_delta):
            logging.debug('exp_body')
            upstream_delta = TransactionMetadata()
            if tx.body and tx.body.finalized():
                self.assertEqual(tx.body.pread(0), b'Hello, World!')
                upstream_delta.data_response = Response(
                    205, 'upstream data')
            tx.merge_from(upstream_delta)
            return upstream_delta

        upstream_endpoint.add_expectation(exp_body)
        upstream_endpoint.add_expectation(exp_body)


        rest_endpoint = self.create_endpoint(
            static_base_url=self.router_url,
            timeout_start=5, timeout_data=5)

        logging.info('_exploder_micro start tx')
        prev = TransactionMetadata()
        tx = rest_endpoint.upstream_tx
        tx.mail_from = Mailbox('alice@example.com')
        tx.remote_host = HostPort('1.2.3.4', 12345)
        rest_endpoint.on_update(prev.delta(tx))

        # no rcpt -> buffered
        self.assertEqual(tx.mail_response.code, 250)
        self.assertIn('exploder noop', tx.mail_response.message)

        logging.info('_exploder_micro patch rcpt')
        prev = tx.copy()
        tx.rcpt_to.append(Mailbox('bob@example.com'))
        rest_endpoint.on_update(prev.delta(tx))
        self.assertRcptCodesEqual([202], tx.rcpt_response)
        self.assertEqual(tx.rcpt_response[0].message, 'upstream rcpt 1')

        logging.info('_exploder_micro patch body')

        prev = tx.copy()
        tx.body=InlineBlob(b'Hello, World!', last=True)
        rest_endpoint.on_update(prev.delta(tx))
        logging.debug('test_exploder_multi_rcpt %s', tx)
        self.assertEqual(205, tx.data_response.code)
        self.assertEqual('upstream data', tx.data_response.message)


    def _test_micro(self, micro):
        logging.debug('warmup')
        micro()
        logging.debug('real')
        start = time.monotonic()
        iters=100
        para=1
        def run_micro():
            for i in range(0,int(iters/para)):
                start = time.monotonic()
                logging.debug('micro iter start')
                micro()
                logging.debug('micro iter done %f', time.monotonic() - start)

        done = None
        def pmicro():
            threads = []
            for i in range(0,para - 1):
                t = Thread(target=run_micro)
                t.start()
                threads.append(t)
            run_micro()
            for t in threads:
                t.join()
            nonlocal done
            done = time.monotonic()
        cProfile.runctx('fn()', None, {'fn': partial(pmicro)},
                        #sort=SortKey.CUMULATIVE
                        sort=SortKey.TIME)
        total = done - start
        logging.warning('done %s %f %f', micro, total, iters/total)

    def disabled_test_micro(self):
        self._test_micro(self._rest_smoke_micro)
        self._test_micro(self._exploder_micro)


    def test_notification_retry_timeout(self):
        logging.info('RouterServiceTest.test_notification_retry_timeout')
        rest_endpoint = self.create_endpoint(
            static_base_url=self.router_url)

        cancelled = False

        def exp(tx, tx_delta):
            nonlocal cancelled
            logging.debug(tx)
            if tx.cancelled:
                cancelled = True
            self.assertEqual(tx.mail_from.mailbox, 'alice@example.com')
            self.assertEqual([m.mailbox for m in tx.rcpt_to],
                             ['bob@example.com'])
            prev = tx.copy()
            tx.mail_response = Response(201)
            tx.rcpt_response = [Response(402)]
            return prev.delta(tx)
        upstream_endpoint = FakeFilter()
        for i in range(0, 3):
            upstream_endpoint.add_expectation(exp)
        self.add_endpoint(upstream_endpoint)

        logging.info('test_notification start tx')
        tx = TransactionMetadata()
        rest_endpoint.wire_downstream(tx)
        delta = TransactionMetadata(
            sender=Sender(name='submission', tag='smtp-msa'),
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob@example.com')],
            body=InlineBlob(b'Hello, World!', last=True),
            remote_host=HostPort('1.2.3.4', 12345))
        tx.merge_from(delta)
        rest_endpoint.on_update(delta, 10)
        logging.debug('RouterServiceTest.test_notification after update %s',
                      tx)

        self.assertEqual(250, tx.mail_response.code, 201)
        self.assertEqual('MAIL ok (exploder noop)', tx.mail_response.message)
        self.assertRcptCodesEqual([250], tx.rcpt_response)
        self.assertIn('RCPT ok (AsyncFilterWrapper store&forward)',
                      tx.rcpt_response[0].message)

        for i in range(0,2):
            logging.debug('test_notification upstream tx %d', i)
            upstream_endpoint = FakeFilter()
            def exp(tx, tx_delta):
                logging.debug(tx)
                self.assertEqual(tx.mail_from.mailbox, 'alice@example.com')
                self.assertEqual([m.mailbox for m in tx.rcpt_to],
                                 ['bob@example.com'])
                prev = tx.copy()
                tx.mail_response = Response(201)
                tx.rcpt_response = [Response(402)]
                return prev.delta(tx)
            for j in range(0, 3):
                upstream_endpoint.add_expectation(exp)

            logging.debug('dequeue attempt')
            self.add_endpoint(upstream_endpoint)
            self._dequeue()

            if i == 1:
                logging.debug('expect dsn')
                dsn_endpoint = FakeFilter()
                def exp_dsn(tx, tx_delta):
                    logging.debug(tx)
                    self.assertEqual(tx.mail_from.mailbox, '')
                    self.assertEqual([m.mailbox for m in tx.rcpt_to],
                                     ['alice@example.com'])
                    dsn = tx.body.pread(0)
                    logging.debug('test_notification %s', dsn)
                    self.assertIn(b'subject: Delivery Status Notification', dsn)

                    prev = tx.copy()
                    tx.mail_response = Response(203)
                    tx.rcpt_response = [Response(204)]
                    tx.data_response = Response(205)
                    return prev.delta(tx)
                dsn_endpoint.add_expectation(exp_dsn)
                logging.debug('dequeue dsn')
                self.add_endpoint(dsn_endpoint)
                self._dequeue()

    def test_notification_fast_perm(self):
        logging.info('RouterServiceTest.test_notification_fast_perm')
        rest_endpoint = self.create_endpoint(
            static_base_url=self.router_url)

        logging.info('test_notification start tx')
        tx = TransactionMetadata()
        rest_endpoint.wire_downstream(tx)
        delta = TransactionMetadata(
            sender=Sender(name='submission', tag='smtp-msa'),
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
            upstream = FakeFilter()
            logging.debug(id(upstream))
            upstream.add_expectation(exp_rcpt)
            self.add_endpoint(upstream)
        tx.merge_from(delta)
        rest_endpoint.on_update(delta, 10)
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
            unused_upstream_endpoint = FakeFilter()
            logging.debug(id(unused_upstream_endpoint))
            self.add_endpoint(unused_upstream_endpoint)

        dsn_endpoint = FakeFilter()
        logging.debug(id(dsn_endpoint))
        def exp_dsn(tx, tx_delta):
            logging.error(tx)
            self.assertEqual('', tx.mail_from.mailbox)
            self.assertEqual('alice@example.com', tx.rcpt_to[0].mailbox)
            upstream_delta=TransactionMetadata(
                mail_response=Response(250),
                rcpt_response=[Response(250)])

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

        upstream_endpoint = FakeFilter()
        self.add_endpoint(upstream_endpoint)

        logging.info('test_message_builder start 1st tx')
        read_body = False
        def exp(tx, tx_delta):
            logging.debug('test_message_builder.exp %s', tx)
            self.assertEqual('alice@example.com', tx.mail_from.mailbox)
            self.assertEqual(['bob1@example.com'],
                             [m.mailbox for m in tx.rcpt_to])
            upstream_delta = TransactionMetadata()
            prev = tx.copy()
            if tx_delta.mail_from:
                tx.mail_response = Response(201)
            if tx_delta.rcpt_to:
                tx.rcpt_response = [Response(202)]
            if tx_delta.body is None or not tx_delta.body.finalized():
                return prev.delta(tx)
            body = tx.body.pread(0)
            logging.debug(body)
            self.assertIn(b'subject: hello\r\n', body)
            tx.data_response = Response(203)
            nonlocal read_body
            read_body = True
            return prev.delta(tx)

        for i in range(0,3):
            upstream_endpoint.add_expectation(exp)

        body_file = tempfile.NamedTemporaryFile()
        body = 'hello, world!'
        body_utf8 = body.encode('utf-8')
        body_file.write(body_utf8)
        body_file.flush()

        sender = RestSender(self.router_url,
                            'alice@example.com',
                            sender='submission',
                            message_builder = {
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
                "content": {"create_id": "my_plain_body",
                            "filename": body_file.name}
            }]
        })

        sender.send('bob1@example.com')
        self.assertEqual(201, sender.tx_json['mail_response']['code'])
        self.assertEqual([202], [r['code'] for r in sender.tx_json['rcpt_response']])
        self.assertEqual(203, sender.tx_json['data_response']['code'])
        self.assertTrue(read_body)

        logging.debug('start second tx')

        upstream_endpoint = FakeFilter()
        self.add_endpoint(upstream_endpoint)

        read_body = False
        def exp2(tx, tx_delta):
            logging.debug('test_message_builder.exp2 %s', tx)
            self.assertEqual('alice@example.com', tx.mail_from.mailbox)
            self.assertEqual(['bob2@example.com'],
                             [m.mailbox for m in tx.rcpt_to])
            upstream_delta = TransactionMetadata()
            prev = tx.copy()
            if tx_delta.mail_from:
                tx.mail_response = Response(204)
            if tx_delta.rcpt_to:
                tx.rcpt_response = [Response(205)]
            if tx_delta.body is None or not tx_delta.body.finalized():
                return prev.delta(tx)
            body = tx.body.pread(0)
            logging.debug(body)
            self.assertIn(b'subject: hello\r\n', body)
            tx.data_response = Response(206)
            nonlocal read_body
            read_body = True
            return prev.delta(tx)

        for i in range(0,2):
            upstream_endpoint.add_expectation(exp2)

        sender.force_reuse = True
        sender.send('bob2@example.com')
        logging.debug(sender.tx_json)
        self.assertEqual(204, sender.tx_json['mail_response']['code'])
        self.assertEqual([205], [r['code'] for r in sender.tx_json['rcpt_response']])
        self.assertEqual(206, sender.tx_json['data_response']['code'])
        self.assertTrue(read_body)

    def test_receive_parsing(self):
        logging.info('RouterServiceTest.test_receive_parsing')
        rest_endpoint = self.create_endpoint(
            static_base_url=self.router_gw_url,
            timeout_start=5, timeout_data=5)
        tx = TransactionMetadata()
        rest_endpoint.wire_downstream(tx)
        with open('testdata/multipart.msg', 'rb') as f:
            body = f.read()
        delta = TransactionMetadata(
            sender=Sender(name='ingress', tag='smtp-mx'),
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
                self.assertEqual(tx_delta.body.blobs['0'].pread(0),
                                 b'yolocat')
                self.assertEqual(tx_delta.body.blobs['1'].pread(0),
                                 b'yolocat2')
                self.assertIsNotNone(tx_delta.body.json)
                upstream_delta.data_response=Response(203)
            tx.merge_from(upstream_delta)
            logging.debug(upstream_delta)
            return upstream_delta
        upstream_endpoint = FakeFilter()
        for i in range(0, 3):
            upstream_endpoint.add_expectation(exp)
        self.add_endpoint(upstream_endpoint)

        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        self.assertEqual(tx.mail_response.code, 250)
        self.assertRcptCodesEqual([202], tx.rcpt_response)
        self.assertEqual(tx.data_response.code, 203)

    def test_multi_node(self):
        service2_url, service2 = self._setup_router()

        # start a tx on self.service
        rest_endpoint = self.create_endpoint(
            static_base_url=self.router_url,
            timeout_start=5, timeout_data=5)
        body = b'hello, world!'
        tx = TransactionMetadata()
        rest_endpoint.wire_downstream(tx)
        delta = TransactionMetadata(mail_from=Mailbox('alice@example.com'),
                                    rcpt_to=[Mailbox('bob@example.com')])

        upstream_endpoint = FakeFilter()
        self.add_endpoint(upstream_endpoint)

        # NOTE OutputHandler currently sets final_attempt_reason on
        # downstream_timeout so this only fails over if the OH/router
        # "unexpectedly" failed.
        def exp_rcpt(tx, tx_delta):
            time.sleep(1)
            raise Exception()  # simulate router crash
        upstream_endpoint.add_expectation(exp_rcpt)

        tx.merge_from(delta)
        rest_endpoint.on_update(delta, timeout=1)
        tx_url = rest_endpoint.transaction_url
        logging.debug(rest_endpoint.transaction_url)
        parsed = urlparse(rest_endpoint.transaction_url)

        tx_url2 = urljoin(service2_url, parsed.path)

        # GET the tx from service2, since the tx is leased in
        # self.service, it should be redirected

        client = Client()
        resp = client.get(tx_url2)
        logging.debug('%s %s', resp.status_code, resp.headers)
        self.assertEqual(308, resp.status_code)
        self.assertEqual(tx_url, resp.headers['location'])

        resp = client.patch(tx_url2,
                            headers={'content-type': 'application/json',
                                     'if-match': '123'},
                            json={})
        logging.debug('%s %s %s', resp.status_code, resp.headers, resp.content)
        self.assertEqual(308, resp.status_code)
        self.assertEqual(tx_url, resp.headers['location'])

        resp = client.put(tx_url2 + '/body', content=body)
        logging.debug('%s %s %s', resp.status_code, resp.headers, resp.content)
        self.assertEqual(308, resp.status_code)
        self.assertEqual(tx_url + '/body', resp.headers['location'])

        # OH terminated by uncaught exception from exp_rcpt()
        # -> tx unleased
        self.service.shutdown()

        resp = client.get(tx_url2)
        logging.debug('%s %s', resp.status_code, resp.json())
        self.assertEqual(200, resp.status_code)

        # sending the body will make the tx input_done -> loadable
        upstream_endpoint2 = FakeFilter()
        self.add_endpoint(upstream_endpoint2)

        def exp_body(tx, tx_delta):
            time.sleep(1)
            upstream_delta=TransactionMetadata()
            prev = tx.copy()
            if tx_delta.mail_from:
                tx.mail_response=Response(203)
            if tx_delta.rcpt_to:
                tx.rcpt_response=[Response(204)]
            if tx_delta.body and tx_delta.body.finalized():
                tx.data_response = Response(205)
            return upstream_delta
        for i in range(0,3):
            upstream_endpoint2.add_expectation(exp_body)

        resp = client.put(tx_url2 + '/body', content=body)
        logging.debug('%s %s %s', resp.status_code, resp.headers, resp.content)
        self.assertEqual(200, resp.status_code)

        self._dequeue(1, service2)

        deadline = Deadline(5)
        while deadline.remaining():
            resp = client.get(
                tx_url2,
                headers={'request-timeout': str(int(deadline.remaining()))},
                timeout = deadline.remaining())

            tx_json = resp.json()
            logging.debug('%s %s', resp.status_code, tx_json)
            if 'final_attempt_reason' not in tx_json:
                time.sleep(0.3)
                continue
            self.assertEqual(203, tx_json['mail_response']['code'])
            self.assertEqual(
                [204], [r['code'] for r in tx_json['rcpt_response']])
            self.assertEqual(205, tx_json['data_response']['code'])
            self.assertEqual(
                'upstream response success', tx_json['final_attempt_reason'])
            break
        else:
            self.fail('expected tx')

        self.assertTrue(service2.shutdown())

    def _test_add_route_sync(
            self,
            mail_resp, rcpt_resp, data_resp,
            mail_code, rcpt_code, data_code):
        rest_endpoint = self.create_endpoint(
            static_base_url=self.router_url,
            timeout_start=5, timeout_data=5)
        tx = TransactionMetadata()
        rest_endpoint.wire_downstream(tx)
        body = b'hello, world!'
        delta = TransactionMetadata(
            sender=Sender(name='submission', tag='submission-sync-sor'),
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob@example.com')],
            body=InlineBlob(body, last=True))

        def exp_add_route(tx, tx_delta):
            logging.debug(tx)
            #self.assertEqual('submission-sor', tx.host)
            prev = tx.copy()
            if tx.mail_from and not tx.mail_response:
                tx.mail_response = mail_resp
            if tx.rcpt_to and not tx.rcpt_response:
                tx.rcpt_response = [rcpt_resp]
            if tx.body and tx.body.finalized() and not tx.data_response:
                tx.data_response = data_resp
            return prev.delta(tx)
        add_route_endpoint = FakeFilter()
        add_route_endpoint.add_expectation(exp_add_route)
        add_route_endpoint.add_expectation(exp_add_route)
        self.add_endpoint(add_route_endpoint)

        def exp_upstream(tx, tx_delta):
            logging.debug(tx)
            #self.assertEqual('sor', tx.host)
            prev = tx.copy()
            if tx.mail_from and not tx.mail_response:
                tx.mail_response = Response(201)
            if tx.rcpt_to and not tx.rcpt_response:
                tx.rcpt_response = [Response(203)]
            if tx.body and tx.body.finalized() and not tx.data_response:
                tx.data_response = Response(205)
            return prev.delta(tx)
        upstream_endpoint = FakeFilter()
        upstream_endpoint.add_expectation(exp_upstream)
        upstream_endpoint.add_expectation(exp_upstream)
        self.add_endpoint(upstream_endpoint)

        rest_endpoint.downstream_tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        self.assertEqual(tx.mail_response.code, mail_code)
        self.assertRcptCodesEqual([rcpt_code], tx.rcpt_response)
        self.assertEqual(tx.data_response.code, data_code)

    def test_add_route_sync_success(self):
        self._test_add_route_sync(
            Response(202),
            Response(204),
            Response(206),
            201, 203, 205)

    def test_add_route_sync_err(self):
        self._test_add_route_sync(
            Response(402),
            Response(404),
            Response(406),
            402, 404, 503)

    def test_add_route_sync_data_err(self):
        self._test_add_route_sync(
            Response(202),
            Response(204),
            Response(406),
            201, 203, 406)

    def test_add_route_sf_success(self):
        rest_endpoint = self.create_endpoint(
            static_base_url=self.router_url,
            timeout_start=5, timeout_data=5)
        body = b'hello, world!'
        tx = TransactionMetadata()
        rest_endpoint.wire_downstream(tx)
        delta = TransactionMetadata(
            sender=Sender(name='submission', tag='submission-sf-sor'),
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob@example.com')],
            body=InlineBlob(body, last=True))

        def exp_add_route(tx, tx_delta):
            logging.debug(tx)
            self.assertEqual('sor', tx.sender.name)
            upstream_delta=TransactionMetadata(
                mail_response=Response(202),
                rcpt_response=[Response(204)],
                data_response=Response(206))
            tx.merge_from(upstream_delta)
            return upstream_delta
        add_route_endpoint = FakeFilter()
        add_route_endpoint.add_expectation(exp_add_route)
        self.add_endpoint(add_route_endpoint)

        def exp_upstream(tx, tx_delta):
            logging.debug(tx)
            self.assertEqual('submission', tx.sender.name)
            self.assertEqual('submission-sf-sor', tx.sender.tag)
            upstream_delta=TransactionMetadata(
                mail_response=Response(201),
                rcpt_response=[Response(203)],
                data_response=Response(205))
            tx.merge_from(upstream_delta)
            return upstream_delta
        upstream_endpoint = FakeFilter()
        upstream_endpoint.add_expectation(exp_upstream)
        self.add_endpoint(upstream_endpoint)

        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertRcptCodesEqual([203], tx.rcpt_response)
        self.assertEqual(tx.data_response.code, 205)

        # self._dequeue(1)
        while add_route_endpoint.expectation:
            logging.debug('add route')
            time.sleep(1)

    def test_output_handler_exception(self):
        rest_endpoint = self.create_endpoint(
            static_base_url=self.router_url,
            timeout_start=5, timeout_data=5)
        body = b'hello, world!'
        tx = TransactionMetadata()
        rest_endpoint.wire_downstream(tx)
        delta = TransactionMetadata(
            mail_from=Mailbox('alice@example.com'),
            rcpt_to=[Mailbox('bob@example.com')],
            body=InlineBlob(body, last=True))

        def exp(tx, tx_delta):
            raise ValueError('bad')
        upstream_endpoint = FakeFilter()
        upstream_endpoint.add_expectation(exp)
        self.add_endpoint(upstream_endpoint)
        tx.merge_from(delta)
        rest_endpoint.on_update(delta)

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

        upstream_endpoint = FakeFilter()
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
