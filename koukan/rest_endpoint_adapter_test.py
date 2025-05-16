# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import List, Optional, Tuple
import unittest
import logging
import io
import json
import time
from threading import Condition, Lock

from werkzeug.datastructures import ContentRange
from werkzeug.http import parse_content_range_header

from fastapi import (
    Request as FastApiRequest,
    Response as FastApiResponse )

from httpx import Response as HttpxResponse

from koukan.blob import InlineBlob
from koukan.rest_endpoint_adapter import RestHandler, SyncFilterAdapter
from koukan.fake_endpoints import FakeSyncFilter, MockAsyncFilter
from koukan.executor import Executor
from koukan.filter import Mailbox, TransactionMetadata, WhichJson
from koukan.response import Response
from koukan.executor import Executor
from koukan.storage_schema import VersionConflictException

class SyncFilterAdapterTest(unittest.TestCase):
    def setUp(self):
        self.executor = Executor(inflight_limit=10, watchdog_timeout=5)

    def tearDown(self):
        self.executor.shutdown(timeout=5)

    def test_basic(self):
        upstream = FakeSyncFilter()
        sync_filter_adapter = SyncFilterAdapter(
            self.executor, upstream, 'rest_id')

        def exp_mail(tx, tx_delta):
            self.assertEqual(tx.mail_from.mailbox, 'alice')
            upstream_delta=TransactionMetadata(
                mail_response = Response(201))
            self.assertIsNotNone(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream.add_expectation(exp_mail)

        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        sync_filter_adapter.update(tx, tx.copy())
        sync_filter_adapter.wait(tx.version, 1)
        upstream_tx = sync_filter_adapter.get()
        self.assertEqual(upstream_tx.mail_response.code, 201)

        def exp_rcpt(tx, tx_delta):
            self.assertEqual(tx.mail_from.mailbox, 'alice')
            self.assertEqual([r.mailbox for r in tx.rcpt_to], ['bob'])
            upstream_delta=TransactionMetadata(
                rcpt_response = [Response(202)])
            self.assertIsNotNone(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream.add_expectation(exp_rcpt)

        delta = TransactionMetadata(rcpt_to=[Mailbox('bob')])
        tx.merge_from(delta)
        tx.version = sync_filter_adapter.version()
        sync_filter_adapter.update(tx, delta)
        for i in range(0,3):
            sync_filter_adapter.wait(tx.version, 1)
            tx = sync_filter_adapter.get()
            if [r.code for r in tx.rcpt_response] == [202]:
                break
            self.assertFalse(sync_filter_adapter.done)
        else:
            self.fail('expected rcpt_response')

        body = b'hello, world!'

        def exp_body(tx, tx_delta):
            logging.debug(tx.body)
            if not tx.body.finalized():
                return
            self.assertEqual(body, tx.body.pread(0))
            upstream_delta=TransactionMetadata(
                data_response = Response(203))
            self.assertIsNotNone(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream.add_expectation(exp_body)

        blob_writer = sync_filter_adapter.get_blob_writer(
            create=True, blob_rest_id=None, tx_body=True)
        b=b'hello, '
        b2=b'world!'
        blob_writer.append_data(0, b, None)
        blob_writer = sync_filter_adapter.get_blob_writer(
            create=False, blob_rest_id=None, tx_body=True)
        blob_writer.append_data(len(b), b2, len(b) + len(b2))

        for i in range(0,3):
            tx = sync_filter_adapter.get()
            logging.debug(tx)
            if tx is not None and tx.data_response is not None and tx.data_response.code == 203:
                break
            sync_filter_adapter.wait(tx.version, 1)
        else:
            self.fail('expected data response')

        for i in range(0,3):
            sync_filter_adapter.wait(tx.version, 1)
            if sync_filter_adapter.done:
                break
            tx = sync_filter_adapter.get()
        else:
            self.fail('expected done')
        self.assertTrue(sync_filter_adapter.idle(time.time(), 0, 0))

    def test_upstream_filter_exceptions(self):
        upstream = FakeSyncFilter()
        sync_filter_adapter = SyncFilterAdapter(
            self.executor, upstream, 'rest_id')

        def exp(tx,delta):
            raise ValueError()
        upstream.add_expectation(exp)

        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        sync_filter_adapter.update(tx, tx.copy())
        sync_filter_adapter.wait(tx.version, 1)
        upstream_tx = sync_filter_adapter.get()
        self.assertEqual(450, upstream_tx.mail_response.code)
        self.assertIn('unexpected exception', upstream_tx.mail_response.message)

class RestHandlerTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.executor = Executor(inflight_limit=10, watchdog_timeout=5)

    def _headers(self, d : List[Tuple[str,str]]) -> List[Tuple[bytes,bytes]]:
        return [(k.encode('ascii'), v.encode('ascii')) for k,v in d]

    async def test_create_tx(self):
        endpoint = MockAsyncFilter(incremental=True)
        tx = TransactionMetadata()

        def exp(tx, tx_delta):
            self.assertIsNotNone(tx.host)
            delta = TransactionMetadata(rest_id='rest_id',
                                        version=1)
            tx.merge_from(delta)
            return delta
        endpoint.expect_update(exp)

        handler = RestHandler(async_filter=endpoint, http_host='msa',
                              executor=self.executor)
        scope = {'type': 'http',
                 'headers': []}
        req = FastApiRequest(scope)
        resp = await handler.handle_async(
            req, lambda: handler.create_tx(req, req_json={}))
        self.assertEqual(resp.status_code, 201)
        self.assertEqual(resp.body, b'{}')
        self.assertEqual(resp.headers['location'], '/transactions/rest_id')
        logging.debug('test_create_tx create resp %s', resp.headers)
        self.assertIsNotNone(resp.headers.get('etag', None))


        endpoint.expect_get(TransactionMetadata(
            host='msa',
            version=1))
        def exp_mail(tx, tx_delta):
            self.assertIsNotNone(tx.mail_from)
            upstream_delta = TransactionMetadata(version=2)
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        endpoint.expect_update(exp_mail)

        handler = RestHandler(async_filter=endpoint, tx_rest_id='rest_id',
                              http_host='msa',
                              executor=self.executor)
        scope = {'type': 'http',
                 'headers': self._headers([
                     ('if-match', resp.headers['etag']),
                     ('request-timeout', '1')])}
        req = FastApiRequest(scope)
        resp = await handler.handle_async(
            req, lambda: handler.patch_tx(
                req, req_json={"mail_from": {"m": "alice"}}))
        logging.debug('test_create_tx patch tx resp %s', resp.body)
        self.assertEqual(resp.status_code, 200)


        endpoint.expect_get(TransactionMetadata(
            host='msa',
            mail_from=Mailbox('alice'),
            version=2))
        endpoint.expect_get(TransactionMetadata(
            host='msa',
            mail_from=Mailbox('alice'),
            mail_response=Response(201),
            version=3))
        handler = RestHandler(async_filter=endpoint, tx_rest_id='rest_id',
                              http_host='msa',
                              executor=self.executor)

        scope = {'type': 'http',
                 'headers': self._headers(
                     [('if-none-match', '2'),
                      ('request-timeout', '1')])}
        req = FastApiRequest(scope)

        resp = await handler.get_tx_async(req)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(json.loads(resp.body), {
            'mail_from': {},
            'mail_response': {'code': 201, 'message': 'ok'}
        })


        endpoint.expect_get(TransactionMetadata(
            host='msa',
            mail_from=Mailbox('alice'),
            mail_response=Response(201),
            version=3))
        def exp_rcpt(tx, tx_delta):
            self.assertEqual(1, len(tx.rcpt_to))
            upstream_delta = TransactionMetadata(
                version=4)
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        endpoint.expect_update(exp_rcpt)

        handler = RestHandler(async_filter=endpoint, tx_rest_id='rest_id',
                              http_host='msa',
                              executor=self.executor)

        scope = {'type': 'http',
                 'headers': self._headers([
                     ('if-match', resp.headers['etag'])])}
        req = FastApiRequest(scope)
        resp = await handler.handle_async(
            req, lambda: handler.patch_tx(
                req, req_json={"rcpt_to": [{"m": "bob"}]}))
        self.assertEqual(resp.status_code, 200)
        logging.debug('test_create_tx patch tx resp %s', resp.body)
        self.assertEqual(json.loads(resp.body), {
            'mail_from': {},
            'mail_response': {'code': 201, 'message': 'ok'},
            'rcpt_to': [{}]
        })


        endpoint.expect_get(TransactionMetadata(
            host='msa',
            mail_from=Mailbox('alice'),
            mail_response=Response(201),
            rcpt_to=[Mailbox('bob')],
            rcpt_response=[Response(202)],
            version=4))
        scope = {'type': 'http',
                 'headers': []}
        req = FastApiRequest(scope)
        resp = await handler.get_tx_async(req)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(json.loads(resp.body), {
            'mail_from': {},
            'rcpt_to': [{}],
            'mail_response': {'code': 201, 'message': 'ok'},
            'rcpt_response': [{'code': 202, 'message': 'ok'}]
        })


        endpoint.expect_get(TransactionMetadata(
            host='msa',
            mail_from=Mailbox('alice'),
            mail_response=Response(201),
            rcpt_to=[Mailbox('bob')],
            rcpt_response=[Response(202)],
            version=4))
        def exp_rcpt2(tx, tx_delta):
            self.assertEqual(2, len(tx.rcpt_to))
            upstream_delta = TransactionMetadata(
                version=5)
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        endpoint.expect_update(exp_rcpt2)

        handler = RestHandler(
            async_filter=endpoint, tx_rest_id='rest_id', http_host='msa',
            executor=self.executor)
        scope = {'type': 'http',
                 'headers': self._headers([
                     ('if-match', resp.headers['etag'])])}
        req = FastApiRequest(scope)
        resp = await handler.handle_async(
            req, lambda: handler.patch_tx(
                req, req_json={"rcpt_to": [{"m": "bob2"}],
                               "rcpt_to_list_offset": 1}))
        self.assertEqual(resp.status_code, 200)
        logging.debug('test_create_tx patch tx resp %s', resp.body)
        self.assertEqual(json.loads(resp.body), {
            'mail_from': {},
            'rcpt_to': [{}, {}],
            'mail_response': {'code': 201, 'message': 'ok'},
            'rcpt_response': [{'code': 202, 'message': 'ok'}]
        })


        endpoint.expect_get(TransactionMetadata(
            host='msa',
            mail_from=Mailbox('alice'),
            mail_response=Response(201),
            rcpt_to=[Mailbox('bob'), Mailbox('bob2')],
            rcpt_response=[Response(202), Response(203)],
            version=6))

        scope = {'type': 'http',
                 'headers': []}
        req = FastApiRequest(scope)
        resp = await handler.get_tx_async(req)
        self.assertEqual(json.loads(resp.body), {
            'mail_from': {},
            'rcpt_to': [{}, {}],
            'mail_response': {'code': 201, 'message': 'ok'},
            'rcpt_response': [{'code': 202, 'message': 'ok'},
                              {'code': 203, 'message': 'ok'}]
        })
        tx_etag = resp.headers['etag']


        endpoint.body = InlineBlob(b'')
        body = b'hello, world!'
        async def input():
            return {'type': 'http.request',
                    'body': body,
                    'more_body': False}
        endpoint.body = InlineBlob(b'')
        handler = RestHandler(
            async_filter=endpoint,
            http_host='msa',
            rest_id_factory = lambda: 'blob-rest-id',
            tx_rest_id='rest_id',
            executor=self.executor,
            chunk_size=8)

        scope = {'type': 'http',
                 'headers': self._headers([
                     ('content-length', str(len(body)))])}
        req = FastApiRequest(scope, input)
        resp = await handler.put_blob_async(req, tx_body=True)
        logging.debug('test_create_tx create blob resp %s', resp.body)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(endpoint.body.d, body)
        self.assertEqual(endpoint.body.content_length(), len(body))


        endpoint.expect_get(TransactionMetadata(
            host='msa',
            mail_from=Mailbox('alice'),
            mail_response=Response(201),
            rcpt_to=[Mailbox('bob'), Mailbox('bob2')],
            rcpt_response=[Response(202), Response(203)],
            body=InlineBlob(body, last=True),
            version=7))

        scope = {'type': 'http',
                 'headers': []}
        req = FastApiRequest(scope)
        resp = await handler.get_tx_async(req)
        self.assertEqual(json.loads(resp.body), {
            'mail_from': {},
            'rcpt_to': [{}, {}],
            'body': {},
            'mail_response': {'code': 201, 'message': 'ok'},
            'rcpt_response': [{'code': 202, 'message': 'ok'},
                              {'code': 203, 'message': 'ok'}],
        })

        endpoint.expect_get(TransactionMetadata(
            host='msa',
            mail_from=Mailbox('alice'),
            mail_response=Response(201),
            rcpt_to=[Mailbox('bob'), Mailbox('bob2')],
            rcpt_response=[Response(202), Response(203)],
            body=InlineBlob(body, last=True),
            data_response=Response(204),
            version=8))

        scope = {'type': 'http',
                 'headers': []}
        req = FastApiRequest(scope)
        resp = await handler.get_tx_async(req)
        self.assertEqual(json.loads(resp.body), {
            'mail_from': {},
            'rcpt_to': [{}, {}],
            'body': {},
            'mail_response': {'code': 201, 'message': 'ok'},
            'rcpt_response': [{'code': 202, 'message': 'ok'},
                              {'code': 203, 'message': 'ok'}],
            'data_response': {'code': 204, 'message': 'ok'},
        })


    def assert_eq_content_range(self, r1, r2):
        self.assertEqual(r1.units, r2.units)
        self.assertEqual(r1.start, r2.start)
        self.assertEqual(r1.stop, r2.stop)
        self.assertEqual(r1.length, r2.length)

    async def test_blob_chunking(self):
        endpoint = MockAsyncFilter(incremental=True)

        endpoint.body = InlineBlob(b'')
        handler = RestHandler(
            async_filter=endpoint,
            http_host='msa',
            #rest_id_factory = lambda: 'blob-rest-id',
            tx_rest_id='tx_rest_id',
            executor=self.executor)
        scope = {'type': 'http',
                 'headers': []}
        req = FastApiRequest(scope)

        handler = RestHandler(
            async_filter=endpoint, blob_rest_id='blob-rest-id',
            http_host='msa',
            rest_id_factory = lambda: 'rest-id',
            tx_rest_id='tx_rest_id',
            executor=self.executor)

        b = b'hello, '
        range = ContentRange('bytes', 0, len(b))
        scope = {'type': 'http',
                 'headers': self._headers([
                     ('content-length', str(len(b))),
                     ('content-range', str(range))])}
        async def input():
            return {'type': 'http.request',
                    'body': b,
                    'more_body': False}
        req = FastApiRequest(scope, input)
        resp = await handler.put_blob_async(req, 'blob-rest-id')
        self.assertEqual(resp.status_code, 200)
        self.assert_eq_content_range(
            parse_content_range_header(
                resp.headers['content-range']),
            ContentRange('bytes', 0, 7, None))


        handler = RestHandler(
            async_filter=endpoint, blob_rest_id='blob-rest-id',
            http_host='msa',
            rest_id_factory = lambda: 'rest-id',
            tx_rest_id='tx_rest_id',
            executor=self.executor)
        b2 = b'world!'
        range = ContentRange('bytes', len(b), len(b) + len(b2),
                             len(b) + len(b2))
        scope = {'type': 'http',
                 'headers': self._headers([
                     ('content-length', str(len(b2))),
                     ('content-range', str(range))])}
        async def input():
            return {'type': 'http.request',
                    'body': b2,
                    'more_body': False}
        req = FastApiRequest(scope, input)
        resp = await handler.put_blob_async(req, 'blob-rest-id')
        self.assertEqual(resp.status_code, 200)
        self.assert_eq_content_range(
            parse_content_range_header(resp.headers['content-range']),
            ContentRange('bytes', 0, 13, 13))


    # transfer-encoding: chunked
    # i.e. no content-length header
    async def test_chunked_blob(self):
        endpoint = MockAsyncFilter(incremental=True)

        endpoint.body = InlineBlob(b'')
        handler = RestHandler(
            async_filter=endpoint,
            http_host='msa',
            tx_rest_id='tx_rest_id',
            executor=self.executor)
        scope = {'type': 'http',
                 'headers': []}
        req = FastApiRequest(scope)

        handler = RestHandler(
            async_filter=endpoint, blob_rest_id='blob-rest-id',
            http_host='msa',
            rest_id_factory = lambda: 'rest-id',
            tx_rest_id='tx_rest_id',
            executor=self.executor)

        b = b'hello, '
        b2 = b'world!'

        range = ContentRange('bytes', 0, len(b))
        scope = {'type': 'http',
                 'headers': self._headers([])}

        chunks = [
            {'type': 'http.request',
             'body': b,
             'more_body': True},
            {'type': 'http.request',
             'body': b2,
             'more_body': False}]
        async def input():
            return chunks.pop(0)

        req = FastApiRequest(scope, input)
        resp = await handler.put_blob_async(req, 'blob-rest-id')
        self.assertEqual(resp.status_code, 200)
        #self.assertNotIn('content-range', resp.headers)

        self.assertEqual(b+b2, endpoint.body.d)


    async def _test_uri_qualification(
            self,
            session_uri : Optional[str], service_uri : Optional[str],
            rest_lro : bool):
        endpoint = MockAsyncFilter(incremental=True)

        handler = RestHandler(
            async_filter=endpoint,
            executor=self.executor,
            blob_rest_id='blob-rest-id',
            http_host='msa',
            rest_id_factory = lambda: 'rest-id',
            tx_rest_id='tx_rest_id',
            session_uri=session_uri,
            service_uri=service_uri,
            endpoint_yaml={'rest_lro': rest_lro})

        scope = {'type': 'http',
                 'headers': self._headers([])}
        req = FastApiRequest(scope)

        def exp(tx, tx_delta):
            delta = TransactionMetadata(rest_id='rest_id',
                                        version=1)
            tx.merge_from(delta)
            return delta
        endpoint.expect_update(exp)

        resp = await handler.handle_async(
            req, lambda: handler.create_tx(req, req_json={}))
        logging.debug(resp.headers)
        return resp.headers['location']

    async def test_uri_qualification(self):
        location = await self._test_uri_qualification(
            'http://0.router', 'http://router', rest_lro=True)
        self.assertTrue(location.startswith('http://router'))
        location = await self._test_uri_qualification(
            'http://0.router', 'http://router', rest_lro=False)
        self.assertTrue(location.startswith('http://0.router'))
        location = await self._test_uri_qualification(
            None, None, rest_lro=False)
        self.assertTrue(location.startswith('/transactions'))


    async def _test_get_redirect(self, session_uri : Optional[str] = None
                                 ) -> FastApiResponse:
        endpoint = MockAsyncFilter(incremental=True)

        handler = RestHandler(
            async_filter=endpoint,
            executor=self.executor,
            blob_rest_id='blob-rest-id',
            http_host='msa',
            rest_id_factory = lambda: 'rest-id',
            tx_rest_id='tx_rest_id',
            session_uri='http://0.router',
            service_uri='http://router')

        scope = {'type': 'http',
                 'headers': self._headers([('if-none-match', '1'),
                                           ('request-timeout', '5')])}
        req = FastApiRequest(scope)

        tx = TransactionMetadata(rest_id='rest_id',
                                 version=1)
        tx.session_uri = session_uri
        endpoint.expect_get(tx)

        return await handler.get_tx_async(req)

    async def test_get_redirect(self):
        resp = await self._test_get_redirect()
        self.assertEqual(304, resp.status_code)  # no change
        self.assertNotIn('location', resp.headers)
        resp = await self._test_get_redirect('http://1.router')
        self.assertEqual(307, resp.status_code)
        self.assertTrue(resp.headers['location'].startswith('http://1.router'))

    async def test_ping_session(self):
        endpoint = MockAsyncFilter(incremental=True)

        mu = Lock()
        cv = Condition(mu)

        def client(u):
            logging.debug(u)
            with mu:
                self.uri = u
                cv.notifyAll()
            return HttpxResponse(200)

        handler = RestHandler(
            async_filter=endpoint,
            executor=self.executor,
            blob_rest_id='blob-rest-id',
            http_host='msa',
            rest_id_factory = lambda: 'rest-id',
            tx_rest_id='tx_rest_id',
            session_uri='http://0.router',
            service_uri='http://router',
            client=client)

        scope = {'type': 'http',
                 'headers': self._headers([('if-match', '1'),
                                           ('request-timeout', '5')])}
        req = FastApiRequest(scope)

        tx = TransactionMetadata(rest_id='rest_id',
                                 version=1)
        tx.session_uri = 'http://127.0.0.1:12345'
        endpoint.expect_get(tx)

        def exp(tx, tx_delta):
            delta = TransactionMetadata(rest_id='rest_id',
                                        version=1)
            delta.session_uri='http://127.0.0.1:12345'
            tx.merge_from(delta)
            return delta
        endpoint.expect_update(exp)

        resp = await handler.handle_async(
            req, lambda: handler.patch_tx(req, req_json={
                'mail_from': {'m': 'alice@example.com'}}))

        with mu:
            self.assertTrue(cv.wait_for(lambda: self.uri is not None, 1))
        logging.debug('%s %s', resp.status_code, resp.body)
        self.assertEqual('http://127.0.0.1:12345/transactions/tx_rest_id',
                         self.uri)

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d %(message)s')
    unittest.main()
