from typing import List, Tuple
import unittest
import logging
import io
import json

from flask import (
    Flask,
    Request as FlaskRequest )
from werkzeug.datastructures import ContentRange
from werkzeug.http import parse_content_range_header

from fastapi import (
    Request as FastApiRequest,
    Response as FastApiResponse )

from koukan.blob import InlineBlob
from koukan.rest_endpoint_adapter import RestHandler, SyncFilterAdapter
from koukan.fake_endpoints import FakeSyncFilter, MockAsyncFilter
from koukan.executor import Executor
from koukan.filter import Mailbox, TransactionMetadata, WhichJson
from koukan.response import Response
from koukan.executor import Executor

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
            self.assertEqual(tx.body_blob.read(0), body)
            upstream_delta=TransactionMetadata(
                data_response = [Response(203)])
            self.assertIsNotNone(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream.add_expectation(exp_body)

        delta = TransactionMetadata(body_blob=InlineBlob(body))
        tx.merge_from(delta)
        sync_filter_adapter.update(tx, delta)
        for i in range(0,3):
            sync_filter_adapter.wait(tx.version, 1)
            if sync_filter_adapter.done:
                break
            tx = sync_filter_adapter.get()
        else:
            self.fail('expected done')

class RestHandlerTest(unittest.TestCase):
    def test_create_tx(self):
        app = Flask(__name__)
        endpoint = MockAsyncFilter()
        tx = TransactionMetadata()

        with app.test_request_context():
            def exp(tx, tx_delta):
                self.assertIsNotNone(tx.host)
                delta = TransactionMetadata(rest_id='rest_id',
                                            version=1)
                tx.merge_from(delta)
                return delta
            endpoint.expect_update(exp)
            handler = RestHandler(async_filter=endpoint, http_host='msa')
            resp = handler.create_tx(FlaskRequest.from_values(),
                                     req_json={})
            self.assertEqual(resp.status, '201 CREATED')
            self.assertEqual(resp.json, {})
            self.assertEqual(resp.headers['location'], '/transactions/rest_id')


            endpoint.expect_get(TransactionMetadata(
                host='msa',
                version=1))
            def exp_mail(tx, tx_delta):
                self.assertIsNotNone(tx.mail_from)
                upstream_delta = TransactionMetadata(
                    version=2)
                assert tx.merge_from(upstream_delta) is not None
                return upstream_delta
            endpoint.expect_update(exp_mail)

            handler = RestHandler(async_filter=endpoint, tx_rest_id='rest_id',
                                  http_host='msa')
            resp = handler.patch_tx(
                FlaskRequest.from_values(
                    headers={'if-match': resp.headers['etag'],
                             'request-timeout': '1'}),
                req_json={"mail_from": {"m": "alice"}})
            self.assertEqual(resp.status, '200 OK')

            handler = RestHandler(async_filter=endpoint, tx_rest_id='rest_id',
                                          http_host='msa')
            endpoint.expect_get(TransactionMetadata(
                host='msa',
                mail_from=Mailbox('alice'),
                mail_response=Response(201),
                version=3))
            resp = handler.get_tx(FlaskRequest.from_values())
            self.assertEqual(resp.json, {
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
                                          http_host='msa')

            resp = handler.patch_tx(
                FlaskRequest.from_values(
                    headers={'if-match': resp.headers['etag']}),
                req_json={"rcpt_to": [{"m": "bob"}]})
            self.assertEqual(resp.status, '200 OK')
            logging.debug('%s', resp.json)
            self.assertEqual(resp.json, {
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
            resp = handler.get_tx(FlaskRequest.from_values())
            self.assertEqual(resp.json, {
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
                async_filter=endpoint, tx_rest_id='rest_id', http_host='msa')
            resp = handler.patch_tx(
                FlaskRequest.from_values(
                    headers={'if-match': resp.headers['etag']}),
                req_json={"rcpt_to": [{"m": "bob2"}],
                          "rcpt_to_list_offset": 1})
            self.assertEqual(resp.status, '200 OK')
            logging.debug('%s', resp.json)
            self.assertEqual(resp.json, {
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

            resp = handler.get_tx(FlaskRequest.from_values())
            self.assertEqual(resp.json, {
                'mail_from': {},
                'rcpt_to': [{}, {}],
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'},
                                  {'code': 203, 'message': 'ok'}]
            })
            tx_etag = resp.headers['etag']


            body = b'hello, world!'
            endpoint.body_blob = InlineBlob(b'')
            handler = RestHandler(
                async_filter=endpoint,
                http_host='msa',
                rest_id_factory = lambda: 'blob-rest-id',
                tx_rest_id='rest_id')
            resp = handler.create_blob(
                FlaskRequest.from_values(data=body),
                tx_body=True)
            logging.debug(resp.response)
            self.assertEqual(resp.status, '201 CREATED')
            self.assertEqual(endpoint.body_blob.d, body)
            self.assertEqual(endpoint.body_blob.content_length(), len(body))


            endpoint.expect_get(TransactionMetadata(
                host='msa',
                mail_from=Mailbox('alice'),
                mail_response=Response(201),
                rcpt_to=[Mailbox('bob'), Mailbox('bob2')],
                rcpt_response=[Response(202), Response(203)],
                body='',  # placeholder
                version=7))
            resp = handler.get_tx(FlaskRequest.from_values())
            self.assertEqual(resp.json, {
                'mail_from': {},
                'rcpt_to': [{}, {}],
                'body': {},
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'},
                                  {'code': 203, 'message': 'ok'}],
            })
            tx_etag = resp.headers['etag']

            endpoint.expect_get(TransactionMetadata(
                host='msa',
                mail_from=Mailbox('alice'),
                mail_response=Response(201),
                rcpt_to=[Mailbox('bob'), Mailbox('bob2')],
                rcpt_response=[Response(202), Response(203)],
                body='',  # placeholder
                data_response=Response(204),
                version=8))

            handler = RestHandler(async_filter=endpoint, tx_rest_id='rest_id',
                                  http_host='msa')
            resp = handler.get_tx(FlaskRequest.from_values())
            self.assertEqual(resp.status, '200 OK')
            logging.debug('RestHandlerTest get %s', resp.json)
            self.assertEqual(resp.json, {
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

    def test_blob_chunking(self):
        endpoint = MockAsyncFilter() 
        app = Flask(__name__)

        with app.test_request_context():
            # content-range header is not accepted in non-chunked blob post
            handler = RestHandler(
                async_filter=endpoint,
                http_host='msa',
                rest_id_factory = lambda: 'blob-rest-id',
                tx_rest_id='tx_rest_id')
            resp = handler.create_blob(
                FlaskRequest.from_values(
                    headers={'content-range': ContentRange('bytes', 0,10,10)}))
            self.assertEqual(resp.status, '400 BAD REQUEST')

            # POST ...blob...?upload=chunked may eventually take
            # json metadata as the entity but for now that's unimplemented
            handler = RestHandler(
                async_filter=endpoint,
                http_host='msa',
                rest_id_factory = lambda: 'blob-rest-id',
                tx_rest_id='tx_rest_id')
            resp = handler.create_blob(
                FlaskRequest.from_values(
                    query_string={'upload': 'chunked'},
                    data='unimplemented params'),
                req_upload='chunked')
            self.assertEqual(resp.status, '400 BAD REQUEST')

            endpoint.blob['blob-rest-id'] = InlineBlob(b'')
            handler = RestHandler(
                async_filter=endpoint,
                http_host='msa',
                rest_id_factory = lambda: 'blob-rest-id',
                tx_rest_id='tx_rest_id')
            resp = handler.create_blob(
                FlaskRequest.from_values(
                    query_string={'upload': 'chunked'}),
                req_upload='chunked')
            self.assertEqual(resp.status, '201 CREATED')
            self.assertNotIn('content-range', resp.headers)

            handler = RestHandler(
                async_filter=endpoint,
                blob_rest_id='blob-rest-id',
                http_host='msa',
                rest_id_factory = lambda: 'rest-id',
                tx_rest_id='tx_rest_id')
            b = b'hello, '
            range = ContentRange('bytes', 0, len(b))
            resp = handler.put_blob(
                FlaskRequest.from_values(
                    data=b,  # for content-length
                    headers={'content-range': range}),
                'blob-rest-id')
            self.assertEqual(resp.status, '200 OK')
            self.assert_eq_content_range(
                resp.content_range,
                ContentRange('bytes', 0, 7, None))

            handler = RestHandler(
                async_filter=endpoint, blob_rest_id='blob-rest-id',
                http_host='msa',
                rest_id_factory = lambda: 'rest-id',
                tx_rest_id='tx_rest_id')
            b2 = b'world!'
            range = ContentRange('bytes', len(b), len(b) + len(b2),
                                 len(b) + len(b2))
            resp = handler.put_blob(
                FlaskRequest.from_values(
                    data=b2,  # for content-length
                    headers={'content-range': range}),
                'blob-rest-id')
            self.assertEqual(resp.status, '200 OK')
            self.assert_eq_content_range(resp.content_range,
                                         ContentRange('bytes', 0, 13, 13))



class RestHandlerAsyncTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.executor = Executor(inflight_limit=10, watchdog_timeout=5)

    def _headers(self, d : List[Tuple[str,str]]) -> List[Tuple[bytes,bytes]]:
        return [(k.encode('ascii'), v.encode('ascii')) for k,v in d]

    async def test_create_tx(self):
        app = Flask(__name__)
        endpoint = MockAsyncFilter()
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
            mail_response=Response(201),
            version=3))
        handler = RestHandler(async_filter=endpoint, tx_rest_id='rest_id',
                              http_host='msa',
                              executor=self.executor)

        scope = {'type': 'http',
                 'headers': []}
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


        endpoint.body_blob = InlineBlob(b'')
        body = b'hello, world!'
        async def input():
            return {'type': 'http.request',
                    'body': body,
                    'more_body': False}
        endpoint.body_blob = InlineBlob(b'')
        handler = RestHandler(
            async_filter=endpoint,
            http_host='msa',
            rest_id_factory = lambda: 'blob-rest-id',
            tx_rest_id='rest_id',
            executor=self.executor)

        scope = {'type': 'http',
                 'headers': self._headers([
                     ('content-length', str(len(body)))])}
        req = FastApiRequest(scope, input)
        resp = await handler.create_blob_async(req, tx_body=True)
        logging.debug('test_create_tx create blob resp %s', resp.body)
        self.assertEqual(resp.status_code, 201)
        self.assertEqual(endpoint.body_blob.d, body)
        self.assertEqual(endpoint.body_blob.content_length(), len(body))


        endpoint.expect_get(TransactionMetadata(
            host='msa',
            mail_from=Mailbox('alice'),
            mail_response=Response(201),
            rcpt_to=[Mailbox('bob'), Mailbox('bob2')],
            rcpt_response=[Response(202), Response(203)],
            body='',  # placeholder
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
            body='',  # placeholder
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
        endpoint = MockAsyncFilter()
        app = Flask(__name__)

        # content-range header is not accepted in non-chunked blob post
        handler = RestHandler(
            async_filter=endpoint,
            http_host='msa',
            rest_id_factory = lambda: 'blob-rest-id',
            tx_rest_id='tx_rest_id',
            executor=self.executor)

        scope = {'type': 'http',
                 'headers': self._headers([
                     ('content-range', str(ContentRange('bytes', 0,10,10)))])}
        req = FastApiRequest(scope)

        resp = await handler.create_blob_async(req)
        self.assertEqual(resp.status_code, 400)

        # POST ...blob...?upload=chunked may eventually take
        # json metadata as the entity but for now that's unimplemented
        handler = RestHandler(
            async_filter=endpoint,
            http_host='msa',
            rest_id_factory = lambda: 'blob-rest-id',
            tx_rest_id='tx_rest_id',
            executor=self.executor)
        scope = {'type': 'http',
                 'headers': [(b'content-length', b'23')]}
        async def input():
            return {'type': 'http.request',
                    'body': 'unimplemented params',
                    'more_body': False}
        req = FastApiRequest(scope, input)
        resp = await handler.create_blob_async(
            req, req_upload='chunked')
        self.assertEqual(resp.status_code, 400)


        endpoint.body_blob = InlineBlob(b'')
        handler = RestHandler(
            async_filter=endpoint,
            http_host='msa',
            #rest_id_factory = lambda: 'blob-rest-id',
            tx_rest_id='tx_rest_id',
            executor=self.executor)
        scope = {'type': 'http',
                 'headers': []}
        req = FastApiRequest(scope)

        resp = await handler.create_blob_async(
            req, tx_body=True, req_upload='chunked')
        logging.debug(resp.body)
        self.assertEqual(resp.status_code, 201)
        self.assertNotIn('content-range', resp.headers)


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





if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s [%(thread)d] %(message)s')
    unittest.main()