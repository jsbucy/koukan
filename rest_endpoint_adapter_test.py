from typing import List, Tuple
import unittest
import logging
import io
from threading import Thread
import json

from flask import (
    Flask,
    Request as FlaskRequest )
from werkzeug.datastructures import ContentRange
from werkzeug.http import parse_content_range_header

from fastapi import (
    Request as FastApiRequest,
    Response as FastApiResponse )

from blob import InlineBlob
from rest_endpoint_adapter import RestHandler, SyncFilterAdapter
from fake_endpoints import FakeAsyncEndpoint, FakeSyncFilter
from executor import Executor
from filter import Mailbox, TransactionMetadata, WhichJson
from response import Response
from executor import Executor

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
        sync_filter_adapter.update(tx, tx.copy(), 1)
        self.assertEqual(tx.mail_response.code, 201)

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
        sync_filter_adapter.update(tx, delta, 1)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])

        tx = sync_filter_adapter.get(1)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])
        self.assertFalse(sync_filter_adapter.done)

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
        sync_filter_adapter.update(tx, delta, 1)
        self.assertTrue(sync_filter_adapter.done)

class RestHandlerTest(unittest.TestCase):
    def test_create_tx(self):
        app = Flask(__name__)
        endpoint = FakeAsyncEndpoint(rest_id='rest_id')
        tx = TransactionMetadata()
        tx.rest_id = 'rest_id'
        endpoint.merge(tx)

        with app.test_request_context():
            handler = RestHandler(async_filter=endpoint, http_host='msa')
            resp = handler.create_tx(FlaskRequest.from_values(),
                                     req_json={})
            self.assertEqual(resp.status, '201 CREATED')
            self.assertEqual(resp.json, {})
            self.assertEqual(resp.headers['location'], '/transactions/rest_id')

            handler = RestHandler(async_filter=endpoint, tx_rest_id='rest_id',
                                          http_host='msa')
            resp = handler.patch_tx(
                FlaskRequest.from_values(
                    headers={'if-match': resp.headers['etag'],
                             'request-timeout': '1'}),
                req_json={"mail_from": {"m": "alice"}})
            self.assertEqual(resp.status, '200 OK')

            endpoint.merge(
                TransactionMetadata(mail_response=Response(201)))
            handler = RestHandler(async_filter=endpoint, tx_rest_id='rest_id',
                                          http_host='msa')
            resp = handler.get_tx(FlaskRequest.from_values())
            self.assertEqual(resp.json, {
                'mail_from': {},
                'mail_response': {'code': 201, 'message': 'ok'}
            })

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

            endpoint.merge(TransactionMetadata(rcpt_response=[Response(202)]))

            resp = handler.get_tx(FlaskRequest.from_values())
            self.assertEqual(resp.json, {
                'mail_from': {},
                'rcpt_to': [{}],
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'}]
            })


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

            tx = TransactionMetadata.from_json(resp.json, WhichJson.REST_READ)
            updated = tx.copy()
            updated.rcpt_response.append(Response(203))
            delta = tx.delta(updated)
            tx = updated

            endpoint.merge(delta)
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

            endpoint.merge(TransactionMetadata(
                body_blob=endpoint.body_blob,  # XXX
                data_response=Response(204)))
            resp = handler.get_tx(FlaskRequest.from_values())
            self.assertEqual(resp.json, {
                'mail_from': {},
                'rcpt_to': [{}, {}],
                'body': {},
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'},
                                  {'code': 203, 'message': 'ok'}],
                'data_response': {'code': 204, 'message': 'ok'},
            })
            tx_etag = resp.headers['etag']

            # XXX why the second GET here?
            handler = RestHandler(async_filter=endpoint, tx_rest_id='rest_id',
                                  http_host='msa')
            endpoint.merge(TransactionMetadata(data_response=Response(204)))
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
        endpoint = FakeAsyncEndpoint(rest_id='rest-id')
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

            endpoint.body_blob = InlineBlob(b'')
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
                async_filter=endpoint, blob_rest_id='blob-rest-id',
                http_host='msa',
                rest_id_factory = lambda: 'rest-id',
                tx_rest_id='tx_rest_id')
            b = b'hello, '
            range = ContentRange('bytes', 0, len(b))
            resp = handler.put_blob(
                FlaskRequest.from_values(
                    data=b,  # for content-length
                    headers={'content-range': range}),
                b)  # actually read from here
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
                b2)  # actually read from here
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
        endpoint = FakeAsyncEndpoint(rest_id='rest_id')
        tx = TransactionMetadata()
        tx.rest_id = 'rest_id'
        endpoint.merge(tx)

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
        logging.debug('%s', resp.headers)
        self.assertIsNotNone(resp.headers.get('etag', None))

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
        logging.debug(resp.body)
        self.assertEqual(resp.status_code, 200)


        endpoint.merge(
            TransactionMetadata(mail_response=Response(201)))
        handler = RestHandler(async_filter=endpoint, tx_rest_id='rest_id',
                              http_host='msa',
                              executor=self.executor)

        scope = {'type': 'http',
                 'headers': []}
        req = FastApiRequest(scope)
        resp = await handler.handle_async(req, lambda: handler.get_tx(req))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(json.loads(resp.body), {
            'mail_from': {},
            'mail_response': {'code': 201, 'message': 'ok'}
        })

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
        logging.debug('%s', resp.body)
        self.assertEqual(json.loads(resp.body), {
            'mail_from': {},
            'mail_response': {'code': 201, 'message': 'ok'},
            'rcpt_to': [{}]
        })

        endpoint.merge(TransactionMetadata(rcpt_response=[Response(202)]))


        scope = {'type': 'http',
                 'headers': []}
        req = FastApiRequest(scope)
        resp = await handler.handle_async(req, lambda: handler.get_tx(req))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(json.loads(resp.body), {
            'mail_from': {},
            'rcpt_to': [{}],
            'mail_response': {'code': 201, 'message': 'ok'},
            'rcpt_response': [{'code': 202, 'message': 'ok'}]
        })


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
        logging.debug('%s', resp.body)
        self.assertEqual(json.loads(resp.body), {
            'mail_from': {},
            'rcpt_to': [{}, {}],
            'mail_response': {'code': 201, 'message': 'ok'},
            'rcpt_response': [{'code': 202, 'message': 'ok'}]
        })

        tx = TransactionMetadata.from_json(
            json.loads(resp.body), WhichJson.REST_READ)
        updated = tx.copy()
        updated.rcpt_response.append(Response(203))
        delta = tx.delta(updated)
        tx = updated

        endpoint.merge(delta)
        scope = {'type': 'http',
                 'headers': []}
        req = FastApiRequest(scope)
        resp = await handler.handle_async(
            req, lambda: handler.get_tx(req))
        self.assertEqual(json.loads(resp.body), {
            'mail_from': {},
            'rcpt_to': [{}, {}],
            'mail_response': {'code': 201, 'message': 'ok'},
            'rcpt_response': [{'code': 202, 'message': 'ok'},
                              {'code': 203, 'message': 'ok'}]
        })
        tx_etag = resp.headers['etag']

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
        logging.debug(resp.body)
        self.assertEqual(resp.status_code, 201)
        self.assertEqual(endpoint.body_blob.d, body)
        self.assertEqual(endpoint.body_blob.content_length(), len(body))

        endpoint.merge(TransactionMetadata(
            body_blob=endpoint.body_blob,  # XXX Filter api postcondition?
            data_response=Response(204)))
        scope = {'type': 'http',
                 'headers': []}
        req = FastApiRequest(scope)
        resp = await handler.handle_async(req, lambda: handler.get_tx(req))
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
        endpoint = FakeAsyncEndpoint(rest_id='rest-id')
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
            rest_id_factory = lambda: 'blob-rest-id',
            tx_rest_id='tx_rest_id',
            executor=self.executor)
        scope = {'type': 'http',
                 'headers': []}
        req = FastApiRequest(scope)

        resp = await handler.create_blob_async(
            req, req_upload='chunked')
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
