import unittest
import logging
import io
from threading import Thread

from flask import (
    Flask,
    Request as FlaskRequest )
from werkzeug.datastructures import ContentRange

from rest_endpoint_adapter import RestEndpointAdapter, SyncFilterAdapter
from fake_endpoints import SyncEndpoint, FakeAsyncEndpoint
from executor import Executor
from filter import Mailbox, TransactionMetadata
from response import Response
from blobs import InMemoryBlobStorage


class SyncFilterAdapterTest(unittest.TestCase):
    def setUp(self):
        self.executor = Executor(inflight_limit=10, watchdog_timeout=5)

    def tearDown(self):
        self.executor.shutdown(timeout=5)

    def test_basic(self):
        sync_endpoint = SyncEndpoint()
        sync_filter_adapter = SyncFilterAdapter(
            self.executor, sync_endpoint, 'rest_id')

        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        sync_filter_adapter.update(tx, 1)
        self.assertIsNotNone(tx.mail_from)

        tx = TransactionMetadata(rcpt_to=[Mailbox('bob')])
        sync_filter_adapter.update(tx, 1)
        self.assertIsNotNone(tx.mail_from)
        self.assertEqual(len(tx.rcpt_to), 1)

        sync_endpoint.set_mail_response(Response(201))
        tx = sync_filter_adapter.get(1)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual(len(tx.rcpt_response), 0)

        sync_endpoint.add_rcpt_response(Response(202))
        tx = sync_filter_adapter.get(1)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])




class RestEndpointAdapterTest(unittest.TestCase):
    def test_create_tx(self):
        app = Flask(__name__)
        endpoint = FakeAsyncEndpoint(rest_id='rest-id')
        endpoint.rest_id = 'rest_id'
        blob_storage = InMemoryBlobStorage()

        with app.test_request_context():
            handler = RestEndpointAdapter(endpoint, http_host='msa')
            resp = handler.create_tx(FlaskRequest.from_values(json={}))
            self.assertEqual(resp.status, '201 CREATED')
            self.assertEqual(resp.json, {})

            handler = RestEndpointAdapter(endpoint, tx_rest_id='rest_id',
                                          http_host='msa')
            resp = handler.patch_tx(FlaskRequest.from_values(
                json={"mail_from": {"m": "alice"}},
                headers={'if-match': resp.headers['etag'],
                         'request-timeout': '1'}))
            self.assertEqual(resp.status, '200 OK')

            endpoint.merge(
                TransactionMetadata(mail_response=Response(201)))
            handler = RestEndpointAdapter(endpoint, tx_rest_id='rest_id',
                                          http_host='msa')
            resp = handler.get_tx(FlaskRequest.from_values(json={}))
            self.assertEqual(resp.json, {
                'mail_response': {'code': 201, 'message': 'ok'}
            })

            handler = RestEndpointAdapter(endpoint, tx_rest_id='rest_id',
                                          http_host='msa')

            resp = handler.patch_tx(FlaskRequest.from_values(
                json={"rcpt_to": [{"m": "bob"}]},
                headers={'if-match': resp.headers['etag']}))
            self.assertEqual(resp.status, '200 OK')
            logging.debug('%s', resp.json)
            self.assertEqual(resp.json, {
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{}]
            })

            endpoint.merge(TransactionMetadata(rcpt_response=[Response(202)]))

            resp = handler.get_tx(FlaskRequest.from_values(json={}))
            self.assertEqual(resp.json, {
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'}]
            })


            handler = RestEndpointAdapter(endpoint, tx_rest_id='rest_id',
                                          http_host='msa')
            resp = handler.patch_tx(FlaskRequest.from_values(
                json={"rcpt_to": [{"m": "bob2"}]},
                headers={'if-match': resp.headers['etag']}))
            self.assertEqual(resp.status, '200 OK')
            logging.debug('%s', resp.json)
            self.assertEqual(resp.json, {
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'},
                                  {}]
            })

            endpoint.merge(TransactionMetadata(rcpt_response=[Response(203)]))
            resp = handler.get_tx(FlaskRequest.from_values(json={}))
            self.assertEqual(resp.json, {
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'},
                                  {'code': 203, 'message': 'ok'}]
            })
            tx_etag = resp.headers['etag']

            handler = RestEndpointAdapter(
                endpoint,
                http_host='msa',
                blob_storage=blob_storage,
                rest_id_factory = lambda: 'blob-rest-id')
            resp = handler.create_blob(FlaskRequest.from_values(
                data='hello, world!'))
            logging.debug(resp.response)
            self.assertEqual(resp.status, '201 CREATED')
            self.assertEqual(handler.blob_rest_id(), 'blob-rest-id')

            # patch body into tx
            handler = RestEndpointAdapter(endpoint, tx_rest_id='rest_id',
                                          http_host='msa',
                                          blob_storage=blob_storage)
            resp = handler.patch_tx(FlaskRequest.from_values(
                json={'body': '/blob/blob-rest-id'},
                headers={'if-match': tx_etag}))
            self.assertEqual(resp.status, '200 OK')
            logging.debug('RestEndpointAdapterTest patch body %s', resp.json)
            self.assertEqual(resp.json, {
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'},
                                  {'code': 203, 'message': 'ok'}],
                #xxx should be populated
                'data_response': {}
            })

            endpoint.merge(TransactionMetadata(data_response=Response(204)))
            resp = handler.get_tx(FlaskRequest.from_values(json={}))
            self.assertEqual(resp.json, {
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'},
                                  {'code': 203, 'message': 'ok'}],
                'data_response': {'code': 204, 'message': 'ok'},
            })
            tx_etag = resp.headers['etag']


            handler = RestEndpointAdapter(endpoint, tx_rest_id='rest_id',
                                          http_host='msa',
                                          blob_storage=blob_storage)
            endpoint.merge(TransactionMetadata(data_response=Response(204)))
            resp = handler.get_tx(FlaskRequest.from_values(json={}))
            self.assertEqual(resp.status, '200 OK')
            logging.debug('RestEndpointAdapterTest get %s', resp.json)
            self.assertEqual(resp.json, {
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
        blob_storage = InMemoryBlobStorage()

        with app.test_request_context():
            # content-range header is not accepted in non-chunked blob post
            handler = RestEndpointAdapter(
                endpoint,
                http_host='msa',
                blob_storage=blob_storage,
                rest_id_factory = lambda: 'blob-rest-id')
            resp = handler.create_blob(
                FlaskRequest.from_values(
                    headers={'content-range': ContentRange('bytes', 0,10,10)}))
            self.assertEqual(resp.status, '400 BAD REQUEST')

            # POST /blob?upload=chunked may eventually take
            # json metadata as the entity but for now that's unimplemented
            handler = RestEndpointAdapter(
                endpoint,
                http_host='msa',
                blob_storage=blob_storage,
                rest_id_factory = lambda: 'blob-rest-id')
            resp = handler.create_blob(
                FlaskRequest.from_values(
                    query_string={'upload': 'chunked'},
                    data='unimplemented params'))
            self.assertEqual(resp.status, '400 BAD REQUEST')

            handler = RestEndpointAdapter(
                endpoint,
                http_host='msa',
                blob_storage=blob_storage,
                rest_id_factory = lambda: 'blob-rest-id')
            resp = handler.create_blob(
                FlaskRequest.from_values(
                    query_string={'upload': 'chunked'}))
            self.assertEqual(resp.status, '201 CREATED')
            self.assertNotIn('content-range', resp.headers)

            handler = RestEndpointAdapter(
                endpoint, blob_rest_id='blob-rest-id',
                http_host='msa',
                blob_storage=blob_storage,
                rest_id_factory = lambda: 'rest-id')
            b = b'hello, '
            range = ContentRange('bytes', 0, len(b))
            resp = handler.put_blob(
                FlaskRequest.from_values(
                    data=b,
                    headers={'content-range': range}))
            self.assertEqual(resp.status, '200 OK')
            self.assert_eq_content_range(resp.content_range,
                                         ContentRange('bytes', 0, 7, None))

            handler = RestEndpointAdapter(
                endpoint, blob_rest_id='blob-rest-id',
                http_host='msa',
                blob_storage=blob_storage,
                rest_id_factory = lambda: 'rest-id')
            b2 = b'world!'
            range = ContentRange('bytes', len(b), len(b) + len(b2),
                                 len(b) + len(b2))
            resp = handler.put_blob(
                FlaskRequest.from_values(
                    data=b2,
                    headers={'content-range': range}))
            self.assertEqual(resp.status, '200 OK')
            self.assert_eq_content_range(resp.content_range,
                                         ContentRange('bytes', 0, 13, 13))



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')
    unittest.main()
