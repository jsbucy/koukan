import unittest
import logging
import io
from threading import Thread

from flask import (
    Flask,
    Request as FlaskRequest )
from werkzeug.datastructures import ContentRange

from rest_endpoint_adapter import RestEndpointAdapter, SyncFilterAdapter
from fake_endpoints import FakeAsyncEndpoint, FakeSyncFilter
from executor import Executor
from filter import Mailbox, TransactionMetadata, WhichJson
from response import Response
from storage import Storage


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


class RestEndpointAdapterTest(unittest.TestCase):
    def test_create_tx(self):
        app = Flask(__name__)
        endpoint = FakeAsyncEndpoint(rest_id='rest-id')
        tx = TransactionMetadata()
        tx.rest_id = 'rest_id'
        endpoint.merge(tx)
        storage = Storage.get_sqlite_inmemory_for_test()

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
                'mail_from': {},
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
                'mail_from': {},
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_to': [{}]
            })

            endpoint.merge(TransactionMetadata(rcpt_response=[Response(202)]))

            resp = handler.get_tx(FlaskRequest.from_values(json={}))
            self.assertEqual(resp.json, {
                'mail_from': {},
                'rcpt_to': [{}],
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'}]
            })


            handler = RestEndpointAdapter(endpoint, tx_rest_id='rest_id',
                                          http_host='msa')
            resp = handler.patch_tx(FlaskRequest.from_values(
                json={"rcpt_to": [{"m": "bob2"}],
                      "rcpt_to_list_offset": 1},
                headers={'if-match': resp.headers['etag']}))
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
            resp = handler.get_tx(FlaskRequest.from_values(json={}))
            self.assertEqual(resp.json, {
                'mail_from': {},
                'rcpt_to': [{}, {}],
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'},
                                  {'code': 203, 'message': 'ok'}]
            })
            tx_etag = resp.headers['etag']

            handler = RestEndpointAdapter(
                endpoint,
                http_host='msa',
                rest_id_factory = lambda: 'blob-rest-id',
                blob_storage=storage)
            resp = handler.put_blob(
                FlaskRequest.from_values(data='hello, world!'),
                tx_body=True)
            logging.debug(resp.response)
            self.assertEqual(resp.status, '200 OK')
            self.assertEqual(handler.blob_rest_id(), 'blob-rest-id')

            endpoint.merge(TransactionMetadata(data_response=Response(204)))
            resp = handler.get_tx(FlaskRequest.from_values(json={}))
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


            handler = RestEndpointAdapter(endpoint, tx_rest_id='rest_id',
                                          http_host='msa')
            endpoint.merge(TransactionMetadata(data_response=Response(204)))
            resp = handler.get_tx(FlaskRequest.from_values(json={}))
            self.assertEqual(resp.status, '200 OK')
            logging.debug('RestEndpointAdapterTest get %s', resp.json)
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
        storage = Storage.get_sqlite_inmemory_for_test()

        tx_cursor = storage.get_transaction_cursor()
        tx_cursor.create('tx_rest_id', TransactionMetadata())

        with app.test_request_context():
            # content-range header is not accepted in non-chunked blob post
            handler = RestEndpointAdapter(
                endpoint,
                http_host='msa',
                blob_storage=storage,
                rest_id_factory = lambda: 'blob-rest-id',
                tx_rest_id='tx_rest_id')
            resp = handler.create_blob(
                FlaskRequest.from_values(
                    headers={'content-range': ContentRange('bytes', 0,10,10)}))
            self.assertEqual(resp.status, '400 BAD REQUEST')

            # POST /blob?upload=chunked may eventually take
            # json metadata as the entity but for now that's unimplemented
            handler = RestEndpointAdapter(
                endpoint,
                http_host='msa',
                blob_storage=storage,
                rest_id_factory = lambda: 'blob-rest-id',
                tx_rest_id='tx_rest_id')
            resp = handler.create_blob(
                FlaskRequest.from_values(
                    query_string={'upload': 'chunked'},
                    data='unimplemented params'))
            self.assertEqual(resp.status, '400 BAD REQUEST')

            handler = RestEndpointAdapter(
                endpoint,
                http_host='msa',
                blob_storage=storage,
                rest_id_factory = lambda: 'blob-rest-id',
                tx_rest_id='tx_rest_id')
            resp = handler.create_blob(
                FlaskRequest.from_values(
                    query_string={'upload': 'chunked'}))
            self.assertEqual(resp.status, '201 CREATED')
            self.assertNotIn('content-range', resp.headers)

            handler = RestEndpointAdapter(
                endpoint, blob_rest_id='blob-rest-id',
                http_host='msa',
                blob_storage=storage,
                rest_id_factory = lambda: 'rest-id',
                tx_rest_id='tx_rest_id')
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
                blob_storage=storage,
                rest_id_factory = lambda: 'rest-id',
                tx_rest_id='tx_rest_id')
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
