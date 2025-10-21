# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import List, Optional
import unittest
import logging
import socketserver
from threading import Thread
import time
import json
from urllib.parse import urljoin

from wsgiref.simple_server import make_server
import wsgiref.util
from werkzeug.datastructures import ContentRange
import werkzeug.http

from koukan.deadline import Deadline
from koukan.rest_endpoint import RestEndpoint, RestEndpointClientProvider
from koukan.rest_schema import FINALIZE_BLOB_HEADER
from koukan.filter import (
    HostPort,
    Mailbox,
    Resolution,
    TransactionMetadata,
    WhichJson )
from koukan.blob import CompositeBlob, InlineBlob
from koukan.response import Response as MailResponse
from koukan.message_builder import MessageBuilderSpec

class Request:
    method = None
    path = None
    query = None
    content_type = None
    content_range = None
    request_timeout : Optional[int] = None
    body = None
    etag = None
    finalize_length = None

    def __init__(self,
                 method = None,
                 path = None,
                 content_type = None,
                 content_range = None,
                 body = None,
                 etag = None):
        self.path = path
        self.content_type = content_type
        self.content_range = content_range
        self.body = body
        self.etag = etag

class Response:
    http_resp = None
    content_type = None
    body = None
    content_range = None
    location = None
    etag = None
    timeout = False
    def __init__(self,
                 http_resp = None,
                 content_type = None,
                 body = None,
                 content_range = None,
                 location = None,
                 resp_json = None,
                 etag = None,
                 timeout = False):
        self.http_resp = http_resp
        self.content_type = content_type
        self.body = body
        self.content_range = content_range
        self.location = location
        if resp_json is not None:
            self.body = json.dumps(resp_json)
            self.content_type = 'application/json'
        self.etag = etag
        self.timeout = timeout

def eq_range(lhs, rhs):
    return (lhs.units == rhs.units and
            lhs.start == rhs.start and
            lhs.stop == rhs.stop and
            lhs.length == rhs.length)

class RestEndpointTest(unittest.TestCase):
    requests : List[Request]
    responses : List[Response]
    client_provider : RestEndpointClientProvider

    def __init__(self, name):
        self.requests = []
        self.responses = []
        super().__init__(name)

    def setUp(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s %(filename)s:%(lineno)d %(message)s')

        # find a free port
        with socketserver.TCPServer(("localhost", 0), lambda x,y,z: None) as s:
            self.port = s.server_address[1]

        self.wsgi_server = make_server(
            '127.0.0.1',
            self.port,
            lambda environ, start_response:
              self.wsgi_app(environ, start_response))
        self.server_thread = Thread(
            target=lambda: self.wsgi_server.serve_forever(), daemon=True)
        self.server_thread.start()
        self.static_base_url='http://localhost:%d/' % self.port
        logging.info('RestEndpointTest server listening on %d', self.port)

        self.client_provider = RestEndpointClientProvider()

        self.tx_path = '/transactions/123'
        self.tx_url = urljoin(self.static_base_url, self.tx_path)
        self.body_path = '/transactions/123/body'
        self.body_url = urljoin(self.static_base_url, self.body_path)
        self.message_builder_url = urljoin(
            self.static_base_url, '/transactions/123/message_builder')
        self.blob_url = urljoin(
            self.static_base_url, '/transactions/123/blob/blob_rest_id')

    def tearDown(self):
        # i.e. all expected requests were sent
        self.assertFalse(self.responses)

    def assertEqualRange(self, lhs, rhs):
        if not eq_range(lhs, rhs):
            logging.debug('%s != %s', lhs, rhs)
            self.fail()

    def wsgi_app(self, environ, start_response):
        # logging.debug(environ)
        req = Request()
        req.method = environ['REQUEST_METHOD']
        req.path = environ['PATH_INFO']
        req.query = environ['QUERY_STRING']
        req.content_type = environ['CONTENT_TYPE']
        req.etag = environ.get('HTTP_IF_NONE_MATCH', None)
        if range_header := environ.get('HTTP_CONTENT_RANGE', None):
            req.content_range = (
                werkzeug.http.parse_content_range_header(range_header))
        if timeout_header := environ.get('HTTP_REQUEST_TIMEOUT', None):
            req.request_timeout = int(timeout_header)

        header = 'HTTP_' + FINALIZE_BLOB_HEADER.upper().replace('-', '_')
        if finalize_length := environ.get(header, None):
            finalize_length.strip()
            req.finalize_length = int(finalize_length)

        if ((wsgi_input := environ.get('wsgi.input', None)) and
            (length := environ.get('CONTENT_LENGTH', None))):
            req.body = wsgi_input.read(int(length))
        self.requests.append(req)

        resp = self.responses.pop(0)
        if resp.timeout:
            time.sleep(5)
        resp_headers = []
        if resp.content_type:
            resp_headers.append(('content-type', resp.content_type))
        if resp.content_range:
            resp_headers.append(('content-range', str(resp.content_range)))
        if resp.location:
            resp_headers.append(('location', resp.location))
        if resp.etag:
            resp_headers.append(('etag', resp.etag))

        start_response(resp.http_resp, resp_headers)
        resp_body = []
        if resp.body:
            resp_body.append(resp.body.encode('utf-8'))
        return resp_body

    def create_endpoint(self, **kwargs):
        filter = RestEndpoint(client_provider=self.client_provider, **kwargs)
        tx = TransactionMetadata()
        filter.wire_downstream(tx)
        return filter, tx

    # low-level methods

    def testCreate(self):
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url)
        self.responses.append(Response(
            http_resp = '201 created',
            resp_json={},
            location=self.tx_url,
            etag='1'))
        upstream_delta = rest_endpoint._create(
            resolution=None, tx=tx, deadline=Deadline())
        self.assertIsNotNone(upstream_delta)
        req = self.requests.pop(0)
        self.assertEqual(req.path, '/transactions')
        self.assertEqual(req.body, b'{}')
        self.assertEqual(req.content_type, 'application/json')

        self.assertEqual(self.tx_url, rest_endpoint.transaction_url)

        # check get_json() while we're at it
        js = {'hello': 'world'}
        self.responses.append(Response(
            http_resp = '200 ok',
            resp_json=js,
            etag='1'))
        resp_json = rest_endpoint.get_json(timeout=None)
        self.assertEqual(resp_json, js)
        req = self.requests.pop(0)
        self.assertIsNone(req.request_timeout)

        self.responses.append(Response(
            http_resp = '200 ok',
            resp_json=js,
            etag='2'))
        resp_json = rest_endpoint.get_json(timeout=2)
        self.assertEqual(resp_json, js)
        req = self.requests.pop(0)
        self.assertEqual(req.request_timeout, 1)

    def testCreateBadResponse(self):
        rest_endpoint, tx = self.create_endpoint(static_base_url=self.static_base_url)
        delta = TransactionMetadata(mail_from=Mailbox('alice'))
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = 'bad json',
            etag='1'))
        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        self.assertEqual(tx.mail_response.code, 450)

    def testCreateTimeoutPost(self):
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url, timeout_start=1)
        # POST /transactions times out
        self.responses.append(Response(timeout=True))
        delta = TransactionMetadata(mail_from=Mailbox('alice'))
        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        self.assertEqual(tx.mail_response.code, 450)

    def testCreateTimeoutGet(self):
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url, timeout_start=1)
        # POST /transactions -> 201
        self.responses.append(Response(
            http_resp = '201 created',
            resp_json={},
            location = self.tx_url,
            etag='1'))
        # GET /transactions/123 times out
        self.responses.append(Response(timeout=True))
        delta = TransactionMetadata(mail_from=Mailbox('alice'))
        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        self.assertEqual(tx.mail_response.code, 450)

    def testCreateNoSpin(self):
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url)
        # POST
        self.responses.append(Response(
            http_resp = '201 created',
            resp_json={ 'mail_from': {} },
            location = self.tx_url,
            etag='1'))
        # GET
        self.responses.append(Response(
            http_resp = '200 ok',
            resp_json={ 'mail_from': {} },
            etag='1'))
        # GET
        self.responses.append(Response(
            http_resp = '200 ok',
            resp_json={
                'mail_from': {},
                'mail_response': {'code': 234 }
            },
            etag='2'))

        delta = TransactionMetadata(mail_from=Mailbox('alice'))
        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        logging.debug(tx)
        self.assertEqual(tx.mail_response.code, 234)


    def testUpdateBadResponsePost(self):
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url)
        delta = TransactionMetadata(mail_from=Mailbox('alice'))
        self.responses.append(Response(
            http_resp = '201 created',
            resp_json={
                'mail_from': {},
                'mail_response': {'code': 200}},
            location = self.tx_url,
            etag='1'))
        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        self.assertEqual(tx.mail_response.code, 200)

        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = 'bad json',
            etag='2'))
        tx_delta = TransactionMetadata(rcpt_to=[Mailbox('bob')])
        tx.merge_from(tx_delta)
        rest_endpoint.on_update(tx_delta, None)
        self.assertEqual([r.code for r in tx.rcpt_response], [450])

    def _test_update_bad_response_get(self, resp_json):
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url)
        delta = TransactionMetadata(mail_from=Mailbox('alice'))

        self.responses.append(Response(
            http_resp = '201 created',
            content_type = 'application/json',
            resp_json={
                'mail_from': {},
                'mail_response': {'code': 200},
                'body': {'blob_status': {'uri': self.body_url }}},
            location = self.tx_url,
            etag='1'))
        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        self.assertEqual(tx.mail_response.code, 200)

        self.responses.append(Response(
            http_resp = '200 ok',
            resp_json={
                'mail_from': {},
                'mail_response': {'code': 200},
                'rcpt_to': [{}],
                'body': {'blob_status': {'uri': self.body_url }}},
            etag='2'))
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = resp_json,
            etag='3'))
        tx_delta = TransactionMetadata(rcpt_to=[Mailbox('bob')])
        tx.merge_from(tx_delta)
        rest_endpoint.on_update(tx_delta, None)
        self.assertEqual([r.code for r in tx.rcpt_response], [450])

    def test_update_bad_response_get_json_syntax(self):
        self._test_update_bad_response_get('invalid json')
    def test_update_bad_response_get_json_contents(self):
        self._test_update_bad_response_get(
            json.dumps({'some_weird_stuff': 3.14159}))

    def test_update_get_unchanged_success(self):
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url)
        delta = TransactionMetadata(mail_from=Mailbox('alice'))
        self.responses.append(Response(
            http_resp = '201 created',
            content_type = 'application/json',
            resp_json={'mail_from': {}},
            location = self.tx_url,
            etag = '1'))
        self.responses.append(Response(
            http_resp = '304 unchanged',
            content_type = 'application/json',
            resp_json={'mail_from': {}},
            etag = '1'))
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            resp_json={'mail_from': {},
                       'mail_response': {'code': 201}},
            etag = '2'))

        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        self.assertEqual(201, tx.mail_response.code)

    def test_update_get_unchanged_timeout(self):
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url, timeout_start=3)
        delta = TransactionMetadata(mail_from=Mailbox('alice'))
        self.responses.append(Response(
            http_resp = '201 created',
            content_type = 'application/json',
            resp_json={'mail_from': {}},
            location = self.tx_url,
            etag = '1'))
        for i in range(0,3):
            self.responses.append(Response(
                http_resp = '304 unchanged',
                content_type = 'application/json',
                etag = '1'))

        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        self.assertEqual(450, tx.mail_response.code)

    def testUpdateTimeoutPost(self):
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url, timeout_start=1)
        delta = TransactionMetadata(mail_from=Mailbox('alice'))
        self.responses.append(Response(
            http_resp = '201 created',
            resp_json={'mail_response': {'code': 200}},
            location = self.tx_url,
            etag='1'))
        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        self.assertEqual(tx.mail_response.code, 200)

        self.responses.append(Response(timeout=True))
        tx_delta = TransactionMetadata(rcpt_to=[Mailbox('bob')])
        tx.merge_from(tx_delta)
        rest_endpoint.on_update(tx_delta, None)
        self.assertEqual([r.code for r in tx.rcpt_response], [450])

    def testUpdateTimeoutGet(self):
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url, timeout_start=1)
        delta = TransactionMetadata(mail_from=Mailbox('alice'))
        self.responses.append(Response(
            http_resp = '201 created',
            resp_json={'mail_response': {}},
            location = self.tx_url,
            etag='1'))
        self.responses.append(Response(timeout=True))
        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        self.assertEqual(tx.mail_response.code, 450)


    def testPutBlob(self):
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url, chunk_size=8)

        # POST /transactions
        self.responses.append(Response(
            http_resp = '201 created',
            location=self.tx_url,
            resp_json={
                'mail_from': {},
                'mail_response': {'code': 201},
                'rcpt_to': [{}],
                'rcpt_response': [{'code': 202}],
                'body': {'blob_status': {'uri': self.body_url }}
            },
            etag='1'))

        # PUT /transactions/123/body
        # range 0-7/*
        self.responses.append(Response(
            http_resp = '200 ok',
            content_range=ContentRange('bytes', 0, 8, None)))

        # suppose something like a previous request timed out during
        # data transfer and was partially applied to get into this
        # state
        # PUT /transactions/123/body
        # range 8-12/13 -> 416 0-10/*
        self.responses.append(Response(
            http_resp = '416 bad range',
            content_range=ContentRange('bytes', 0, 10, None)))

        # PUT /transactions/123/body
        # range 11-12/13
        self.responses.append(Response(
            http_resp = '200 ok',
            content_range=ContentRange('bytes', 10, 13, 13)))

        delta = TransactionMetadata(mail_from=Mailbox('alice'),
                                 rcpt_to=[Mailbox('bob')])
        tx.merge_from(delta)
        rest_endpoint.on_update(delta)

        logging.debug('finalized blob')

        blob = InlineBlob(b'hello, world!', last=True)
        rest_endpoint._put_blob(
            blob, rest_endpoint.rest_upstream_tx.body.reuse_uri.parsed_uri)

        # POST /transactions
        req = self.requests.pop(0)
        self.assertEqual(req.path, '/transactions')

        # PUT /transactions/123/body
        # range 0-8/*
        req = self.requests.pop(0)
        self.assertEqual(req.content_range.start, 0)
        self.assertEqual(req.content_range.stop, 8)
        self.assertEqual(req.content_range.length, None)
        self.assertEqual(req.body, b'hello, w')

        # PUT /transactions/123/body
        # range 8-13/13
        req = self.requests.pop(0)
        self.assertEqual(req.body, b'orld!')
        self.assertEqual(req.content_range.start, 8)
        self.assertEqual(req.content_range.stop, 13)
        self.assertEqual(req.content_range.length, 13)

        # PUT /transactions/123/body
        # range 10-13/13
        req = self.requests.pop(0)
        self.assertEqual(req.body, b'ld!')
        self.assertEqual(req.content_range.start, 10)
        self.assertEqual(req.content_range.stop, 13)
        self.assertEqual(req.content_range.length, 13)



    def testPutBlobSingle(self):
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url)

        # POST /transactions
        self.responses.append(Response(
            http_resp = '201 created',
            location=self.tx_url,
            resp_json={
                'mail_from': {},
                'mail_response': {'code': 201},
                'rcpt_to': [{}],
                'rcpt_response': [{'code': 202}],
                'body': {'blob_status': {'uri': self.body_url }}
            },
            etag='1'))

        # POST /transactions/123/body
        self.responses.append(Response(http_resp = '201 created'))

        b = b'hello, world!'
        delta = TransactionMetadata(mail_from=Mailbox('alice'),
                                    rcpt_to=[Mailbox('bob')],
                                    body=InlineBlob(b, last=True))
        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        logging.debug(tx)

        # POST /transactions
        req = self.requests.pop(0)
        self.assertEqual(req.path, '/transactions')

        # POST /transactions/123/body
        req = self.requests.pop(0)
        self.assertEqual(self.body_path, req.path)
        self.assertIsNone(req.content_range)
        self.assertEqual(req.body, b)

    def testPutBlobBadResponse(self):
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url, timeout_data=1, chunk_size=4)
        delta = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')])

        tx_out = TransactionMetadata(
            mail_response=MailResponse(201),
            rcpt_response=[MailResponse(202)])

        # POST /transactions
        self.responses.append(Response(
            http_resp = '201 created',
            resp_json={
                'mail_from': {},
                'mail_response': {'code': 201},
                'rcpt_to': [{}],
                'rcpt_response': [{'code': 202}],
                'body': {'blob_status': {'uri': self.body_url }}},
            location = self.tx_url,
            etag='1'))

        # POST /transactions/123/body
        self.responses.append(Response(http_resp = '200 ok'))

        # PUT /transactions/123/body -> server returns invalid
        # content-range header
        self.responses.append(Response(
            http_resp = '200 ok',
            resp_json={},
            content_range = 'bad range'))

        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])
        blob_bytes = b'hello'
        tx_delta = TransactionMetadata(
            body=InlineBlob(blob_bytes, last=True))
        tx.merge_from(tx_delta)
        rest_endpoint.on_update(tx_delta, None)
        self.assertEqual(tx.data_response.code, 450)

    def testPutBlobTimeout(self):
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url, timeout_data=1, chunk_size=4)

        self.responses.append(Response(
            http_resp = '201 created',
            resp_json={
                'mail_from': {},
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_to': [{}],
                'rcpt_response': [{'code': 202, 'message': 'ok'}],
                'body': {'blob_status': {'uri': self.body_url }}},
            location = self.tx_url,
            etag='1'))
        delta = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')])
        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])

        tx_delta = TransactionMetadata(
           body=InlineBlob(b'hello', last=True))
        tx.merge_from(tx_delta)
        self.responses.append(Response(http_resp = '200 ok'))
        self.responses.append(Response(timeout=True))
        rest_endpoint.on_update(tx_delta, None)
        logging.debug(tx)
        self.assertEqual(tx.data_response.code, 450)

    def test_smoke(self):
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url,
            min_poll=0.1,
            chunk_size=8)

        # POST
        self.responses.append(Response(
            http_resp = '201 created',
            resp_json={
                'mail_from': {},
                'rcpt_to': [{}],
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'}],
                'body': {'blob_status': {'uri': self.body_url}}
            },
            location = self.tx_url,
            etag = '1'))

        logging.debug('testFilterApi envelope')
        delta = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')])
        tx.merge_from(delta)
        rest_endpoint.on_update(delta)

        req = self.requests.pop(0)
        self.assertEqual(req.method, 'POST')
        self.assertEqual(req.path, '/transactions')
        logging.debug(tx)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])

        logging.debug('testFilterApi !last append')
        # PUT /transactions/123/body
        # range: 0-7/*
        self.responses.append(Response(
            http_resp = '200 ok',
            resp_json={},
            content_range=ContentRange(
                'bytes', 0, 7, None)))

        tx_delta = TransactionMetadata()
        tx.body = tx_delta.body = CompositeBlob()
        b = InlineBlob(b'hello, ', last=True)
        tx.body.append(b, 0, b.len())
        rest_endpoint.on_update(tx_delta)
        self.assertIsNone(tx.data_response)

        req = self.requests.pop(0)
        self.assertEqual(req.method, 'PUT')
        self.assertEqual(self.body_path, req.path)
        self.assertEqual(req.content_range.stop, 7)
        self.assertEqual(req.content_range.length, None)

        logging.debug('testFilterApi last append')

        b = InlineBlob(b'world!', last=True)
        tx.body.append(b, 0, b.len(), True)

        # PUT /transactions/123/body
        # range: 7-12/12
        self.responses.append(Response(
            http_resp = '200 ok',
            resp_json={},
            content_range=ContentRange(
                'bytes', 0, tx.body.len(), tx.body.len())))

        # GET
        self.responses.append(Response(
            http_resp = '200 ok',
            etag = '3',
            resp_json={
                'mail_from': {},
                'rcpt_to': [{}],
                'body': {
                    'blob_status': {
                        'uri': self.body_url,
                        'finalized': True
                    }
                },
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'}],
                'data_response': {'code': 203, 'message': 'ok'}}))

        # same tx/delta
        rest_endpoint.on_update(tx_delta)
        logging.debug('testFilterApi after patch body %s', tx)
        self.assertEqual(tx.data_response.code, 203)

        req = self.requests.pop(0)
        self.assertEqual(req.method, 'PUT')
        self.assertEqual(self.body_path, req.path)
        self.assertEqual(req.content_range.stop, 13)
        self.assertEqual(req.content_range.length, 13)

        req = self.requests.pop(0)
        self.assertEqual(req.method, 'GET')
        self.assertEqual(self.tx_path, req.path)
        self.assertEqual(req.etag, '1')

    def testFilterApiOneshot(self):
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url, min_poll=0.1)

        body = b'hello, world!'

        # POST /transactions
        self.responses.append(Response(
            http_resp = '201 created',
            resp_json={
                'mail_from': {},
                'rcpt_to': [{}],
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'}],
                'body': {'blob_status': {'uri': self.body_url}}
            },
            location = self.tx_url,
            etag='1'))

        # POST /transactions/123/body
        self.responses.append(Response(
            http_resp = '200 created',
            resp_json={},
            content_range=ContentRange(
                'bytes', 0, len(body), len(body))))

        # GET /transactions/123
        self.responses.append(Response(
            http_resp = '200 ok',
            resp_json={
                'mail_from': {},
                'rcpt_to': [{}],
                'body': {
                    'blob_status': {'uri': self.body_url, 'finalized': True}
                },
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'}],
                'data_response': {'code': 203, 'message': 'ok'} },
            etag='2'))

        delta = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')],
            body=InlineBlob(body, last=True))
        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        logging.debug('tx after update %s', tx)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])
        self.assertEqual(tx.data_response.code, 203)

    def testFilterApiMultiRcpt(self):
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url, min_poll=0.1)

        # POST /transactions
        mail_resp = MailResponse(201)
        rcpt0_resp = MailResponse(202)
        rcpt1_resp = MailResponse(203)
        self.responses.append(Response(
            http_resp = '201 created',
            resp_json={
                'mail_from': {},
                'rcpt_to': [{}],
                'mail_response': mail_resp.to_json(WhichJson.REST_READ),
                'rcpt_response': [rcpt0_resp.to_json(WhichJson.REST_READ)]},
            location = self.tx_url,
            etag='1'))

        delta = TransactionMetadata(
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob')])
        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [rcpt0_resp.code])

        # PATCH /transactions/123
        self.responses.append(Response(
            http_resp = '200 ok',
            resp_json={
                'mail_from': {},
                'rcpt_to': [{}, {}],
                'mail_response': mail_resp.to_json(WhichJson.REST_READ),
                'rcpt_response': [rcpt0_resp.to_json(WhichJson.REST_READ)]},
            etag='2'))
        # GET /transactions/123
        self.responses.append(Response(
            http_resp = '200 ok',
            resp_json={
                'mail_from': {},
                'rcpt_to': [{}, {}],
                'mail_response': mail_resp.to_json(WhichJson.REST_READ),
                'rcpt_response': [rcpt0_resp.to_json(WhichJson.REST_READ),
                                  rcpt1_resp.to_json(WhichJson.REST_READ)]},
            etag='3'))

        prev = tx.copy()
        tx.rcpt_to.append(Mailbox('bob2'))
        rest_endpoint.on_update(prev.delta(tx), None)
        logging.debug('RestEndpointTest.testFilterApiMultiRcpt '
                      'after patch 2nd rcpt %s', tx)
        self.assertEqual([202, 203], [r.code for r in tx.rcpt_response])

    def testFilterApiEmptyLastChunk(self):
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url, min_poll=0.1)

        body = b'hello, world!'

        # POST /transactions
        self.responses.append(Response(
            http_resp = '201 created',
            resp_json={
                'mail_from': {},
                'rcpt_to': [{}],
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'}],
                'body': {'blob_status': {'uri': self.body_url}}
            },
            location = self.tx_url,
            etag='1'))

        # PUT /transactions/123/body
        self.responses.append(Response(
            http_resp = '200 ok',
            content_range=ContentRange('bytes', 0, len(body))))

        delta = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')],
            body=InlineBlob(body))
        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        logging.debug('tx after update %s', tx)
        self.assertEqual(201, tx.mail_response.code)
        self.assertEqual([202], [r.code for r in tx.rcpt_response])

        req = self.requests.pop(0)
        self.assertEqual(req.method, 'POST')
        self.assertEqual(req.path, '/transactions')
        req = self.requests.pop(0)
        self.assertEqual(req.method, 'PUT')
        self.assertEqual(self.body_path, req.path)
        self.assertEqual(req.content_range.start, 0)
        self.assertEqual(req.content_range.stop, 13)
        self.assertEqual(req.content_range.length, None)


        # PUT /transactions/123/body
        self.responses.append(Response(
            http_resp = '200 ok',
            resp_json={},
            content_range=ContentRange(
                'bytes', 0, len(body), len(body))))

        # GET /transactions/123
        self.responses.append(Response(
            http_resp = '200 ok',
            resp_json={
                'mail_from': {},
                'rcpt_to': [{}],
                'body': {
                    'blob_status': {
                        'uri': self.body_url, 'finalized': True
                    }
                },

                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'}],
                'data_response': {'code': 203, 'message': 'ok'} },
            etag='2'))

        tx.body.append(b'', last=True)
        rest_endpoint.on_update(
            TransactionMetadata(body=tx.body), None)
        logging.debug('tx after update %s', tx)
        self.assertEqual(203, tx.data_response.code)

        req = self.requests.pop(0)
        self.assertEqual(req.method, 'PUT')
        self.assertEqual(self.body_path, req.path)
        self.assertIsNone(req.content_range)
        self.assertEqual(13, req.finalize_length)

    def test_discovery(self):
        resolution = Resolution([
            HostPort('first', 25),
            HostPort('second', 25)])
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url, min_poll=0.1)
        # POST
        self.responses.append(Response(http_resp = '500 timeout'))

        self.responses.append(Response(
            http_resp = '201 created',
            resp_json={
                'remote_host': ['second', 25],
                'mail_from': {},
                'rcpt_to': [{}] },
            location = self.tx_url,
            etag='1'))

        # GET
        self.responses.append(Response(
            http_resp = '200 ok',
            resp_json={
                'remote_host': ['second', 25],
                'mail_from': {},
                'rcpt_to': [{}],
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'}] },
            etag='2'))

        delta = TransactionMetadata(
            remote_host = HostPort('example.com', 25),
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob')],
            resolution=resolution)
        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        self.assertEqual(201, tx.mail_response.code)
        self.assertEqual([202], [r.code for r in tx.rcpt_response])

    def test_discovery_fail(self):
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url, min_poll=0.1)
        resolution = Resolution([HostPort('first', 25)])
        self.responses.append(Response(http_resp = '500 timeout'))
        delta = TransactionMetadata(
            remote_host = HostPort('example.com', 25),
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob')],
            resolution=resolution)
        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        self.assertEqual(tx.mail_response.code, 450)

    def test_bad_upstream_delta(self):
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url, min_poll=0.1, chunk_size=8)

        # POST
        self.responses.append(Response(
            http_resp = '201 created',
            resp_json={'mail_from': {'m': 'alice'}},
            location = self.tx_url,
            etag='1'))

        logging.debug('test_bad_delta envelope')
        delta = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')])
        tx.merge_from(delta)
        rest_endpoint.on_update(delta)

        req = self.requests.pop(0)  # POST /transactions
        self.assertEqual(tx.mail_response.code, 450)

    def test_parsed(self):
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url, min_poll=0.1)
        delta = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')])

        # POST /transactions
        self.responses.append(Response(
            http_resp = '201 created',
            resp_json={
                'remote_host': ['mx.example.com', 25],
                'mail_from': {},
                'rcpt_to': [{}],
                'body': {
                    'blob_status': {'uri': self.body_url},
                    'message_builder': {
                        'uri': self.message_builder_url,
                    }
                }
            },
            location = self.tx_url,
            etag='1'))

        # GET /transactions/123
        self.responses.append(Response(
            http_resp = '200 ok',
            resp_json={
                'remote_host': ['mx.example.com', 25],
                'mail_from': {},
                'rcpt_to': [{}],
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'}],
                'body': {
                    'blob_status': {'uri': self.body_url},
                    'message_builder': {
                        'uri': self.message_builder_url,
                    }
                }
            },
            etag='2'))

        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        self.assertEqual(201, tx.mail_response.code)
        self.assertEqual([202], [r.code for r in tx.rcpt_response])
        logging.debug(tx)

        parsed_delta = TransactionMetadata()
        blob = b'hello, world!\r\n'
        body = (b'Message-id: <abc@def>\r\n'
                b'\r\n'
                b'hello, world!\r\n')
        parsed_delta.body = MessageBuilderSpec(
            {'text_body': [{
                "content": {"create_id": "blob_rest_id"}
            }]},
            blobs = {'blob_rest_id':
                     InlineBlob(blob, rest_id='blob_rest_id', last=True)})
        parsed_delta.body.body_blob = InlineBlob(body, last=True)
        tx.merge_from(parsed_delta)

        # POST /transactions/123/message_builder
        self.responses.append(Response(
            http_resp = '200 ok',
            resp_json={
                'remote_host': ['mx.example.com', 25],
                'mail_from': {},
                'rcpt_to': [{}],
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'}],
                'body': {
                    'blob_status': {'uri': self.body_url},
                    'message_builder': {
                        'blob_status': {
                            'blob_rest_id': {'uri': self.blob_url}
                        }
                    }
                }
            }))

        # PUT /transactions/123/blob/blob_id
        self.responses.append(Response(
            http_resp = '200 ok'))

        # PUT /transactions/123/body
        self.responses.append(Response(
            http_resp = '200 created'))

        # GET /transactions/123
        self.responses.append(Response(
            http_resp = '200 ok',
            resp_json={
                'remote_host': ['mx.example.com', 25],
                'mail_from': {},
                'rcpt_to': [{}],
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'}],
                'data_response': {'code': 203, 'message': 'ok'},
                'body': {
                    'blob_status': {'uri': self.body_url, 'finalized': True},
                    'message_builder': {
                        'blob_status': {
                            'blob_rest_id': {'uri': self.blob_url,
                                             'finalized': True}
                        }
                    }
                }
            },
            etag='3'))

        rest_endpoint.on_update(parsed_delta, None)
        logging.debug(tx)
        self.assertEqual(tx.data_response.code, 203)


    def test_parsed_json_err(self):
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url, min_poll=0.1)
        delta = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')])

        # POST /transactions
        self.responses.append(Response(
            http_resp = '201 created',
            resp_json={
                'remote_host': ['mx.example.com', 25],
                'mail_from': {},
                'rcpt_to': [{}],
                'body': {
                    'blob_status': {'uri': self.body_url},
                    'message_builder': {
                        'uri': self.message_builder_url,
                        'blob_status': {
                            'blob_rest_id': {'uri': self.blob_url}
                        }
                    }
                }
            },
            location = self.tx_url,
            etag='1'))

        # GET /transactions/123
        self.responses.append(Response(
            http_resp = '200 ok',
            resp_json={
                'remote_host': ['mx.example.com', 25],
                'mail_from': {},
                'rcpt_to': [{}],
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'}],
                'body': {
                    'blob_status': {'uri': self.body_url},
                    'message_builder': {
                        'uri': self.message_builder_url,
                        'blob_status': {
                            'blob_rest_id': {'uri': self.blob_url}
                        }
                    }
                }
            },
            etag='2'))

        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        self.assertEqual(201, tx.mail_response.code)
        self.assertEqual([202], [r.code for r in tx.rcpt_response])

        parsed_delta = TransactionMetadata()
        blob = b'hello, world!\r\n'
        parsed_delta.body = MessageBuilderSpec(
            {'text_body': [{
                'content': {'create_id': 'blob_rest_id'}
            }]},
            blobs={'blob_rest_id':InlineBlob(
                blob, rest_id='blob_rest_id', last=True)})
        body = (b'Message-id: <abc@def>\r\n'
                b'\r\n'
                b'hello, world!\r\n')
        parsed_delta.body.body_blob = InlineBlob(body, last=True)
        tx.merge_from(parsed_delta)

        # POST /transactions/123/message_builder
        self.responses.append(Response(
            http_resp = '500 err'))

        rest_endpoint.on_update(parsed_delta, None)
        self.assertEqual(tx.data_response.code, 400)

    def test_parsed_blob_err(self):
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url,
            min_poll=0.1)
        delta = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')])

        # POST /transactions
        self.responses.append(Response(
            http_resp = '201 created',
            resp_json={
                'remote_host': ['mx.example.com', 25],
                'mail_from': {},
                'rcpt_to': [{}],
                'body': {
                    'blob_status': {'uri': self.body_url},
                    'message_builder': {
                        'uri': self.message_builder_url,
                        'blob_status': {
                            'blob_rest_id': {'uri': self.blob_url}
                        }
                    }
                }
            },
            location = self.tx_url,
            etag='1'))

        # GET /transactions/123
        self.responses.append(Response(
            http_resp = '200 ok',
            resp_json={
                'remote_host': ['mx.example.com', 25],
                'mail_from': {},
                'rcpt_to': [{}],
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'}],
                'body': {
                    'blob_status': {'uri': self.body_url},
                    'message_builder': {
                        'uri': self.message_builder_url,
                        'blob_status': {
                            'blob_rest_id': {'uri': self.blob_url}
                        }
                    }
                }
            },
            etag='2'))

        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        self.assertEqual(201, tx.mail_response.code)
        self.assertEqual([202], [r.code for r in tx.rcpt_response])

        parsed_delta = TransactionMetadata()
        blob = b'hello, world!\r\n'

        parsed_delta.body = MessageBuilderSpec(
            {'text_body': [{
                'content': {'create_id': 'blob_rest_id'}
            }]},
            blobs = {'blob_rest_id':
                     InlineBlob(blob, rest_id='blob_rest_id', last=True)})
        body = (b'Message-id: <abc@def>\r\n'
                b'\r\n'
                b'hello, world!\r\n')
        parsed_delta.body.body_blob = InlineBlob(body, last=True)
        tx.merge_from(parsed_delta)

        # POST /transactions/123/message_builder
        self.responses.append(Response(
            http_resp = '200 ok',
            resp_json={
                'remote_host': ['mx.example.com', 25],
                'mail_from': {},
                'rcpt_to': [{}],
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'}],
                'body': {
                    'blob_status': {'uri': self.body_url},
                    'message_builder': {
                        'uri': self.message_builder_url,
                        'blob_status': {
                            'blob_rest_id': {'uri': self.blob_url}
                        }
                    }
                }
            }
        ))

        # POST /transactions/123/blob/blob_id
        self.responses.append(Response(http_resp = '500 err'))

        rest_endpoint.on_update(parsed_delta, None)
        self.assertEqual(tx.data_response.code, 450)

    def test_all_rcpts_fail(self):
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url, min_poll=0.1)
        body = b'hello, world'
        delta = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')],
            body=InlineBlob(body, last=True))

        # POST /transactions
        self.responses.append(Response(
            http_resp = '201 created',
            resp_json={
                'remote_host': ['mx.example.com', 25],
                'mail_from': {},
                'rcpt_to': [{}],
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 502, 'message': 'err'}] },
            location = self.tx_url,
            etag='1'))
        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        self.assertEqual(201, tx.mail_response.code)
        self.assertEqual([502], [r.code for r in tx.rcpt_response])
        self.assertEqual(503, tx.data_response.code)

    def test_data_resp_wait(self):
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url, min_poll=0.1)
        body = b'hello, world'
        delta = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')],
            body=InlineBlob(body, last=True))

        # POST /transactions
        self.responses.append(Response(
            http_resp = '201 created',
            resp_json={
                'remote_host': ['mx.example.com', 25],
                'mail_from': {},
                'rcpt_to': [{}],
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'err'}],
                'body': {'blob_status': {'uri': self.body_url}}
            },
            location = self.tx_url,
            etag='1'))

        # POST /transactions/123/body
        self.responses.append(Response(
            http_resp = '200 created'))

        # GET /transactions/123
        self.responses.append(Response(
            http_resp = '200 ok',
            resp_json={
                'remote_host': ['mx.example.com', 25],
                'mail_from': {},
                'rcpt_to': [{}],
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'err'}],
                'body': {
                    'blob_status': {'uri': self.body_url, 'finalized': True}
                }},
            etag='2'))

        # GET /transactions/123
        self.responses.append(Response(
            http_resp = '200 ok',
            resp_json={
                'remote_host': ['mx.example.com', 25],
                'mail_from': {},
                'rcpt_to': [{}],
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'err'}],
                'body': {
                    'blob_status': {
                        'uri': self.body_url, 'finalized': True
                    }
                },
                'data_response': {'code': 203, 'message': 'ok'}},
            etag='3'))

        tx.merge_from(delta)
        rest_endpoint.on_update(delta)
        self.assertEqual(201, tx.mail_response.code)
        self.assertEqual([202], [r.code for r in tx.rcpt_response])
        self.assertEqual(203, tx.data_response.code)


    def test_cancel(self):
        rest_endpoint, tx = self.create_endpoint(
            static_base_url=self.static_base_url, min_poll=0.1)
        delta = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')])

        # POST /transactions
        self.responses.append(Response(
            http_resp = '201 created',
            resp_json={
                'remote_host': ['mx.example.com', 25],
                'mail_from': {},
                'rcpt_to': [{}],
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'}] },
            location = self.tx_url,
            etag='1'))
        tx.merge_from(delta)
        rest_endpoint.on_update(delta)

        # POST /transactions/123/cancel
        self.responses.append(Response(
            http_resp = '200 ok',
            resp_json={}))

        delta = TransactionMetadata(cancelled = True)
        tx.merge_from(delta)
        rest_endpoint.on_update(delta)


if __name__ == '__main__':
    unittest.main()
