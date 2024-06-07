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

from deadline import Deadline
from rest_endpoint import RestEndpoint
from filter import HostPort, Mailbox, TransactionMetadata
from blob import CompositeBlob, InlineBlob
from response import Response as MailResponse

class Request:
    method = None
    path = None
    query = None
    content_type = None
    content_range = None
    request_timeout : Optional[int] = None
    body = None
    def __init__(self,
                 method = None,
                 path = None,
                 content_type = None,
                 content_range = None,
                 body = None):
        self.path = path
        self.content_type = content_type
        self.content_range = content_range
        self.body = body

class Response:
    http_resp = None
    content_type = None
    body = None
    content_range = None
    location = None
    def __init__(self,
                 http_resp = None,
                 content_type = None,
                 body = None,
                 content_range = None,
                 location = None):
        self.http_resp = http_resp
        self.content_type = content_type
        self.body = body
        self.content_range = content_range
        self.location = location

def eq_range(lhs, rhs):
    return (lhs.units == rhs.units and
            lhs.start == rhs.start and
            lhs.stop == rhs.stop and
            lhs.length == rhs.length)

class RestEndpointTest(unittest.TestCase):
    requets : List[Request]
    responses : List[Response]

    def __init__(self, name):
        self.requests = []
        self.responses = []
        super().__init__(name)

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

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

    def tearDown(self):
        # i.e. all expected requests were sent
        self.assertFalse(self.responses)

    def assertEqualRange(self, lhs, rhs):
        if not eq_range(lhs, rhs):
            logging.debug('%s != %s', lhs, rhs)
            self.fail()

    def wsgi_app(self, environ, start_response):
        #logging.debug(environ)
        req = Request()
        req.method = environ['REQUEST_METHOD']
        req.path = environ['PATH_INFO']
        req.query = environ['QUERY_STRING']
        req.content_type = environ['CONTENT_TYPE']
        if range_header := environ.get('HTTP_CONTENT_RANGE', None):
            req.content_range = (
                werkzeug.http.parse_content_range_header(range_header))
        if timeout_header := environ.get('HTTP_REQUEST_TIMEOUT', None):
            req.request_timeout = int(timeout_header)

        if ((wsgi_input := environ.get('wsgi.input', None)) and
            (length := environ.get('CONTENT_LENGTH', None))):
            req.body = wsgi_input.read(int(length))
        self.requests.append(req)

        if not self.responses:
            # some test uses this to exercise timeouts but we want to
            # fastfail the rest of the time?
            time.sleep(5)
            start_response('500 timeout', [])
            return ''

        resp = self.responses.pop(0)
        resp_headers = []
        if resp.content_type:
            resp_headers.append(('content-type', resp.content_type))
        if resp.content_range:
            resp_headers.append(('content-range', str(resp.content_range)))
        if resp.location:
            resp_headers.append(('location', resp.location))

        start_response(resp.http_resp, resp_headers)
        resp_body = []
        if resp.body:
            resp_body.append(resp.body.encode('utf-8'))
        return resp_body

    # low-level methods

    def testCreate(self):
        rest_endpoint = RestEndpoint(static_base_url=self.static_base_url)
        tx = TransactionMetadata()
        self.responses.append(Response(
            http_resp = '201 created',
            content_type = 'application/json',
            body = json.dumps({}),
            location='/transactions/123'))
        rest_resp = rest_endpoint._start(tx, Deadline())
        self.assertEqual(rest_resp.status_code, 201)
        req = self.requests.pop(0)
        self.assertEqual(req.path, '/transactions')
        self.assertEqual(req.body, b'{}')
        self.assertEqual(req.content_type, 'application/json')

        self.assertEqual(rest_endpoint.transaction_url,
                         urljoin(self.static_base_url, '/transactions/123'))

        # check get_json() while we're at it
        js = {'hello': 'world'}
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = json.dumps(js)))
        resp_json = rest_endpoint.get_json(timeout=None)
        self.assertEqual(resp_json, js)
        req = self.requests.pop(0)
        self.assertIsNone(req.request_timeout)

        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = json.dumps(js)))
        resp_json = rest_endpoint.get_json(timeout=2)
        self.assertEqual(resp_json, js)
        req = self.requests.pop(0)
        self.assertEqual(req.request_timeout, 1)

    def testCreateBadResponse(self):
        rest_endpoint = RestEndpoint(static_base_url=self.static_base_url)
        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = 'bad json'))
        upstream_delta = rest_endpoint.on_update(tx, tx.copy())
        self.assertEqual(tx.mail_response.code, 450)

    def testCreateTimeoutPost(self):
        rest_endpoint = RestEndpoint(static_base_url=self.static_base_url,
                                     timeout_start=1)
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = json.dumps({'mail_response': {}}),
            location='/transactions/124'))
        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        upstream_delta = rest_endpoint.on_update(tx, tx.copy())
        self.assertEqual(tx.mail_response.code, 450)

    def testCreateTimeoutGet(self):
        rest_endpoint = RestEndpoint(static_base_url=self.static_base_url,
                                     timeout_start=1)
        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        upstream_delta = rest_endpoint.on_update(tx, tx.copy())
        self.assertEqual(tx.mail_response.code, 450)

    def testCreateNoSpin(self):
        rest_endpoint = RestEndpoint(static_base_url=self.static_base_url)
        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        # POST
        self.responses.append(Response(
            http_resp = '201 created',
            content_type = 'application/json',
            body = json.dumps({ 'mail_from': {} }),
            location = '/transactions/123'))
        # GET
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = json.dumps({ 'mail_from': {} })))
        # GET
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = json.dumps({
                'mail_from': {},
                'mail_response': {'code': 234 }
            })))

        upstream_delta = rest_endpoint.on_update(tx, tx.copy(), 10)
        logging.debug(tx)
        self.assertEqual(tx.mail_response.code, 234)

    def testUpdate(self):
        rest_endpoint = RestEndpoint(
            transaction_url=self.static_base_url + '/transactions/124')

        self.responses.append(Response(
            http_resp = '201 created',
            content_type = 'application/json',
            body = json.dumps({'mail_response': {}}),
            location = '/transactions/124'))

        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        rest_resp = rest_endpoint._update(tx, Deadline())
        self.assertEqual(rest_resp.status_code, 201)
        req = self.requests.pop(0)
        self.assertEqual(req.path, '/transactions/124')
        self.assertEqual(
            req.body,
            json.dumps({'mail_from': {'m': 'alice'}}).encode('utf-8'))
        self.assertEqual(req.content_type, 'application/json')
        self.assertEqual(rest_resp.json(),
                         {'mail_response': {}})

    def testUpdateBadResponsePost(self):
        rest_endpoint = RestEndpoint(static_base_url=self.static_base_url)
        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        self.responses.append(Response(
            http_resp = '201 created',
            content_type = 'application/json',
            body = json.dumps({
                'mail_from': {},
                'mail_response': {'code': 200}}),
            location = '/transactions/124'))
        upstream_delta = rest_endpoint.on_update(tx, tx.copy())
        self.assertEqual(tx.mail_response.code, 200)

        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = 'bad json'))
        tx_delta = TransactionMetadata(rcpt_to=[Mailbox('bob')])
        assert tx.merge_from(tx_delta) is not None
        upstream_delta = rest_endpoint.on_update(tx, tx_delta)
        self.assertEqual([r.code for r in tx.rcpt_response], [450])

    def testUpdateBadResponseGet(self):
        rest_endpoint = RestEndpoint(static_base_url=self.static_base_url)
        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        self.responses.append(Response(
            http_resp = '201 created',
            content_type = 'application/json',
            body = json.dumps({
                'mail_from': {},
                'mail_response': {'code': 200}}),
            location = '/transactions/124'))
        upstream_delta = rest_endpoint.on_update(tx, tx.copy())
        self.assertEqual(tx.mail_response.code, 200)

        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = json.dumps({
                'mail_from': {},
                'mail_response': {'code': 200},
                'rcpt_to': [{}]})))
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = 'bad json'))
        tx_delta = TransactionMetadata(rcpt_to=[Mailbox('bob')])
        assert tx.merge_from(tx_delta) is not None
        upstream_delta = rest_endpoint.on_update(tx, tx_delta)
        self.assertEqual([r.code for r in tx.rcpt_response], [450])

    def testUpdateTimeoutPost(self):
        rest_endpoint = RestEndpoint(static_base_url=self.static_base_url,
                                     timeout_start=1)
        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        self.responses.append(Response(
            http_resp = '201 created',
            content_type = 'application/json',
            body = json.dumps({'mail_response': {'code': 200}}),
            location = '/transactions/124'))
        upstream_delta = rest_endpoint.on_update(tx, tx.copy())
        self.assertEqual(tx.mail_response.code, 200)

        tx_delta = TransactionMetadata(rcpt_to=[Mailbox('bob')])
        assert tx.merge_from(tx_delta) is not None
        upstream_delta = rest_endpoint.on_update(tx, tx_delta)
        self.assertEqual([r.code for r in tx.rcpt_response], [450])

    def testUpdateTimeoutGet(self):
        rest_endpoint = RestEndpoint(static_base_url=self.static_base_url,
                                     timeout_start=1)
        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        self.responses.append(Response(
            http_resp = '201 created',
            content_type = 'application/json',
            body = json.dumps({'mail_response': {}}),
            location = '/transactions/124'))
        upstream_delta = rest_endpoint.on_update(tx, tx.copy())
        self.assertEqual(tx.mail_response.code, 450)


    def testPutBlob(self):
        rest_endpoint = RestEndpoint(static_base_url=self.static_base_url,
                                     chunk_size=8)

        # POST /blob
        self.responses.append(Response(
            http_resp = '201 created',
            location = '/blob/123'))

        self.responses.append(Response(
            http_resp = '200 ok',
            content_range=ContentRange('bytes', 0, 8, None)))


        # suppose something like a previous request was applied but
        # the response timed out to get into this state
        # PUT /blob 8-12/13 -> 416 0-10/*
        self.responses.append(Response(
            http_resp = '416 bad range',
            content_range=ContentRange('bytes', 0, 10, None)))

        # PUT /blob 11-12/13
        self.responses.append(Response(
            http_resp = '416 bad range',
            content_range=ContentRange('bytes', 11, 12, 13)))

        blob = InlineBlob(b'hello, world!')
        rest_endpoint._put_blob(blob)

        # POST /blob?upload=chunked
        req = self.requests.pop(0)
        self.assertEqual(req.path, '/blob')
        self.assertEqual(req.query, 'upload=chunked')
        self.assertIsNone(req.content_range)
        self.assertEqual(req.body, b'')

        # PUT /blob/xyz
        req = self.requests.pop(0)
        self.assertEqual(req.content_range.start, 0)
        self.assertEqual(req.content_range.stop, 8)
        self.assertEqual(req.content_range.length, None)
        self.assertEqual(req.body, b'hello, w')

        # PUT /blob/xyz
        req = self.requests.pop(0)
        self.assertEqual(req.body, b'orld!')
        self.assertEqual(req.content_range.start, 8)
        self.assertEqual(req.content_range.stop, 13)
        self.assertEqual(req.content_range.length, 13)

        # PUT /blob/xyz
        req = self.requests.pop(0)
        self.assertEqual(req.body, b'ld!')
        self.assertEqual(req.content_range.start, 10)
        self.assertEqual(req.content_range.stop, 13)
        self.assertEqual(req.content_range.length, 13)


    def testPutBlobChunk(self):
        rest_endpoint = RestEndpoint(static_base_url=self.static_base_url)

        self.responses.append(Response(
            http_resp = '201 created',
            location='/blob/123'))

        resp, len = rest_endpoint._put_blob_chunk(
            128, b'hello', False)
        self.assertTrue(resp.ok())
        self.assertEqual(len, 0)
        self.assertEqual(rest_endpoint.blob_url, '/blob/123')
        self.assertEqual(rest_endpoint.full_blob_url,
                         urljoin(self.static_base_url, '/blob/123'))
        req = self.requests.pop(0)
        self.assertEqual(req.method, 'POST')
        self.assertEqual(req.path, '/blob')
        self.assertEqual(req.query, 'upload=chunked')

        self.assertIsNone(req.content_range)

        self.responses.append(Response(
            http_resp = '200 ok',
            content_range=ContentRange('bytes', 0, 133, None)))
        resp, len = rest_endpoint._put_blob_chunk(128, b'hello', True)
        req = self.requests.pop(0)
        self.assertTrue(resp.ok())
        self.assertEqual(len, 133)
        self.assertEqual(req.method, 'PUT')
        self.assertEqual(req.path, '/blob/123')
        self.assertEqual(req.content_range.start, 128)
        self.assertEqual(req.content_range.stop, 133)
        self.assertEqual(req.content_range.length, 133)

        self.responses.append(Response(
            http_resp = '416 invalid range',
            content_range=ContentRange('bytes', 0, 120, None)))
        resp, len = rest_endpoint._put_blob_chunk(128, b'hello', True)
        req = self.requests.pop(0)
        self.assertTrue(resp.ok())
        self.assertEqual(len, 120)
        self.assertEqual(req.method, 'PUT')
        self.assertEqual(req.path, '/blob/123')
        self.assertEqual(req.content_range.start, 128)
        self.assertEqual(req.content_range.stop, 133)
        self.assertEqual(req.content_range.length, 133)


    def testPutBlobBadResponse(self):
        rest_endpoint = RestEndpoint(static_base_url=self.static_base_url,
                                     timeout_data=1,
                                     chunk_size=4)
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')])

        tx_out = TransactionMetadata(
            mail_response=MailResponse(201),
            rcpt_response=[MailResponse(202)])

        # POST /transactions
        self.responses.append(Response(
            http_resp = '201 created',
            content_type = 'application/json',
            body = json.dumps({
                'mail_from': {},
                'mail_response': {'code': 201},
                'rcpt_to': [{}],
                'rcpt_response': [{'code': 202}] }),
            location = '/transactions/124'))

        # POST /blob
        self.responses.append(Response(
            http_resp = '201 created',
            content_type = 'application/json',
            location = '/blob/123'))

        # PUT /blob/123 -> invalid content-range
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            content_range = 'bad range'))

        upstream_delta = rest_endpoint.on_update(tx, tx.copy())
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])
        blob_bytes = b'hello'
        tx_delta = TransactionMetadata(
            body_blob=InlineBlob(blob_bytes, len(blob_bytes)))
        assert tx.merge_from(tx_delta) is not None
        upstream_delta = rest_endpoint.on_update(tx, tx_delta, 5)
        self.assertEqual(tx.data_response.code, 450)

    def testPutBlobTimeout(self):
        rest_endpoint = RestEndpoint(static_base_url=self.static_base_url,
                                     timeout_data=1, chunk_size=4)

        self.responses.append(Response(
            http_resp = '201 created',
            content_type = 'application/json',
            body = json.dumps({
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'}]}),
            location = '/transactions/123'))
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')])
        upstream_delta = rest_endpoint.on_update(tx, tx.copy())


        tx_delta = TransactionMetadata(body_blob=InlineBlob(b'hello'))
        assert tx.merge_from(tx_delta) is not None
        self.responses.append(Response(
            http_resp = '201 created',
            content_type = 'application/json',
            body = json.dumps({'body': '/blob/123'}),
            location = '/transactions/124'))

        upstream_delta = rest_endpoint.on_update(tx, tx_delta)
        self.assertEqual(tx.data_response.code, 450)


    def testFilterApi(self):
        rest_endpoint = RestEndpoint(static_base_url=self.static_base_url,
                                     min_poll=0.1,
                                     chunk_size=8)

        # POST
        self.responses.append(Response(
            http_resp = '201 created',
            content_type = 'application/json',
            body = json.dumps({
                'mail_from': {},
                'rcpt_to': [{}],
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'}]}),
            location = '/transactions/123'))

        logging.debug('testFilterApi envelope')
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')])
        upstream_delta = rest_endpoint.on_update(tx, tx.copy(), 5)

        req = self.requests.pop(0)
        self.assertEqual(req.method, 'POST')
        self.assertEqual(req.path, '/transactions')

        for t in [upstream_delta, tx]:
            self.assertEqual(t.mail_response.code, 201)
            self.assertEqual([r.code for r in t.rcpt_response], [202])

        logging.debug('testFilterApi !last append')
        # incomplete -> noop
        tx_delta = TransactionMetadata()
        tx.body_blob = tx_delta.body_blob = CompositeBlob()
        b = InlineBlob(b'hello, ')
        tx.body_blob.append(b, 0, b.len())
        upstream_delta = rest_endpoint.on_update(tx, tx_delta, 5)
        self.assertIsNone(upstream_delta.mail_response)
        self.assertFalse(upstream_delta.rcpt_response)
        self.assertIsNone(upstream_delta.data_response)

        logging.debug('testFilterApi last append')

        b = InlineBlob(b'world!')
        tx.body_blob.append(b, 0, b.len(), True)

        # POST /blob
        self.responses.append(Response(
            http_resp = '201 created',
            content_type = 'application/json',
            location = '/blob/xyz'))

        # PUT /blob/xyz
        # PUT 0-11/12
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            content_range=ContentRange(
                'bytes', 0, tx.body_blob.len(), tx.body_blob.len())))

        # PATCH /transactions/123 {'body': '/blob/xyz'}
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = json.dumps({
                'mail_from': {},
                'rcpt_to': [{}],
                'body': {},
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'}]})))

        # GET
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = json.dumps({
                'mail_from': {},
                'rcpt_to': [{}],
                'body': {},
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'}],
                'data_response': {'code': 203, 'message': 'ok'}})))

        # same tx/delta
        upstream_delta = rest_endpoint.on_update(tx, tx_delta, timeout=5)
        logging.debug('testFilterApi after patch body %s', tx)
        self.assertEqual(tx.data_response.code, 203)

        req = self.requests.pop(0)
        self.assertEqual(req.method, 'POST')
        self.assertEqual(req.path, '/blob')
        req = self.requests.pop(0)
        self.assertEqual(req.method, 'PUT')
        self.assertEqual(req.path, '/blob/xyz')
        req = self.requests.pop(0)
        self.assertEqual(req.method, 'PATCH')
        self.assertEqual(req.path, '/transactions/123')
        req = self.requests.pop(0)
        self.assertEqual(req.method, 'GET')
        self.assertEqual(req.path, '/transactions/123')



    def testFilterApiMultiRcpt(self):
        rest_endpoint = RestEndpoint(static_base_url=self.static_base_url,
                                     min_poll=0.1)

        # POST
        mail_resp = MailResponse(201)
        rcpt0_resp = MailResponse(202)
        rcpt1_resp = MailResponse(203)
        self.responses.append(Response(
            http_resp = '201 created',
            content_type = 'application/json',
            body = json.dumps({
                'mail_from': {},
                'rcpt_to': [{}],
                'mail_response': mail_resp.to_json(),
                'rcpt_response': [rcpt0_resp.to_json()]}),
            location = '/transactions/123'))

        tx = TransactionMetadata(
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob')])
        upstream_delta = rest_endpoint.on_update(tx, tx.copy())
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [rcpt0_resp.code])

        # PATCH
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = json.dumps({
                'mail_from': {},
                'rcpt_to': [{}, {}],
                'mail_response': mail_resp.to_json(),
                'rcpt_response': [rcpt0_resp.to_json()]})))
        # GET
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = json.dumps({
                'mail_from': {},
                'rcpt_to': [{}, {}],
                'mail_response': mail_resp.to_json(),
                'rcpt_response': [rcpt0_resp.to_json(),
                                  rcpt1_resp.to_json()]})))

        tx_delta = TransactionMetadata(rcpt_to=[Mailbox('bob2')])
        assert tx.merge_from(tx_delta) is not None
        upstream_delta = rest_endpoint.on_update(tx, tx_delta)
        logging.debug('RestEndpointTest.testFilterApiMultiRcpt '
                      'after patch 2nd rcpt %s', tx)
        self.assertEqual(upstream_delta.mail_response, None)
        self.assertEqual([r.code for r in upstream_delta.rcpt_response],
                         [rcpt1_resp.code])

        self.assertEqual([r.code for r in tx.rcpt_response], [202, 203])


    def disco(self, h):
        yield HostPort('mx.example.com', 25)

    def test_discovery(self):
        rest_endpoint = RestEndpoint(
            static_base_url=self.static_base_url,
            min_poll=0.1,
            remote_host_resolution = lambda h: self.disco(h))

        # POST
        self.responses.append(Response(
            http_resp = '201 created',
            content_type = 'application/json',
            body = json.dumps({
                'remote_host': ['mx.example.com', 25],
                'mail_from': {},
                'rcpt_to': [{}] }),
            location = '/transactions/123'))

        # GET
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = json.dumps({
                'remote_host': ['mx.example.com', 25],
                'mail_from': {},
                'rcpt_to': [{}],
                'mail_response': {'code': 201, 'message': 'ok'},
                'rcpt_response': [{'code': 202, 'message': 'ok'}] }),
            location = '/transactions/123'))

        tx = TransactionMetadata(
            remote_host = HostPort('example.com', 25),
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob')])
        upstream_delta = rest_endpoint.on_update(tx, tx.copy())
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])


    def test_bad_post_resp(self):
        rest_endpoint = RestEndpoint(static_base_url=self.static_base_url,
                                     min_poll=0.1,
                                     chunk_size=8)

        # POST
        self.responses.append(Response(
            http_resp = '201 created',
            content_type = 'application/json',
            body = json.dumps({
                'mail_from': {'m': 'alice'}}),
            location = '/transactions/123'))

        logging.debug('test_bad_delta envelope')
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')])
        upstream_delta = rest_endpoint.on_update(tx, tx.copy(), 5)

        req = self.requests.pop(0)  # POST /transactions
        self.assertEqual(tx.mail_response.code, 450)



if __name__ == '__main__':
    unittest.main()
