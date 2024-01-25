from typing import List
import unittest
import logging
import socketserver
from threading import Thread
import time
import json

from wsgiref.simple_server import make_server
import wsgiref.util
from werkzeug.datastructures import ContentRange
import werkzeug.http

from rest_endpoint import RestEndpoint
from filter import TransactionMetadata, Mailbox
from blob import InlineBlob
from response import Response as MailResponse

class Request:
    path = None
    content_type = None
    content_range = None
    body = None
    def __init__(self,
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
    def __init__(self,
                 http_resp = None,
                 content_type = None,
                 body = None,
                 content_range = None):
        self.http_resp = http_resp
        self.content_type = content_type
        self.body = body
        self.content_range = content_range

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

    def assertEqualRange(self, lhs, rhs):
        self.assertTrue(eq_range(lhs, rhs))

    def wsgi_app(self, environ, start_response):
        #logging.debug(environ)
        req = Request()
        req.path = environ['PATH_INFO']
        req.content_type = environ['CONTENT_TYPE']
        if range_header := environ.get('HTTP_CONTENT_RANGE', None):
            req.content_range = (
                werkzeug.http.parse_content_range_header(range_header))

        if ((wsgi_input := environ.get('wsgi.input', None)) and
            (length := environ.get('CONTENT_LENGTH', None))):
            req.body = wsgi_input.read(int(length))
        self.requests.append(req)

        resp = self.responses.pop(0)
        resp_headers = []
        if resp.content_type:
            resp_headers.append(('content-type', resp.content_type))
        if resp.content_range:
            resp_headers.append(('content-range', str(resp.content_range)))
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
            http_resp = '200 ok',
            content_type = 'application/json',
            body = json.dumps({'url': '/transactions/123'})))
        rest_resp = rest_endpoint._start(tx, tx.to_json())
        self.assertEqual(rest_resp.status_code, 200)
        req = self.requests.pop(0)
        self.assertEqual(req.path, '/transactions')
        self.assertEqual(req.body, b'{}')
        self.assertEqual(req.content_type, 'application/json')

        self.assertEqual(rest_endpoint.transaction_url,
                         self.static_base_url + '/transactions/123')

    def testUpdate(self):
        rest_endpoint = RestEndpoint(
            transaction_url=self.static_base_url + '/transactions/124')

        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = json.dumps({'url': '/transactions/124',
                               'mail_response': {}})))

        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        rest_resp = rest_endpoint._update(tx)
        self.assertEqual(rest_resp.status_code, 200)
        req = self.requests.pop(0)
        self.assertEqual(req.path, '/transactions/124')
        self.assertEqual(
            req.body,
            json.dumps({'mail_from': {'m': 'alice'}}).encode('utf-8'))
        self.assertEqual(req.content_type, 'application/json')
        self.assertEqual(rest_resp.json(),
                         {'url': '/transactions/124',
                          'mail_response': {}})

    def testAppendInline(self):
        rest_endpoint = RestEndpoint(
            transaction_url=self.static_base_url + '/transactions/125')

        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = json.dumps({'url': '/transactions/124',
                               'mail_response': {},
                               'data_response': {},
                               'last': True})))

        rest_resp = rest_endpoint._append_inline(True, InlineBlob(b'hello'))
        self.assertEqual(rest_resp.status_code, 200)
        req = self.requests.pop(0)
        self.assertEqual(req.path, '/transactions/125/appendData')
        resp_json = {'last': True, 'd': 'hello'}
        self.assertEqual(
            req.body, json.dumps(resp_json).encode('utf-8'))
        self.assertEqual(req.content_type, 'application/json')

        self.assertEqual(rest_resp.json(), {
            'url': '/transactions/124',
            'mail_response': {},
            'data_response': {},
            'last': True})

    def testAppendBlob(self):
        rest_endpoint = RestEndpoint(
            transaction_url=self.static_base_url + '/transactions/126')

        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = json.dumps({'url': '/transactions/124',
                               'mail_response': {},
                               'data_response': {},
                               'last': True,
                               'uri': '/blob/127'})))

        rest_resp = rest_endpoint._append_blob(True, None)
        self.assertEqual(rest_resp.status_code, 200)
        resp_json = {'last': True, 'uri': None}
        req = self.requests.pop(0)
        self.assertEqual(req.path, '/transactions/126/appendData')
        self.assertEqual(req.body, json.dumps(resp_json).encode('utf-8'))
        self.assertEqual(req.content_type, 'application/json')
        self.assertEqual(rest_resp.json(), {
            'url': '/transactions/124',
            'mail_response': {},
            'data_response': {},
            'last': True,
            'uri': '/blob/127'})

    def testPutBlobChunk(self):
        rest_endpoint = RestEndpoint(static_base_url=self.static_base_url)

        self.responses.append(Response(
            http_resp = '200 ok'))

        rest_endpoint._put_blob_chunk('/blob/127', 128, b'hello', False)
        req = self.requests.pop(0)
        self.assertEqual(req.content_range.start, 128)
        self.assertEqual(req.content_range.stop, 133)
        self.assertEqual(req.content_range.length, None)

        self.responses.append(Response(
            http_resp = '200 ok'))
        rest_endpoint._put_blob_chunk('/blob/127', 128, b'hello', True)
        req = self.requests.pop(0)
        self.assertEqual(req.content_range.start, 128)
        self.assertEqual(req.content_range.stop, 133)
        self.assertEqual(req.content_range.length, 133)

    # Filter api
    def testFilterApi(self):
        rest_endpoint = RestEndpoint(static_base_url=self.static_base_url,
                                     min_poll=0.1)

        # start tx w/empty envelope

        # POST
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = json.dumps({'url': '/transactions/123'})))

        tx = TransactionMetadata()
        rest_endpoint.on_update(tx)

        req = self.requests.pop(0)  # POST
        self.assertEqual(req.body, b'{}')


        # on_update() w/mail_from

        # PATCH
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = json.dumps({'url': '/transactions/123',
                               'mail_response': {}})))
        # GET
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = json.dumps({'url': '/transactions/123',
                               'mail_response': {}})))
        # GET
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = json.dumps(
                {'url': '/transactions/123',
                 'mail_response': {'code': 201, 'message': 'ok'}})))

        tx.mail_from = Mailbox('alice')
        rest_endpoint.on_update(tx)
        self.assertEqual(tx.mail_response.code, 201)

        req = self.requests.pop(0)  # PATCH
        self.assertEqual(req.body, b'{"mail_from": {"m": "alice"}}')

        req = self.requests.pop(0)  # GET
        self.assertEqual(req.body, None)

        req = self.requests.pop(0)  # GET
        self.assertEqual(req.body, None)

        # on_update() w/rcpt_to with inline response
        # POST
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = json.dumps({'url': '/transactions/123',
                               'rcpt_response': [{'code': 202, 'message': 'ok'}]})))
        tx = TransactionMetadata(rcpt_to = [Mailbox('bob')])
        rest_endpoint.on_update(tx)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])

        req = self.requests.pop(0)  # PATCH
        self.assertEqual(req.body, b'{"rcpt_to": [{"m": "bob"}]}')


        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = json.dumps(
                {'url': '/transactions/123',
                 'mail_response': {'code': 201, 'message': 'ok'},
                 'data_response': {},
                 'last': False})))
        rest_endpoint.append_data(False, InlineBlob(b'hello'))
        req = self.requests.pop(0)
        req_json = {'last': False, 'd': 'hello'}
        self.assertEqual(req.body, json.dumps(req_json).encode('utf-8'))


        rest_endpoint.max_inline = 0
        rest_endpoint.chunk_size = 3
        # POST
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = json.dumps(
                {'uri': '/blob/124'})))
        # PUT 0-3/*
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            content_range=ContentRange('bytes',0,3,None)))
        # PUT 3-6/5
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            content_range=ContentRange('bytes',3,5,5)))
        rest_endpoint.append_data(False, InlineBlob(b'world'))
        req = self.requests.pop(0)
        self.assertEqual(req.path, '/transactions/123/appendData')
        req = self.requests.pop(0)
        self.assertEqual(req.path, '/blob/124')
        self.assertEqualRange(req.content_range,
                              ContentRange('bytes', 0, 3, None))
        req = self.requests.pop(0)
        self.assertEqual(req.path, '/blob/124')
        self.assertEqualRange(req.content_range, ContentRange('bytes', 3, 5, 5))


    def testFilterApiMultiRcpt(self):
        rest_endpoint = RestEndpoint(static_base_url=self.static_base_url,
                                     min_poll=0.1)

        mail_resp = MailResponse(201)
        rcpt0_resp = MailResponse(202)
        rcpt1_resp = MailResponse(203)
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = json.dumps({'url': '/transactions/123',
                               'mail_response': mail_resp.to_json(),
                               'rcpt_response': [rcpt0_resp.to_json()]})))

        tx = TransactionMetadata(
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob')])
        rest_endpoint.on_update(tx)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [rcpt0_resp.code])

        # PATCH
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = json.dumps({'url': '/transactions/123',
                               'mail_response': {},
                               'rcpt_response': []})))
        # GET
        self.responses.append(Response(
            http_resp = '200 ok',
            content_type = 'application/json',
            body = json.dumps({'url': '/transactions/123',
                               'mail_response': mail_resp.to_json(),
                               'rcpt_response': [rcpt0_resp.to_json(),
                                                 rcpt1_resp.to_json()]})))

        tx = TransactionMetadata(rcpt_to=[Mailbox('bob2')])
        rest_endpoint.on_update(tx)
        self.assertEqual(tx.mail_response, None)
        self.assertEqual([r.code for r in tx.rcpt_response], [rcpt1_resp.code])


if __name__ == '__main__':
    unittest.main()
