from typing import Any, List, Optional, Tuple
from threading import Thread, Lock, Condition
import logging
import unittest
import socketserver
import time

from requests.exceptions import ConnectionError

from router_service import Service
from rest_endpoint import RestEndpoint
from response import Response
from blob import CompositeBlob, InlineBlob
from config import Config
from fake_endpoints import SyncEndpoint
from filter import HostPort, Mailbox, TransactionMetadata

# XXX exercise exploder flows
root_yaml = {
    'global': {
        'use_gunicorn': False,
        'tx_idle_timeout': 5,
        'gc_interval': None,
        'dequeue': False,
    },
    'rest_listener': {
    },
    'endpoint': [
        {
            'name': 'smtp-msa',
            'msa': True,
            'chain': [{'filter': 'exploder',
                       'output_chain': 'submission',
                       'msa': True}]
        },
        {
            'name': 'submission',
            'msa': True,
            'chain': [{'filter': 'sync'}]
        },
        {
            'name': 'smtp-in',
            'msa': True,
            'chain': [{'filter': 'exploder',
                       'output_chain': 'inbound-gw',
                       'msa': False}]
        },
        {
            'name': 'inbound-gw',
            'msa': True,
            'chain': [{'filter': 'sync'}]
        },
    ],
    'storage': {
        'engine': 'sqlite_memory'
    }
}

class RouterServiceTest(unittest.TestCase):
    lock : Lock
    cv : Condition
    endpoints : List[SyncEndpoint]

    def get_endpoint(self):
        with self.lock:
            self.cv.wait_for(lambda: bool(self.endpoints))
            return self.endpoints.pop(0)

    def add_endpoint(self, endpoint):
        with self.lock:
            self.endpoints.append(endpoint)
            self.cv.notify_all()

    def setUp(self):
        self.lock = Lock()
        self.cv = Condition(self.lock)

        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

        # find a free port
        with socketserver.TCPServer(("localhost", 0), lambda x,y,z: None) as s:
            self.port = s.server_address[1]
        root_yaml['rest_listener']['addr'] = ('127.0.0.1', self.port)
        self.router_url = 'http://localhost:%d' % self.port
        self.endpoints = []
        self.config = Config()
        self.config.inject_yaml(root_yaml)
        self.config.inject_filter('sync',
                                  lambda yaml, next: self.get_endpoint())
        self.service = Service(config=self.config)
        self.service_thread = Thread(
            daemon=True,
            target=lambda: self.service.main())
        self.service_thread.start()


        self.assertTrue(self.service.wait_started(1))

        # probe for startup
        s = SyncEndpoint()
        s.set_mail_response(Response())
        self.add_endpoint(s)

        for i in range(1,3):
            rest_endpoint = RestEndpoint(
                static_base_url=self.router_url,
                http_host='inbound-gw')
            tx = TransactionMetadata(
                mail_from = Mailbox('probe-from%d' % i))
            t = self.start_tx_update(rest_endpoint, tx)
            logging.info('setUp %s', tx.mail_response)
            self.service._dequeue()
            self.join_tx_update(t)
            if tx.mail_response.ok():
                break
            time.sleep(0.1)
        else:
            self.fail('service not ready')

        self.assertFalse(self.endpoints)

        # gc the probe request so it doesn't cause confusion later
        # TODO rest_endpoint.abort()
        self.assertEqual(1, self.service._gc_inflight(0))

    def tearDown(self):
        self.service.shutdown()
        self.service_thread.join()

    def dump_db(self):
        for l in self.service.storage.db.iterdump():
            print(l)

    def assertRcptCodesEqual(self, responses : List[Optional[Response]],
                             expected_codes):
        self.assertEqual([r.code if r else None for r in responses],
                         expected_codes)

    def assertRcptJsonCodesEqual(self, resp_json, expected_codes):
        self.assertRcptCodesEqual(
            [Response.from_json(r) for r in resp_json.get('rcpt_response', [])],
            expected_codes)

    # native rest
    # first attempt tempfails at rcpt
    # retry+success
    def test_retry(self):
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, http_host='submission')
        rest_resp = rest_endpoint._start(TransactionMetadata(), {})
        tx_json = rest_resp.json()
        logging.debug('RouterServiceTest.test_retry create %s %s',
                      rest_resp, tx_json)
        self.assertEqual(tx_json.get('mail_response', None), None)
        self.assertEqual(tx_json.get('rcpt_response', None), None)
        self.assertEqual(tx_json.get('data_response', None), None)
        self.assertEqual(tx_json.get('last', None), None)

        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        rest_resp = rest_endpoint._update(tx.to_json())
        tx_json = rest_resp.json()
        logging.debug('RouterServiceTest.test_retry patch mail_from %s %s',
                      rest_resp, tx_json)
        self.assertEqual(tx_json.get('mail_response', None), {})
        self.assertEqual(tx_json.get('rcpt_response', None), None)
        self.assertEqual(tx_json.get('data_response', None), None)
        self.assertEqual(tx_json.get('last', None), None)

        tx_json = rest_endpoint.get_json()
        logging.debug('RouterServiceTest.test_retry get after mail_from %s',
                      tx_json)
        tx = TransactionMetadata(rcpt_to = [Mailbox('bob')])
        rest_resp = rest_endpoint._update(tx.to_json())
        tx_json = rest_resp.json()
        logging.debug('RouterServiceTest.test_retry patch rcpt_to %s %s',
                      rest_resp, tx_json)
        self.assertEqual(tx_json.get('mail_response', None), {})
        self.assertEqual(tx_json.get('rcpt_response', None), [{}])
        self.assertEqual(tx_json.get('data_response', None), None)
        self.assertEqual(tx_json.get('last', None), None)

        tx_json = rest_endpoint.get_json()
        logging.debug('RouterServiceTest.test_retry get after rcpt_to %s',
                      tx_json)

        body_blob = CompositeBlob()
        b = InlineBlob(b'hello')
        body_blob.append(b, 0, b.len())
        tx = TransactionMetadata(body_blob=body_blob)
        rest_endpoint.on_update(tx)
        self.assertIsNone(tx.data_response)

        b = InlineBlob(b'world')
        body_blob.append(b, 0, b.len(), True)
        tx = TransactionMetadata(body_blob=body_blob)
        rest_endpoint.on_update(tx)
        self.assertTrue(tx.data_response.temp())

        logging.debug('RouterServiceTest.test_retry get after append %s',
                      tx_json)
        rest_endpoint.get_json()
        rest_endpoint.on_update(TransactionMetadata(max_attempts=100))

        upstream_endpoint = SyncEndpoint()
        self.add_endpoint(upstream_endpoint)
        self.assertTrue(self.service._dequeue())

        # set upstream responses so output tempfails at rcpt
        upstream_endpoint.set_mail_response(Response(201))
        upstream_endpoint.add_rcpt_response(Response(456))


        tx_json = rest_endpoint.get_json(1.2)
        logging.debug('RouterServiceTest.test_retry get after first attempt '
                      '%s %s', rest_resp, tx_json)
        mail_resp = Response.from_json(tx_json.get('mail_response', None))
        self.assertEqual(mail_resp.code, 201)
        self.assertRcptJsonCodesEqual(tx_json, [456])
        self.assertEqual(tx_json.get('data_response', None), {})
        #self.assertEqual(tx_json.get('last', None), True)

        upstream_endpoint = SyncEndpoint()
        self.add_endpoint(upstream_endpoint)
        self.assertTrue(self.service._dequeue())

        # set upstream responses so output (retry) succeeds
        # upstream success, retry succeeds, propagates down to rest
        upstream_endpoint.set_mail_response(Response(201))
        upstream_endpoint.add_rcpt_response(Response(202))
        #upstream_endpoint.add_data_response(None)
        upstream_endpoint.add_data_response(Response(203))

        tx_json = rest_endpoint.get_json(1.2)
        logging.debug('RouterServiceTest.test_retry get after second attempt '
                      '%s %s', rest_resp, tx_json)
        mail_resp = Response.from_json(tx_json.get('mail_response', None))
        self.assertEqual(mail_resp.code, 201)
        self.assertRcptJsonCodesEqual(tx_json, [202])
        data_resp = Response.from_json(tx_json.get('data_response', None))
        self.assertEqual(data_resp.code, 203)
        #self.assertEqual(tx_json.get('last', None), True)

    def _update_tx(self, endpoint, tx):
        endpoint.on_update(tx)

    def start_tx_update(self, rest_endpoint, tx):
        t = Thread(target = lambda: self._update_tx(rest_endpoint, tx))
        t.start()
        return t

    def join_tx_update(self, t):
        t.join(timeout=1)
        self.assertFalse(t.is_alive())

    # xxx need non-exploder test w/filter api, problems in
    # post-exploder/upstream tx won't be reported out synchronously,
    # would potentially bounce
    # and/or get ahold of those tx IDs and verify the status directly

    # exploder multi rcpt
    def test_exploder_multi_rcpt(self):
        logging.info('testExploderMultiRcpt')
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, http_host='smtp-msa')

        logging.info('testExploderMultiRcpt start tx')
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            remote_host=HostPort('1.2.3.4', 12345))
        t = self.start_tx_update(rest_endpoint, tx)

        time.sleep(0.1)
        # exploder tx
        self.assertTrue(self.service._dequeue())

        self.join_tx_update(t)

        # no rcpt -> buffered
        self.assertEqual(tx.mail_response.code, 250)

        logging.info('testExploderMultiRcpt patch rcpt1')
        tx = TransactionMetadata(rcpt_to=[Mailbox('bob')])
        t = self.start_tx_update(rest_endpoint, tx)

        # upstream tx #1
        upstream_endpoint = SyncEndpoint()
        self.add_endpoint(upstream_endpoint)
        self.assertTrue(self.service._dequeue())

        upstream_endpoint.set_mail_response(Response(250))
        upstream_endpoint.add_rcpt_response(Response(202))

        self.join_tx_update(t)
        self.assertRcptCodesEqual(tx.rcpt_response, [202])

        logging.info('testExploderMultiRcpt patch rcpt2')
        tx = TransactionMetadata(rcpt_to=[Mailbox('bob2')])
        t = self.start_tx_update(rest_endpoint, tx)

        # upstream tx #2
        upstream_endpoint2 = SyncEndpoint()
        self.add_endpoint(upstream_endpoint2)
        self.assertTrue(self.service._dequeue())

        upstream_endpoint2.set_mail_response(Response(250))
        upstream_endpoint2.add_rcpt_response(Response(203))

        self.join_tx_update(t)
        self.assertRcptCodesEqual(tx.rcpt_response, [203])

        # output of exploder tx buffers whole payload?
        #upstream_endpoint.add_data_response(None)
        upstream_endpoint.add_data_response(Response(204))
        #upstream_endpoint2.add_data_response(None)
        upstream_endpoint2.add_data_response(Response(204))

        logging.info('testExploderMultiRcpt patch body_blob')

        tx = TransactionMetadata(
            body_blob=InlineBlob(b'Hello, World!'))
        rest_endpoint.on_update(tx)
        self.assertEqual(tx.data_response.code, 204)


if __name__ == '__main__':
    unittest.main()
