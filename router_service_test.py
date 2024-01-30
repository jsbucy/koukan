from typing import Any, List, Tuple
from threading import Thread, Lock, Condition
import logging
import unittest
import socketserver
import time

from requests.exceptions import ConnectionError

from router_service import Service
from rest_endpoint import RestEndpoint
from response import Response
from blob import InlineBlob
from config import Config
from fake_endpoints import SyncEndpoint
from filter import Mailbox, TransactionMetadata

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
            'chain': [{'filter': 'sync'}]
        },
    ],
    'storage': {}
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
        self.config.inject_filter('sync', lambda yaml, next: self.get_endpoint())
        self.service = Service(config=self.config)
        self.service_thread = Thread(
            daemon=True,
            target=lambda: self.service.main())
        self.service_thread.start()

        # probe for startup
        for i in range(1,5):
            try:
                # use an invalid host per config so output will fail
                # immediately and not consume responses from endpoint
                rest_endpoint = RestEndpoint(
                    static_base_url=self.router_url,
                    http_host='probe',
                    wait_response=False)
                tx = TransactionMetadata()
                tx.mail_from = Mailbox('probe-from%d' % i)
                tx.rcpt_to = [Mailbox('probe-to%d' % i)]
                start_resp = rest_endpoint.on_update(tx)
            except ConnectionError:
                    time.sleep(0.1)
            else:
                break
        else:
            self.fail('service not ready')

        time.sleep(1)

        # gc the probe request so it doesn't cause confusion later
        # TODO rest_endpoint.abort()
        self.assertEqual(1, self.service._gc_inflight(0))


    def tearDown(self):
        self.service.shutdown()
        self.service_thread.join()

    def dump_db(self):
        for l in self.service.storage.db.iterdump():
            print(l)

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

        rest_resp = rest_endpoint._update(
            TransactionMetadata(mail_from=Mailbox('alice')))
        tx_json = rest_resp.json()
        logging.debug('RouterServiceTest.test_retry patch mail_from %s %s',
                      rest_resp, tx_json)
        self.assertEqual(tx_json.get('mail_response', None), {})
        self.assertEqual(tx_json.get('rcpt_response', None), None)
        self.assertEqual(tx_json.get('data_response', None), None)
        self.assertEqual(tx_json.get('last', None), None)

        tx_json = rest_endpoint.get_json(1.2)
        logging.debug('RouterServiceTest.test_retry get after mail_from %s',
                      tx_json)

        rest_resp = rest_endpoint._update(
            TransactionMetadata(rcpt_to = [Mailbox('bob')]))
        tx_json = rest_resp.json()
        logging.debug('RouterServiceTest.test_retry patch rcpt_to %s %s',
                      rest_resp, tx_json)
        self.assertEqual(tx_json.get('mail_response', None), {})
        self.assertEqual(tx_json.get('rcpt_response', None), [{}])
        self.assertEqual(tx_json.get('data_response', None), None)
        self.assertEqual(tx_json.get('last', None), None)

        tx_json = rest_endpoint.get_json(1.2)
        logging.debug('RouterServiceTest.test_retry get after rcpt_to %s',
                      tx_json)

        rest_resp = rest_endpoint._append_inline(
            last=False, blob=InlineBlob(b'hello'))
        tx_json = rest_resp.json()
        logging.debug('RouterServiceTest.test_retry post append %s %s',
                      rest_resp, tx_json)
        self.assertEqual(tx_json.get('mail_response', None), {})
        self.assertEqual(tx_json.get('rcpt_response', None), [{}])
        self.assertEqual(tx_json.get('data_response', None), {})
        self.assertEqual(tx_json.get('last', None), False)

        rest_resp = rest_endpoint._append_blob(
            last=True, blob_uri=None)
        logging.debug('RouterServiceTest.test_retry append blob %s',
                      rest_resp)
        tx_json = rest_resp.json()
        blob_uri = tx_json.get('uri', None)
        rest_resp = rest_endpoint._put_blob(
            blob=InlineBlob('world'), uri=blob_uri)


        logging.debug('RouterServiceTest.test_retry get after append %s',
                      tx_json)
        # propagate this timeout to the GET handler which currently
        # uses static 1s!
        rest_endpoint.get_json(1.1)
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
        rcpt_resp = [ Response.from_json(r).code
                     for r in tx_json.get('rcpt_response', []) ]
        self.assertEqual(rcpt_resp, [456])
        self.assertEqual(tx_json.get('data_response', None), {})
        self.assertEqual(tx_json.get('last', None), True)

        upstream_endpoint = SyncEndpoint()
        self.add_endpoint(upstream_endpoint)
        self.assertTrue(self.service._dequeue())

        # set upstream responses so output (retry) succeeds
        # upstream success, retry succeeds, propagates down to rest
        upstream_endpoint.set_mail_response(Response(201))
        upstream_endpoint.add_rcpt_response(Response(202))
        upstream_endpoint.add_data_response(None)
        upstream_endpoint.add_data_response(Response(203))

        tx_json = rest_endpoint.get_json(1.2)
        logging.debug('RouterServiceTest.test_retry get after second attempt '
                      '%s %s', rest_resp, tx_json)
        mail_resp = Response.from_json(tx_json.get('mail_response', None))
        self.assertEqual(mail_resp.code, 201)
        rcpt_resp = [ Response.from_json(r).code
                     for r in tx_json.get('rcpt_response', []) ]
        self.assertEqual(rcpt_resp, [202])
        mail_resp = Response.from_json(tx_json.get('data_response', None))
        self.assertEqual(mail_resp.code, 203)
        self.assertEqual(tx_json.get('last', None), True)

    def _update_tx(self, endpoint, tx):
        endpoint.on_update(tx)

    # exploder multi rcpt
    def testExploderMultiRcpt(self):
        logging.info('testExploderMultiRcpt')
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, http_host='smtp-msa')


        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        t = Thread(target = lambda: self._update_tx(rest_endpoint, tx))
        t.start()

        time.sleep(0.1)
        # exploder tx
        self.assertTrue(self.service._dequeue())

        t.join(timeout=1)
        self.assertFalse(t.is_alive())

        # no rcpt -> buffered
        self.assertEqual(tx.mail_response.code, 250)


        tx = TransactionMetadata(rcpt_to=[Mailbox('bob')])
        t = Thread(target = lambda: self._update_tx(rest_endpoint, tx))
        t.start()

        # upstream tx #1
        upstream_endpoint = SyncEndpoint()
        self.add_endpoint(upstream_endpoint)
        self.assertTrue(self.service._dequeue())

        upstream_endpoint.set_mail_response(Response(250))
        upstream_endpoint.add_rcpt_response(Response(202))

        t.join(timeout=1)
        self.assertFalse(t.is_alive())
        self.assertEqual([r.code for r in tx.rcpt_response], [202])


        tx = TransactionMetadata(rcpt_to=[Mailbox('bob2')])
        t = Thread(target = lambda: self._update_tx(rest_endpoint, tx))
        t.start()

        # upstream tx #2
        upstream_endpoint2 = SyncEndpoint()
        self.add_endpoint(upstream_endpoint2)
        self.assertTrue(self.service._dequeue())

        upstream_endpoint2.set_mail_response(Response(250))
        upstream_endpoint2.add_rcpt_response(Response(203))

        t.join(timeout=1)
        self.assertFalse(t.is_alive())
        self.assertEqual([r.code for r in tx.rcpt_response], [203])




if __name__ == '__main__':
    unittest.main()
