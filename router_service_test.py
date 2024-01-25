from typing import Any, Tuple
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
                       'output_chain': 'outbound-gw',
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
    'endpoint': [
        {
            'name': 'outbound-gw',
            'msa': True,
            'chain': [{'filter': 'sync'}]
        },
        {
            'name': 'inbound-gw',
            'chain': [{'filter': 'sync'}]
        },
    ],
    'storage': {}
}

class RouterServiceTest(unittest.TestCase):
    def get_endpoint(self):
        return self.endpoint
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

        # find a free port
        with socketserver.TCPServer(("localhost", 0), lambda x,y,z: None) as s:
            self.port = s.server_address[1]
        root_yaml['rest_listener']['addr'] = ('127.0.0.1', self.port)
        self.router_url = 'http://localhost:%d' % self.port
        self.endpoint = SyncEndpoint()
        self.config = Config()
        self.config.inject_yaml(root_yaml)
        self.config.inject_filter('sync', lambda yaml, next: self.get_endpoint())
        self.service = Service(config=self.config)
        self.service_thread = Thread(
            daemon=True,
            target=lambda: self.service.main())
        self.service_thread.start()

        # probe for startup
        self.endpoint.set_mail_response(Response(250))
        self.endpoint.add_rcpt_response(Response())
        for i in range(1,5):
            try:
                # use an invalid host per Wiring so output will fail
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

        self.endpoint = SyncEndpoint()

        # gc the probe request so it doesn't cause confusion later
        # TODO rest_endpoint.abort()
        self.assertEqual(1, self.service._gc_inflight(0))


    def tearDown(self):
        self.service.shutdown()
        self.service_thread.join()

    def dump_db(self):
        for l in self.service.storage.db.iterdump():
            print(l)

    def test_retry(self):
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, http_host='outbound-gw',
            wait_response=False)
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
        self.assertTrue(rest_endpoint.set_durable().ok())


        # set upstream responses so output tempfails at rcpt
        self.endpoint.set_mail_response(Response(201))
        self.endpoint.add_rcpt_response(Response(456))

        self.assertEqual(1, self.service._dequeue())

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


        # set upstream responses so output (retry) succeeds
        self.endpoint = SyncEndpoint()
        self.assertTrue(self.service._dequeue())

        # upstream success, retry succeeds, propagates down to rest
        self.endpoint.set_mail_response(Response(201))
        self.endpoint.add_rcpt_response(Response(202))
        self.endpoint.add_data_response(None)
        self.endpoint.add_data_response(Response(203))

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

    # TODO refactor the rest of these tests to use the "raw request"
    # subset of RestEndpoint instead of the Filter
    # api/protocol/personality

    # Moreover, many of these are testing set_durable() which was
    # really a special hook for the smtp gw "multi-rcpt sync/same
    # resp" optimization which has moved to the internal exploder
    # flow. Native rest clients will probably always specify it in the
    # initial POST/never need to PATCH it later?

    # set durable after upstream ok/perm -> noop
    def test_durable_after_upstream_success(self):
        start_endpoint = RestEndpoint(
            static_base_url=self.router_url, http_host='outbound-gw',
            wait_response=False)

        self.endpoint.set_mail_response(Response(250))
        self.endpoint.add_rcpt_response(Response(234))
        self.endpoint.add_data_response(Response(256))

        tx = TransactionMetadata()
        tx.mail_from = Mailbox('alice')
        tx.rcpt_to = [Mailbox('bob')]
        start_resp = start_endpoint.on_update(tx)
        start_endpoint.append_data(last=True, blob=InlineBlob(b'hello'))

        self.assertEqual(1, self.service._dequeue())

        resp = start_endpoint.get_json_response(3, 'data_response')
        logging.info('test_durable_after_upstream_done resp %s', resp)
        self.assertIsNotNone(resp)
        self.assertEqual(256, resp.code)

        # DONE/DELIVERED -> noop
        self.assertTrue(start_endpoint.set_durable().ok())

    def test_durable_after_upstream_perm(self):
        start_endpoint = RestEndpoint(
            static_base_url=self.router_url, http_host='outbound-gw',
            wait_response=False)

        self.endpoint.set_mail_response(Response(250))
        self.endpoint.add_rcpt_response(Response(234))
        self.endpoint.add_data_response(Response(556))

        tx = TransactionMetadata()
        tx.mail_from = Mailbox('alice')
        tx.rcpt_to = [Mailbox('bob')]
        start_resp = start_endpoint.on_update(tx)
        start_endpoint.append_data(last=True, blob=InlineBlob(b'hello'))

        self.assertEqual(1, self.service._dequeue())

        resp = start_endpoint.get_json_response(3, 'data_response')
        logging.info('test_durable_after_upstream_done resp %s', resp)
        self.assertIsNotNone(resp)
        self.assertEqual(556, resp.code)

        # DONE/PERMFAIL -> failed precondition
        self.assertFalse(start_endpoint.set_durable().ok())


    def test_durable_after_upstream_temp(self):
        start_endpoint = RestEndpoint(
            static_base_url=self.router_url, http_host='outbound-gw',
            wait_response=False)

        self.endpoint.set_mail_response(Response(250))
        self.endpoint.add_rcpt_response(Response(234))
        self.endpoint.add_data_response(Response(456))

        tx = TransactionMetadata()
        tx.mail_from = Mailbox('alice')
        tx.rcpt_to = [Mailbox('bob')]
        start_resp = start_endpoint.on_update(tx)
        start_endpoint.append_data(last=True, blob=InlineBlob(b'hello'))

        self.assertEqual(1, self.service._dequeue())

        resp = start_endpoint.get_json_response(2, 'data_response')
        logging.info('test_durable_after_upstream_temp resp %s', resp)
        self.assertIsNotNone(resp)
        self.assertEqual(resp.code, 456)
        self.assertTrue(start_endpoint.set_durable().ok())

    def test_durable_upstream_inflight(self):
        start_endpoint = RestEndpoint(
            static_base_url=self.router_url, http_host='outbound-gw',
            wait_response=False)

        self.endpoint.set_mail_response(Response(250))
        self.endpoint.add_rcpt_response(Response(234))

        tx = TransactionMetadata()
        tx.mail_from = Mailbox('alice')
        tx.rcpt_to = [Mailbox('bob')]
        start_resp = start_endpoint.on_update(tx)
        start_endpoint.append_data(last=True, blob=InlineBlob(b'hello'))

        self.assertEqual(1, self.service._dequeue())

        resp = start_endpoint.get_json_response(2, 'rcpt_response')
        logging.info('test_durable_upstream_inflight start resp %s', resp)
        self.assertIsNotNone(resp)
        self.assertEqual(resp[0].code, 234)

        self.assertTrue(start_endpoint.set_durable().ok())

        self.endpoint.set_mail_response(Response(250))
        self.endpoint.add_rcpt_response(Response(245))
        self.endpoint.add_data_response(Response(267))

        resp = start_endpoint.get_json_response(2, 'data_response')
        logging.info('test_durable_upstream_inflight final resp %s', resp)
        self.assertIsNotNone(resp)
        self.assertEqual(resp.code, 267)

    # set durable after ttl -> failed precondition
    def test_idle_gc_oneshot_temp(self):
        start_endpoint = RestEndpoint(
            static_base_url=self.router_url, http_host='outbound-gw',
            wait_response=False)
        transaction_url = start_endpoint.transaction_url

        self.endpoint.set_mail_response(Response())
        self.endpoint.add_rcpt_response(Response(234))
        self.endpoint.add_data_response(Response(456))

        tx = TransactionMetadata()
        tx.mail_from = Mailbox('alice')
        tx.rcpt_to = [Mailbox('bob')]
        start_resp = start_endpoint.on_update(tx)
        start_endpoint.append_data(last=True, blob=InlineBlob(b'hello'))

        self.assertEqual(1, self.service._dequeue())

        resp = start_endpoint.get_json_response(5, 'data_response')
        logging.info('test_idle_gc resp %s', resp)
        self.assertIsNotNone(resp)
        self.assertEqual(resp.code, 456)

        logging.info('test_idle_gc set durable')

        self.assertEqual(1, self.service._gc_inflight(0))
        self.assertTrue(start_endpoint.set_durable().err())


    # xxx this only aborts insert/oneshot_temp, not inflight
    # do you want this to be the thing that makes output abort or
    # should it time out itself?
    def test_idle_gc_oneshot_inflight(self):
        start_endpoint = RestEndpoint(
            static_base_url=self.router_url, http_host='outbound-gw',
            wait_response=False)
        transaction_url = start_endpoint.transaction_url

        self.endpoint.set_mail_response(Response(250))
        self.endpoint.add_rcpt_response(Response(234))
        # no data response -> output blocks

        # output blocks waiting on rcpt_to
        tx = TransactionMetadata()
        tx.mail_from = Mailbox('alice')
        tx.rcpt_to = [Mailbox('bob')]
        start_resp = start_endpoint.on_update(tx)
        start_endpoint.append_data(last=True, blob=InlineBlob(b'hello'))

        self.assertEqual(1, self.service._dequeue())

        # output waiting for rcpt_to so this never returns anything
        resp = start_endpoint.get_json(2)
        logging.info('test_idle_gc resp %s', resp)
        self.assertNotIn('final_response', resp)
        #self.assertEqual(resp.code, 400)

        logging.info('test_idle_gc set durable')

        self.assertEqual(1, self.service._gc_inflight(0))
        self.assertTrue(start_endpoint.set_durable().err())
        time.sleep(1)  # logging


if __name__ == '__main__':
    unittest.main()
