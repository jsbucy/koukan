from typing import Any, Tuple
from threading import Thread, Lock, Condition
import logging
import unittest
import socketserver
import time


from router_service import Service
from rest_endpoint import RestEndpoint
from rest_endpoint import RestEndpoint
from response import Response
from blob import InlineBlob
from config import Config
from fake_endpoints import SyncEndpoint
from filter import TransactionMetadata

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
        self.config.inject_filter('sync', lambda yaml, next: self.endpoint)
        self.service = Service(config=self.config)
        self.service_thread = Thread(
            daemon=True,
            target=lambda: self.service.main())
        self.service_thread.start()

        # probe for startup
        self.endpoint.set_start_response(Response())
        for i in range(1,5):
            try:
                # use an invalid host per Wiring so output will fail
                # immediately and not consume responses from endpoint
                rest_endpoint = RestEndpoint(
                    base_url=self.router_url,
                    http_host='probe',
                    wait_response=False)
                start_resp = rest_endpoint.start(
                    TransactionMetadata(),
                    mail_from='probe-from%d' % i, rcpt_to='probe-to%d' % i)
            except:
                    time.sleep(0.1)
            else:
                break
        else:
            self.fail('service not ready')

        # gc the probe request so it doesn't cause confusion later
        # TODO rest_endpoint.abort()
        self.assertEqual(1, self.service._gc_inflight(0))


    def tearDown(self):
        self.service.shutdown()
        self.service_thread.join()

    def dump_db(self):
        for l in self.service.storage.db.iterdump():
            print(l)

    def test_read_routing(self):
        start_endpoint = RestEndpoint(
            base_url=self.router_url, http_host='outbound-gw',
            wait_response=False,
            msa=True)

        start_resp = start_endpoint.start(
            TransactionMetadata(),
            mail_from='alice', rcpt_to='bob')
        start_endpoint.append_data(last=True, blob=InlineBlob(b'hello'))
        self.assertTrue(start_endpoint.set_durable().ok())

        transaction_url = start_endpoint.transaction_url

        read_endpoint = RestEndpoint(
            base_url=self.router_url, http_host='outbound-gw',
            transaction_url=transaction_url,
            wait_response=True)

        # read from initial inflight
        # xxx rest service blocks on start/final resp whether you want
        # it to or not
        resp_json = read_endpoint.get_json(2)
        logging.info('inflight %s', resp_json)
        self.assertNotIn('start_response', resp_json)
        self.assertNotIn('final_status', resp_json)


        # set start response, initial inflight tempfails
        self.endpoint.set_start_response(Response(456))

        self.assertEqual(1, self.service._dequeue())

        resp = read_endpoint.get_json_response(3, 'start_response')
        logging.info('test_read_routing start resp %s', resp)
        self.assertIsNotNone(resp)
        self.assertEqual(resp.code, 456)

        self.assertEqual(1, self.service._dequeue())

        # upstream success, retry succeeds, propagates down to rest
        self.endpoint.set_start_response(Response(234))
        self.endpoint.add_data_response(Response(245))

        resp = read_endpoint.get_json_response(3, 'final_status')
        self.assertIsNotNone(resp)
        self.assertEqual(resp.code, 245)


    # set durable after upstream ok/perm -> noop
    def test_durable_after_upstream_success(self):
        start_endpoint = RestEndpoint(
            base_url=self.router_url, http_host='outbound-gw',
            wait_response=False,
            msa=True)

        self.endpoint.set_start_response(Response(234))
        self.endpoint.add_data_response(Response(256))

        start_resp = start_endpoint.start(
            TransactionMetadata(),
            mail_from='alice', rcpt_to='bob')
        start_endpoint.append_data(last=True, blob=InlineBlob(b'hello'))

        self.assertEqual(1, self.service._dequeue())

        resp = start_endpoint.get_json_response(3, 'final_status')
        logging.info('test_durable_after_upstream_done resp %s', resp)
        self.assertIsNotNone(resp)
        self.assertEqual(256, resp.code)

        # DONE/DELIVERED -> noop
        self.assertTrue(start_endpoint.set_durable().ok())

    def test_durable_after_upstream_perm(self):
        start_endpoint = RestEndpoint(
            base_url=self.router_url, http_host='outbound-gw',
            wait_response=False,
            msa=True)

        self.endpoint.set_start_response(Response(234))
        self.endpoint.add_data_response(Response(556))

        start_resp = start_endpoint.start(
            TransactionMetadata(),
            mail_from='alice', rcpt_to='bob')
        start_endpoint.append_data(last=True, blob=InlineBlob(b'hello'))

        self.assertEqual(1, self.service._dequeue())

        resp = start_endpoint.get_json_response(3, 'final_status')
        logging.info('test_durable_after_upstream_done resp %s', resp)
        self.assertIsNotNone(resp)
        self.assertEqual(556, resp.code)

        # DONE/PERMFAIL -> failed precondition
        self.assertFalse(start_endpoint.set_durable().ok())


    def test_durable_after_upstream_temp(self):
        start_endpoint = RestEndpoint(
            base_url=self.router_url, http_host='outbound-gw',
            wait_response=False,
            msa=True)

        self.endpoint.set_start_response(Response(234))
        self.endpoint.add_data_response(Response(456))

        start_resp = start_endpoint.start(
            TransactionMetadata(),
            mail_from='alice', rcpt_to='bob')
        start_endpoint.append_data(last=True, blob=InlineBlob(b'hello'))

        self.assertEqual(1, self.service._dequeue())

        resp = start_endpoint.get_json_response(2, 'final_status')
        logging.info('test_durable_after_upstream_temp resp %s', resp)
        self.assertIsNotNone(resp)
        self.assertEqual(resp.code, 456)
        self.assertTrue(start_endpoint.set_durable().ok())

    def test_durable_upstream_inflight(self):
        start_endpoint = RestEndpoint(
            base_url=self.router_url, http_host='outbound-gw',
            wait_response=False,
            msa=True)

        self.endpoint.set_start_response(Response(234))

        start_resp = start_endpoint.start(
            TransactionMetadata(),
            mail_from='alice', rcpt_to='bob')
        start_endpoint.append_data(last=True, blob=InlineBlob(b'hello'))

        self.assertEqual(1, self.service._dequeue())

        resp = start_endpoint.get_json_response(2, 'start_response')
        logging.info('test_durable_upstream_inflight start resp %s', resp)
        self.assertIsNotNone(resp)
        self.assertEqual(resp.code, 234)

        self.assertTrue(start_endpoint.set_durable().ok())

        self.endpoint.set_start_response(Response(245))
        self.endpoint.add_data_response(Response(267))

        resp = start_endpoint.get_json_response(2, 'final_status')
        logging.info('test_durable_upstream_inflight final resp %s', resp)
        self.assertIsNotNone(resp)
        self.assertEqual(resp.code, 267)

    # set durable after ttl -> failed precondition
    def test_idle_gc_oneshot_temp(self):
        start_endpoint = RestEndpoint(
            base_url=self.router_url, http_host='outbound-gw',
            wait_response=False,
            msa=True)
        transaction_url = start_endpoint.transaction_url

        self.endpoint.set_start_response(Response(234))
        self.endpoint.add_data_response(Response(456))

        start_resp = start_endpoint.start(
            TransactionMetadata(),
            mail_from='alice',  rcpt_to='bob')
        start_endpoint.append_data(last=True, blob=InlineBlob(b'hello'))

        self.assertEqual(1, self.service._dequeue())

        resp = start_endpoint.get_json_response(5, 'final_status')
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
            base_url=self.router_url, http_host='outbound-gw',
            wait_response=False,
            msa=True)
        transaction_url = start_endpoint.transaction_url

        self.endpoint.set_start_response(Response(234))
        # no data response -> output blocks

        # output blocks waiting on rcpt_to
        start_resp = start_endpoint.start(
            TransactionMetadata(),
            mail_from='alice', rcpt_to='bob')
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
