import unittest
import logging

from router_service import Service, Config

from fake_endpoints import SyncEndpoint

from rest_endpoint import RestEndpoint

from typing import Any, Tuple

from threading import Thread, Lock, Condition

import time

from rest_endpoint import RestEndpoint

from response import Response

from blob import InlineBlob

import socketserver


class Wiring:
    def __init__(self, endpoint):
        self.endpoint = endpoint

    def setup(self, config, rest_blob_id_map=None):
        self.config = config
        self.rest_blob_id_map = rest_blob_id_map

    def get_endpoint(self, host) -> Tuple[Any, bool]:
        if host == 'inbound-gw':
            return self.endpoint, False
        elif host == 'outbound-gw':
            return self.endpoint, True
        else:
            logging.warning('router_service_test.Wiring unknown host %s', host)
            return None, None

config_json = {
    'rest_port': None,
    'db_filename': '',
    'use_gunicorn': False,
    'tx_idle_timeout': 1,
    'tx_idle_gc': 1,
}

class RouterServiceTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

        # find a free port
        with socketserver.TCPServer(("localhost", 0), lambda x,y,z: None) as s:
            self.port = s.server_address[1]
        config_json['rest_port'] = self.port
        self.router_url = 'http://localhost:%d' % self.port
        self.endpoint = SyncEndpoint()
        self.service = Service(config=Config(js=config_json))
        wiring = Wiring(self.endpoint)
        self.service_thread = Thread(
            daemon=True,
            target=lambda: self.service.main(wiring))
        self.service_thread.start()

        # probe for startup
        self.endpoint.set_start_response(Response())
        while True:
            try:
                rest_endpoint = RestEndpoint(
                    base_url=self.router_url,
                    http_host='outbound-gw')
                start_resp = rest_endpoint.start(
                    None, None, 'probe', None, 'probe', None)
            except:
                time.sleep(0.1)
            else:
                break

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

        start_resp = start_endpoint.start(None, None, 'alice', None, 'bob', None)
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
        for i in range(1,3):
            resp_json = read_endpoint.get_json(1)
            logging.info('resp %s', resp_json)
            time.sleep(1)
            # XXX this is currently non-deterministic depending on
            # whether you hit the inflight RouterTransaction or read
            # from storage, response propagation is still in flux
            if 'start_response' in resp_json:
                start_resp = Response.from_json(resp_json['start_response'])
                self.assertEqual(start_resp.code, 456)
                # XXX resp propagation
                #self.assertIn('final_status', resp_json)
                #final_status = Response.from_json(resp_json['final_status'])
                #self.assertEqual(final_status.code, 456)
            else:
                self.assertNotIn('final_status', resp_json)

        # upstream success, retry succeeds, propagates down to rest
        self.endpoint.set_start_response(Response(234))
        self.endpoint.add_data_response(Response(245))
        for i in range(1,10):
            resp_json = read_endpoint.get_json(1)
            logging.info('resp %s', resp_json)
            if 'final_status' in resp_json:
                break
            time.sleep(1)
        else:
            self.fail('expected final_status')


    # set durable after upstream ok/perm -> noop
    def test_durable_after_upstream_done(self):
        start_endpoint = RestEndpoint(
            base_url=self.router_url, http_host='outbound-gw',
            wait_response=False,
            msa=True)

        self.endpoint.set_start_response(Response(234))
        self.endpoint.add_data_response(Response(256))

        start_resp = start_endpoint.start(None, None, 'alice', None, 'bob', None)
        start_endpoint.append_data(last=True, blob=InlineBlob(b'hello'))

        for i in range(1,3):
            resp_json = start_endpoint.get_json(1)
            logging.info('resp %s', resp_json)
            if 'final_status' in resp_json:
                resp = Response.from_json(resp_json['final_status'])
                self.assertEqual(256, resp.code)
                break
            time.sleep(1)
        else:
            self.fail('expected final_status')

        # ONESHOT_DONE/DELIVERED -> noop
        self.assertTrue(start_endpoint.set_durable().ok())


    def test_durable_after_upstream_temp(self):
        start_endpoint = RestEndpoint(
            base_url=self.router_url, http_host='outbound-gw',
            wait_response=False,
            msa=True)

        self.endpoint.set_start_response(Response(234))
        self.endpoint.add_data_response(Response(456))

        start_resp = start_endpoint.start(None, None, 'alice', None, 'bob', None)
        start_endpoint.append_data(last=True, blob=InlineBlob(b'hello'))

        time.sleep(1)
        #self.dump_db()

        for i in range(1,3):
            resp_json = start_endpoint.get_json(0.1)
            logging.info('resp %s', resp_json)
            if 'final_status' in resp_json:
                break
            time.sleep(1)
        else:
            self.fail('expected final_status')

        self.assertTrue(start_endpoint.set_durable().ok())

    def test_durable_upstream_inflight(self):
        start_endpoint = RestEndpoint(
            base_url=self.router_url, http_host='outbound-gw',
            wait_response=False,
            msa=True)

        self.endpoint.set_start_response(Response(234))

        start_resp = start_endpoint.start(None, None, 'alice', None, 'bob', None)
        start_endpoint.append_data(last=True, blob=InlineBlob(b'hello'))

        for i in range(1,3):
            resp_json = start_endpoint.get_json(2)
            logging.info('resp %s', resp_json)
            self.assertNotIn('final_status', resp_json)
            time.sleep(1)

        self.assertTrue(start_endpoint.set_durable().ok())

        self.endpoint.set_start_response(Response(245))
        self.endpoint.add_data_response(Response(267))

        for i in range(1,3):
            resp_json = start_endpoint.get_json(2)
            logging.info('resp %s', resp_json)
            if 'final_status' in resp_json:
                resp = Response.from_json(resp_json['final_status'])
                self.assertEqual(resp.code, 267)
                break
            time.sleep(1)
        else:
            self.fail('expected final_status')

    # set durable after ttl -> failed precondition
    def test_idle_gc(self):
        start_endpoint = RestEndpoint(
            base_url=self.router_url, http_host='outbound-gw',
            wait_response=False,
            msa=True)
        transaction_url = start_endpoint.transaction_url

        self.endpoint.set_start_response(Response(234))
        self.endpoint.add_data_response(Response(456))

        start_resp = start_endpoint.start(
            None, None, 'alice', None, 'bob', None)
        start_endpoint.append_data(last=True, blob=InlineBlob(b'hello'))

        for i in range(1,5):
            resp_json = start_endpoint.get_json(2)
            logging.info('test_idle_gc %s', resp_json)
            if 'final_status' in resp_json:
                resp = Response.from_json(resp_json['final_status'])
                if resp.code == 500 and resp.message == 'cancelled':
                    break
            time.sleep(1)
        #else:
        #    self.fail('expected cancellation')
        #self.assertTrue(self.endpoint.aborted)

        logging.info('test_idle_gc set durable')
        #durable_endpoint = RestEndpoint(
        #    base_url=self.router_url, http_host='outbound-gw',
        #    transaction_url=transaction_url)

        time.sleep(3)  # give gc a chance to run
        self.assertTrue(start_endpoint.set_durable().err())



if __name__ == '__main__':
    unittest.main()
