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
            logging.warning('unknown host %s', host)
            return None, None

config_json = {
    'rest_port': None,
    'db_filename': '',
    'use_gunicorn': False
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
        self.service.wsgi_server.shutdown()
        self.service_thread.join()

    def test_read_routing(self):
        logging.info('test_read_routing')

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
                final_status = Response.from_json(resp_json['final_status'])
                self.assertEqual(final_status.code, 456)
            else:
                self.assertNotIn('final_status', resp_json)

        # upstream success, retry succeeds, propagates down to rest
        self.endpoint.set_start_response(Response(234))
        self.endpoint.add_data_response(Response(245))
        for i in range(1,3):
            resp_json = read_endpoint.get_json(1)
            logging.info('resp %s', resp_json)
            if 'final_status' in resp_json:
                break
            time.sleep(1)
        else:
            self.fail('expected final_status')



if __name__ == '__main__':
    unittest.main()
