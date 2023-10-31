import unittest
import logging

from router_service import Service, Config

from fake_endpoints import FakeEndpoint

from rest_endpoint import RestEndpoint

from typing import Any, Tuple

from threading import Thread, Lock, Condition

import time

from rest_endpoint import RestEndpoint

from response import Response

from blob import InlineBlob

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
    "rest_port": 8000,
    "db_filename": "",
    'use_gunicorn': False
}

class RouterServiceTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

        self.endpoint = FakeEndpoint()
        self.service = Service(config=Config(js=config_json))
        wiring = Wiring(self.endpoint)
        self.service_thread = Thread(
            daemon=True,
            target=lambda: self.service.main(
                wiring))
        self.service_thread.start()

        # probe for startup
        self.endpoint.start_response = Response()
        while True:
            try:
                rest_endpoint = RestEndpoint(
                    base_url='http://localhost:8000', http_host='outbound-gw')
                start_resp = rest_endpoint.start(
                    None, None, 'alice', None, 'bob', None)
            except:
                time.sleep(0.1)
            else:
                break

    def tearDown(self):
        self.service.wsgi_server.shutdown()
        self.service_thread.join()

    def test_read_atrest(self):
        logging.info('test_read_atrest')

        rest_endpoint = RestEndpoint(
            base_url='http://localhost:8000', http_host='outbound-gw')

        self.endpoint.start_response = Response()
        self.endpoint.final_response = Response(456, 'upstream temp')
        start_resp = rest_endpoint.start(None, None, 'alice', None, 'bob', None)
        self.assertTrue(start_resp.ok())
        transaction_url = rest_endpoint.transaction_url
        final_resp = rest_endpoint.append_data(True, InlineBlob(b'hello'))
        # XXX cf router_service ShimTransaction on what the semantics
        # of this ought to be
        self.assertEqual(final_resp.code, 456)
        self.assertEqual(final_resp.message, 'upstream temp')
        # NB: a counterintuitive property of this is that the TEMP_FAIL
        # prior to set_durable results in status ONESHOT_DONE
        durable_resp = rest_endpoint.set_durable()
        self.assertTrue(durable_resp.ok())

        for i in range(0,5):
            rest_endpoint = RestEndpoint(
                transaction_url=transaction_url)
            status = rest_endpoint.get_status(1)
            logging.info('pre resp %d %s', i, status)
            # XXX ick RestEndpoint returns a timeout for the field to
            # be populated as a 400 here
            self.assertEqual(status.code, 400)
            self.assertEqual(status.message,
                             'RestEndpoint.get_status timeout')
            time.sleep(0.1)

        self.endpoint.final_response = Response()
        for i in range(0,20):
            rest_endpoint = RestEndpoint(
                transaction_url=transaction_url)
            status = rest_endpoint.get_status(1)
            logging.info('post resp %d %s', i, status)
            if status.ok(): break
            time.sleep(0.1)
        else:
            self.assertTrue(False)

if __name__ == '__main__':
    unittest.main()
