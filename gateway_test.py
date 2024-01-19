
from gateway import SmtpGateway

import unittest

import logging

from config import Config
import socketserver
from threading import Thread

from rest_endpoint import RestEndpoint

import time

from fake_smtpd import FakeSmtpd

from blob import InlineBlob

# xxx -> yaml
config_json = {
    "rest_port": None,
    "router_port": None,
    "mx_port": None,
    "msa_port": None,
    "ehlo_host": "frylock",
    "listen_host": "localhost",
    "use_gunicorn": False,
    'tx_idle_timeout': 1,
    'tx_idle_gc': 1,
}


class GatewayTest(unittest.TestCase):
    def setUp(self):
        logging.info('GatewayTest.setUp')
        for p in ["rest_port", "router_port", "mx_port", "msa_port"]:
            config_json[p] = self.find_unused_port()
        self.config = Config(js=config_json)
        self.gw = SmtpGateway(self.config)

        self.fake_smtpd_port = self.find_unused_port()

        self.fake_smtpd = FakeSmtpd(self.fake_smtpd_port)
        self.fake_smtpd.start()

        self.service_thread = Thread(
            daemon=True,
            target=lambda: self.gw.main())
        self.service_thread.start()

        self.gw_rest_url = 'http://localhost:%d' % self.config.get_int('rest_port')

        while True:
            logging.info('GatewayTest.setUp probe rest')
            try:
                rest_endpoint = RestEndpoint(
                    base_url=self.gw_rest_url,
                    http_host='outbound')
                start_resp = rest_endpoint.start(
                    None, ('localhost', self.fake_smtpd_port),
                    'probe', None, 'probe', None)
            except:
                time.sleep(0.1)
            else:
                break


    def tearDown(self):
        logging.info('GatewayTest.tearDown')
        self.gw.shutdown()
        self.gw.wsgi_server.shutdown()
        self.service_thread.join()
        self.fake_smtpd.stop()

    def find_unused_port(self) -> int:
        with socketserver.TCPServer(("localhost", 0), lambda x,y,z: None) as s:
            return s.server_address[1]

    def test_rest_to_smtp_basic(self):
        rest_endpoint = RestEndpoint(
            base_url=self.gw_rest_url,
            http_host='outbound')
        start_resp = rest_endpoint.start(
            None, ('localhost', self.fake_smtpd_port),
            'alice', None, 'bob', None)
        self.assertEqual(start_resp.code, 250)
        final_resp = rest_endpoint.append_data(
            blob=InlineBlob('hello'), last=True)
        self.assertEqual(final_resp.code, 250)


    def test_rest_to_smtp_idle_gc(self):
        rest_endpoint = RestEndpoint(
            base_url=self.gw_rest_url,
            http_host='outbound')
        start_resp = rest_endpoint.start(
            None, ('localhost', self.fake_smtpd_port),
            'alice', None, 'bob', None)
        self.assertEqual(start_resp.code, 250)
        for i in range(0,5):
            resp_json = rest_endpoint.get_json(2)
            if resp_json is None:
                break
            logging.info(resp_json)
            time.sleep(1)
        else:
            self.fail('expected tx 404 after idle')

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')

    unittest.main()
