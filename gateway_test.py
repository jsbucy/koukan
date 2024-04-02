import unittest
import logging
import socketserver
from threading import Thread
import time

from config import Config
from gateway import SmtpGateway
from fake_smtpd import FakeSmtpd
from blob import InlineBlob
from rest_endpoint import RestEndpoint
from filter import HostPort, Mailbox, TransactionMetadata

from requests.exceptions import ConnectionError

root_yaml = {
    'global': {
    },
    'rest_listener': {
        'use_gunicorn': False,
        'gc_interval': 1,
        'gc_tx_ttl': 1,
        'gc_blob_ttl': 1,
    },
    'smtp_output': {
        'ehlo_host': 'gateway_test',
    },
    'smtp_listener': {
        'services': []
    }
}

class GatewayTest(unittest.TestCase):
    def setUp(self):
        logging.info('GatewayTest.setUp')

        #for p in ["rest_port", "router_port", "mx_port", "msa_port"]:
        #    config_json[p] = self.find_unused_port()

        rest_port = self.find_unused_port()
        root_yaml['rest_listener']['addr'] = ['127.0.0.1', rest_port]

        self.config = Config()
        self.config.inject_yaml(root_yaml)

        self.gw = SmtpGateway(self.config)

        self.fake_smtpd_port = self.find_unused_port()

        self.fake_smtpd = FakeSmtpd(self.fake_smtpd_port)
        self.fake_smtpd.start()

        self.service_thread = Thread(
            daemon=True,
            target=lambda: self.gw.main())
        self.service_thread.start()

        self.gw_rest_url = 'http://localhost:%d' % rest_port

        for i in range(0,5):
            logging.info('GatewayTest.setUp probe rest')
            try:
                rest_endpoint = RestEndpoint(
                    static_base_url=self.gw_rest_url,
                    http_host='outbound')
                tx = TransactionMetadata(
                    remote_host=HostPort('localhost', self.fake_smtpd_port))
                tx.mail_from = Mailbox('probe-from%d' % i)
                tx.rcpt_to = [Mailbox('probe-to%d' % i)]
                rest_endpoint.on_update(tx)
                logging.debug('probe %s', tx.mail_response)
                if tx.mail_response.code >= 300:
                    time.sleep(1)
                    continue
            except ConnectionError:
                time.sleep(1)
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
            static_base_url=self.gw_rest_url,
            http_host='outbound')
        tx=TransactionMetadata(
            remote_host=HostPort('localhost', self.fake_smtpd_port))
        tx.mail_from = Mailbox('alice')
        tx.rcpt_to = [Mailbox('bob')]
        rest_endpoint.on_update(tx)
        logging.info('test_rest_to_smtp_basic mail_resp %s', tx.mail_response)
        self.assertEqual(tx.mail_response.code, 250)
        self.assertEqual([r.code for r in tx.rcpt_response], [250])
        tx = TransactionMetadata(body_blob=InlineBlob('hello'))
        rest_endpoint.on_update(tx)
        self.assertEqual(tx.data_response.code, 250)


    def test_rest_to_smtp_idle_gc(self):
        rest_endpoint = RestEndpoint(
            static_base_url=self.gw_rest_url,
            http_host='outbound')
        tx=TransactionMetadata(
            remote_host=HostPort('localhost', self.fake_smtpd_port))
        tx.mail_from = Mailbox('alice')
        rest_endpoint.on_update(tx)
        self.assertEqual(tx.mail_response.code, 250)

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
