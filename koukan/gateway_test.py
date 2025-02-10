# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import unittest
import logging
import socketserver
from threading import Thread
import time
from parameterized import parameterized_class

from koukan.gateway import SmtpGateway
from koukan.fake_smtpd import FakeSmtpd
from koukan.blob import InlineBlob
from koukan.rest_endpoint import RestEndpoint
from koukan.filter import HostPort, Mailbox, TransactionMetadata

from requests.exceptions import ConnectionError

root_yaml = {
    'global': {
    },
    'rest_listener': {
        'gc_interval': 1,
        'gc_tx_ttl': 1,
        'gc_blob_ttl': 1,
    },
    'smtp_output': {
        'outbound': {
            'ehlo_host': 'gateway_test',
        }
    },
    'smtp_listener': {
        'services': []
    }
}

@parameterized_class(('use_fastapi', 'protocol'),
                     [(True, 'smtp'),
                      (True, 'lmtp'),
                      (False, 'smtp')])
class GatewayTest(unittest.TestCase):
    def setUp(self):
        logging.info('GatewayTest.setUp use_fastapi=%s protocol=%s',
                     self.use_fastapi, self.protocol)

        rest_port = self.find_unused_port()
        root_yaml['rest_listener']['addr'] = ['127.0.0.1', rest_port]
        root_yaml['rest_listener']['use_fastapi'] = self.use_fastapi
        root_yaml['smtp_output']['outbound']['protocol'] = self.protocol

        self.gw = SmtpGateway(root_yaml)

        self.fake_smtpd_port = self.find_unused_port()

        self.fake_smtpd = FakeSmtpd(
            "localhost", self.fake_smtpd_port, self.protocol)
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
                    static_http_host='outbound')
                tx = TransactionMetadata(
                    remote_host=HostPort('127.0.0.1', self.fake_smtpd_port))
                tx.mail_from = Mailbox('probe-from%d' % i)
                tx.rcpt_to = [Mailbox('probe-to%d' % i)]
                upstream_delta = rest_endpoint.on_update(tx, tx.copy())
                logging.debug('probe %s', tx.mail_response)
                if tx.mail_response.code >= 300:
                    time.sleep(0.1)
                    continue
            except ConnectionError:
                time.sleep(0.1)
            else:
                break
        else:
            self.fail('service not ready')

    def tearDown(self):
        logging.info('GatewayTest.tearDown')
        self.gw.shutdown()
        self.service_thread.join()
        self.fake_smtpd.stop()

    def find_unused_port(self) -> int:
        with socketserver.TCPServer(("localhost", 0), lambda x,y,z: None) as s:
            return s.server_address[1]

    def test_rest_to_smtp_basic(self):
        rest_endpoint = RestEndpoint(
            static_base_url=self.gw_rest_url,
            static_http_host='outbound', timeout_start=10, timeout_data=10)
        tx=TransactionMetadata(
            remote_host=HostPort('127.0.0.1', self.fake_smtpd_port))
        tx.mail_from = Mailbox('alice')
        tx.rcpt_to = [Mailbox('bob')]
        upstream_delta = rest_endpoint.on_update(tx, tx.copy())
        logging.info('test_rest_to_smtp_basic mail_resp %s', tx.mail_response)
        self.assertEqual(tx.mail_response.code, 250)
        self.assertEqual([r.code for r in tx.rcpt_response], [250])
        tx_delta = TransactionMetadata(
            body_blob=InlineBlob(b'hello', last=True))
        self.assertIsNotNone(tx.merge_from(tx_delta))
        upstream_delta = rest_endpoint.on_update(tx, tx_delta)
        logging.debug('test_rest_to_smtp_basic body tx response %s', tx)
        self.assertEqual(tx.data_response.code, 250)


    def test_rest_to_smtp_idle_gc(self):
        rest_endpoint = RestEndpoint(
            static_base_url=self.gw_rest_url,
            static_http_host='outbound')
        tx=TransactionMetadata(
            remote_host=HostPort('127.0.0.1', self.fake_smtpd_port))
        tx.mail_from = Mailbox('alice')
        upstream_delta = rest_endpoint.on_update(tx, tx.copy())
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
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d  %(message)s')

    unittest.main()
