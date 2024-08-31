import unittest
import logging
import socketserver
import json
import time
from functools import partial
import asyncio

from config import Config

from gateway import SmtpGateway

from router_service import Service

from fake_smtpd import FakeSmtpd

from ssmtp import main as send_smtp

from executor import Executor

from examples.cli.send_message import Sender

from examples.receiver.receiver import Receiver, create_app
from hypercorn_main import run

class End2EndTest(unittest.TestCase):
    def _find_free_port(self):
        with socketserver.TCPServer(("localhost", 0), lambda x,y,z: None) as s:
            return s.server_address[1]

#    def _update_dest_domain(self, policy):
#        if policy['endpoint'] == 'https://localhost:8001':
#            policy['endpoint'] = self.gateway_base_url

    def _update_address_list(self, policy):
        logging.debug('_update_address_list %s', policy)
        if (dest := policy.get('destination', None)) is None:
            return
        logging.debug(dest)
        if dest['endpoint'] == 'https://localhost:8001':
            dest['endpoint'] = self.gateway_base_url
            dest['host_list'] = [
                {'host': 'fake_smtpd', 'port': self.fake_smtpd_port}]
        elif dest['endpoint'] == 'https://localhost:8000':
            dest['endpoint'] = self.router_base_url
        elif dest['endpoint'] == 'http://localhost:8002':
            dest['endpoint'] = self.receiver_base_url

    def _update_router(self, filter):
        if filter['filter'] != 'router':
            return
        policy = filter['policy']
        logging.debug('_update_router policy %s', policy)
        if policy['name'] in ['dest_domain', 'address_list']:
#            self._update_dest_domain(policy)
#        elif policy['name'] == 'address_list':
            self._update_address_list(policy)

    def _configure(self):
        self.gateway_mx_port = self._find_free_port()
        self.gateway_msa_port = self._find_free_port()
        self.gateway_rest_port = self._find_free_port()
        self.gateway_base_url = 'http://localhost:%d/' % self.gateway_rest_port

        self.receiver_rest_port = self._find_free_port()
        self.receiver_base_url = 'http://localhost:%d' % self.receiver_rest_port

        self.gateway_config = Config()
        self.gateway_config.load_yaml('config/local-test/gateway.yaml')
        gateway_yaml = self.gateway_config.root_yaml
        gateway_listener_yaml = gateway_yaml['rest_listener']
        gateway_listener_yaml['addr'] = ['localhost', self.gateway_rest_port]
        # TODO we might want to generate this on the fly and enable https?
        del gateway_listener_yaml['cert']
        del gateway_listener_yaml['key']
        gateway_yaml['smtp_listener']['services'][0]['addr'] = [
            'localhost', self.gateway_mx_port]
        gateway_yaml['smtp_listener']['services'][1]['addr'] = [
            'localhost', self.gateway_msa_port]

        self.router_rest_port = self._find_free_port()
        self.router_base_url = 'http://localhost:%d/' % self.router_rest_port
        self.router_config = Config()
        self.router_config.load_yaml('config/local-test/router.yaml')
        router_yaml = self.router_config.root_yaml
        router_listener_yaml= router_yaml['rest_listener']
        router_listener_yaml['addr'] = ['localhost', self.router_rest_port]
        del router_listener_yaml['cert']
        del router_listener_yaml['key']

        self.fake_smtpd_port = self._find_free_port()

        for endpoint in gateway_yaml['rest_output']:
            endpoint['endpoint'] = self.router_base_url
            del endpoint['verify']

        for endpoint in router_yaml['endpoint']:
            for filter in endpoint['chain']:
                self._update_router(filter)

        for endpoint in router_yaml['endpoint']:
            for filter in endpoint['chain']:
                if filter['filter'] != 'dns_resolution':
                    continue
                #if filter.get('literal', None) != 'fake_smtpd':
                #    continue
                filter['static_hosts'] = [
                    {'host': '127.0.0.1', 'port': self.fake_smtpd_port}]

        for endpoint in router_yaml['endpoint']:
            for filter in endpoint['chain']:
                if filter['filter'] != 'rest_output':
                    continue
                del filter['verify']

        logging.debug('gateway yaml %s', json.dumps(gateway_yaml, indent=2))
        logging.debug('router_yaml %s', json.dumps(router_yaml, indent=2))
        logging.debug('fake smtpd port %d', self.fake_smtpd_port)
        logging.debug('rest receiver port %d', self.receiver_rest_port)

        self.gateway = SmtpGateway(self.gateway_config)
        self.router = Service(config=self.router_config)
        self.fake_smtpd = FakeSmtpd("localhost", self.fake_smtpd_port)


    def _run(self):
        self.gateway_main_fut = self.executor.submit(
            partial(self.gateway.main, alive=self.executor.ping_watchdog))
        self.router_main_fut = self.executor.submit(
            partial(self.router.main, alive=self.executor.ping_watchdog))
        self.fake_smtpd.start()
        self.hypercorn_shutdown = asyncio.Event()
        self.receiver = Receiver()
        self.executor.submit(
            partial(run, [('localhost', self.receiver_rest_port)], None, None,
                    create_app(self.receiver), self.hypercorn_shutdown,
                    self.executor.ping_watchdog))

    def setUp(self):
        self.executor = Executor(
            inflight_limit = 10, watchdog_timeout=10, debug_futures=True)
        self._configure()
        self._run()
        time.sleep(1)


    def tearDown(self):
        logging.debug('End2EndTest.tearDown')
        self.router.shutdown()
        self.gateway.shutdown()
        self.fake_smtpd.stop()
        self.hypercorn_shutdown.set()
        self.executor.shutdown(timeout=60)
        logging.debug('End2EndTest.tearDown done')

    # mx smtp -> smtp
    def test_smoke(self):
        send_smtp('localhost', self.gateway_mx_port, 'end2end_test',
                  'alice@d', ['bob@d'],
                  'hello, world!')

        for handler in self.fake_smtpd.handlers:
            # smtpd machinery constructs extra handlers during startup?
            if handler.ehlo is None:
                continue
            self.assertEqual(handler.ehlo, 'frylock')
            self.assertEqual(handler.mail_from, 'alice@d')
            self.assertEqual(len(handler.mail_options), 1)
            self.assertTrue(handler.mail_options[0].startswith('SIZE='))
            self.assertEqual(handler.rcpt_to, ['bob@d'])
            self.assertEqual(handler.rcpt_options, [[]])
            self.assertIn(b'hello, world!', handler.data)
            break
        else:
            self.fail('didn\'t receive message')

    # mx smtp -> rest
    def test_rest_receiving(self):
        send_smtp('localhost', self.gateway_mx_port, 'end2end_test',
                  'alice@d', ['bob@dd'],
                  'hello, world!')
        for tx_id,tx in self.receiver.transactions.items():
            logging.debug('test_rest_receiving %s',
                          tx_id)
            logging.debug(json.dumps(tx.tx_json, indent=2))
            logging.debug(json.dumps(tx.message_json, indent=2))

    # mx smtp -> rest

    # submission smtp -> smtp
    # submission mime rest -> smtp

    # submission message_builder rest -> smtp
    # -> @d is short-circuit
    def test_message_builder(self):
        message_builder_spec = {
            "headers": [
                ["from", [{"display_name": "alice a",
                           "address": "alice@d"}]],
                ["to", [{"address": "bob@example.com"}]],
                ["subject", "hello"],
                ["date", {"unix_secs": 1709750551, "tz_offset": -28800}],
                ["message-id", ["abc@xyz"]],
            ],
            "text_body": [{
                "content_type": "text/plain",
                "content_uri": "my_plain_body",
                "file_content": "/etc/lsb-release"
            }]
        }

        sender = Sender('alice@d', message_builder_spec,
                        base_url=self.router_base_url)
        sender.send('bob@example.com')

        for handler in self.fake_smtpd.handlers:
            logging.debug(handler)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s [%(thread)d] %(message)s')

    unittest.main()
