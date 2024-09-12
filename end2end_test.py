import unittest
import logging
import socketserver
import json
import time
from functools import partial
import asyncio
from dkim import dknewkey
import tempfile

from config import Config

from gateway import SmtpGateway

from router_service import Service

from fake_smtpd import FakeSmtpd

from ssmtp import main as send_smtp

from executor import Executor

from examples.cli.send_message import Sender

from examples.receiver.receiver import Receiver, create_app
from hypercorn_main import run
import postgres_test_utils

def setUpModule():
    postgres_test_utils.setUpModule()

def tearDownModule():
    postgres_test_utils.tearDownModule()


class End2EndTest(unittest.TestCase):
    dkim_tempdir = None

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
            dest['options']['receive_parsing'] = {
                'max_inline': 0 }

    def _update_router(self, filter):
        if filter['filter'] != 'router':
            return
        policy = filter['policy']
        logging.debug('_update_router policy %s', policy)
        if policy['name'] in ['dest_domain', 'address_list']:
#            self._update_dest_domain(policy)
#        elif policy['name'] == 'address_list':
            self._update_address_list(policy)

    def _update_dkim(self, filter):
        if filter['filter'] != 'dkim':
            return
        self.dkim_tempdir = tempfile.TemporaryDirectory()
        dir = self.dkim_tempdir.name
        self.dkim_privkey = dir + '/privkey'
        self.dkim_pubkey = dir + '/pubkey'
        dknewkey.GenRSAKeys(self.dkim_privkey)
        dknewkey.ExtractRSADnsPublicKey(self.dkim_privkey, self.dkim_pubkey)
        filter['key'] = self.dkim_privkey

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
        self.pg = postgres_test_utils.setup_postgres(router_yaml['storage'])

        self.fake_smtpd_port = self._find_free_port()

        for endpoint in gateway_yaml['rest_output']:
            endpoint['endpoint'] = self.router_base_url
            del endpoint['verify']

        for endpoint in router_yaml['endpoint']:
            for filter in endpoint['chain']:
                self._update_router(filter)
                self._update_dkim(filter)
                if filter['filter'] == 'dns_resolution':
                    filter['static_hosts'] = [
                        {'host': '127.0.0.1', 'port': self.fake_smtpd_port}]
                elif filter['filter'] == 'rest_output':
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
        self.assertTrue(self.router.shutdown())
        self.assertTrue(self.gateway.shutdown())
        self.fake_smtpd.stop()
        self.hypercorn_shutdown.set()
        self.assertTrue(self.executor.shutdown(timeout=60))
        if self.dkim_tempdir:
            self.dkim_tempdir.cleanup()
        logging.debug('End2EndTest.tearDown done')


    # mx smtp -> smtp
    def test_smoke(self):
        send_smtp('localhost', self.gateway_mx_port, 'end2end_test',
                  'alice@d', ['bob@d'],
                  'hello, world!')

        for handler in self.fake_smtpd.handlers:
            # smtpd machinery constructs extra handlers during startup?
            if handler.ehlo is None:
                logging.debug('empty handler? %s', handler)
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
                  'hello, world!\n')
        for tx_id,tx in self.receiver.transactions.items():
            logging.debug('test_rest_receiving %s', tx_id)
            if tx.tx_json['mail_from']['m'] == 'alice@d':
                break
        else:
            self.fail('didn\'t receive message')

        logging.debug(json.dumps(tx.tx_json, indent=2))
        logging.debug(json.dumps(tx.message_json, indent=2))
        blob_content = {}
        for blob_id,blob in tx.blobs.items():
            blob.seek(0)
            content = blob.read()
            logging.debug('blob %s %s', blob_id, content)
            self.assertNotIn(blob_id, blob_content)
            blob_content[blob_id] = content

        body = tx.file.read()
        logging.debug('raw %s', body)
        self.assertIn(b'Received:', body)

        parsed = tx.message_json

        self.assertIn(
            [ "from", [{ "display_name": "",
                         "address": "alice@d"} ] ],
            parsed['parts']['headers'])

        self.assertEqual(
            parsed['text_body'],
            [ {
                "content_type": "text/plain",
                "blob_rest_id": "0"
            } ])
        self.assertEqual(blob_content['0'], b'hello, world!\n')

    # submission smtp -> smtp

    # submission rest w/mime -> smtp
    # w/payload reuse
    def test_submission_mime(self):
        sender = Sender('alice@d', base_url=self.router_base_url,
                        body_filename='testdata/trivial.msg')
        sender.send('bob@example.com')
        sender.send('bob2@example.com')

        handlers = {}
        for handler in self.fake_smtpd.handlers:
            logging.debug(handler)
            if len(handler.rcpt_to) != 1:
                continue
            rcpt = handler.rcpt_to[0]
            if rcpt not in ['bob@example.com', 'bob2@example.com']:
                continue
            self.assertNotIn(rcpt, handlers)
            handlers[rcpt] = handler
            self.assertIn(b'DKIM-Signature:', handler.data)
            self.assertIn(b'Received:', handler.data)

        self.assertEqual(2, len(handlers))



    # submission rest message_builder -> smtp
    def test_submission_message_builder(self):
        b = """
2024-09-04 15:11:39,723 [127551454058304] End2EndTest.tearDown
2024-09-04 15:11:39,723 [127551454058304] router_service shutdown()
2024-09-04 15:11:39,723 [127551454058304] router service hypercorn shutdown
2024-09-04 15:11:39,724 [127551454058304] Executor.shutdown waiting on 2
2024-09-04 15:11:39,724 [127551313794624] hypercorn_main._ping_alive() done
        """
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
               "put_content": b
           }]
        }

        sender = Sender('alice@d', message_builder_spec,
                        base_url=self.router_base_url)
        sender.send('bob@example.com')
        sender.send('bob2@example.com')

        encoded = (b'CjIwMjQtMDktMDQgMTU6MTE6MzksNzIzIFsxMjc1NTE0NTQwNTgzMDRdIEVuZDJFbmRUZXN0LnRl\r\n'
                   b'YXJEb3duCjIwMjQtMDktMDQgMTU6MTE6MzksNzIzIFsxMjc1NTE0NTQwNTgzMDRdIHJvdXRlcl9z\r\n'
                   b'ZXJ2aWNlIHNodXRkb3duKCkKMjAyNC0wOS0wNCAxNToxMTozOSw3MjMgWzEyNzU1MTQ1NDA1ODMw\r\n'
                   b'NF0gcm91dGVyIHNlcnZpY2UgaHlwZXJjb3JuIHNodXRkb3duCjIwMjQtMDktMDQgMTU6MTE6Mzks\r\n'
                   b'NzI0IFsxMjc1NTE0NTQwNTgzMDRdIEV4ZWN1dG9yLnNodXRkb3duIHdhaXRpbmcgb24gMgoyMDI0\r\n'
                   b'LTA5LTA0IDE1OjExOjM5LDcyNCBbMTI3NTUxMzEzNzk0NjI0XSBoeXBlcmNvcm5fbWFpbi5fcGlu\r\n'
                   b'Z19hbGl2ZSgpIGRvbmUKICAgICAgICA=\r\n')

        handlers = {}
        for handler in self.fake_smtpd.handlers:
            logging.debug(handler)
            if len(handler.rcpt_to) != 1:
                continue
            rcpt = handler.rcpt_to[0]
            if rcpt not in ['bob@example.com', 'bob2@example.com']:
                continue
            self.assertNotIn(rcpt, handlers)
            handlers[rcpt] = handler
            self.assertIn(b'from: alice a <alice@d>', handler.data)
            self.assertIn(b'DKIM-Signature:', handler.data)
            self.assertIn(b'Received:', handler.data)
            self.assertIn(encoded, handler.data)

        self.assertEqual(2, len(handlers))

    # submission rest -> short circuit mx


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s [%(thread)d] %(message)s')

    unittest.main()
