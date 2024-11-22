# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import unittest
import logging
import socketserver
import json
import time
from functools import partial
import asyncio
from dkim import dknewkey
import tempfile

from koukan.config import Config

from koukan.gateway import SmtpGateway

from koukan.router_service import Service

from koukan.fake_smtpd import FakeSmtpd

from koukan.ssmtp import main as send_smtp

from koukan.executor import Executor

from examples.send_message.send_message import Sender

from examples.receiver.receiver import Receiver, create_app
from koukan.hypercorn_main import run
import koukan.postgres_test_utils as postgres_test_utils

def setUpModule():
    postgres_test_utils.setUpModule()

def tearDownModule():
    postgres_test_utils.tearDownModule()


class End2EndTest(unittest.TestCase):
    dkim_tempdir = None

    def _find_free_port(self):
        with socketserver.TCPServer(("localhost", 0), lambda x,y,z: None) as s:
            return s.server_address[1]

    def _update_address_list(self, policy):
        logging.debug('_update_address_list %s', policy)
        if (dest := policy.get('destination', None)) is None:
            return
        logging.debug(dest)
        if dest['endpoint'] == 'http://localhost:8001':
            dest['endpoint'] = self.gateway_base_url
            dest['host_list'] = [
                {'host': 'fake_smtpd', 'port': self.fake_smtpd_port}]
        elif dest['endpoint'] == 'http://localhost:8000':
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
            self._update_address_list(policy)

    def _update_dkim(self, chain):
        last = 0
        dkim = None
        for i,f in enumerate(chain):
            if f['filter'] == 'dkim':
                dkim = i
                break
            if f['filter'] in ['message_builder', 'received_header']:
                last = i
        if dkim is None:
            chain.insert(last, {'filter': 'dkim',
                                'domain': 'd',
                                'selector': 'sel',
                                'key': None})
            dkim = last
        filter_yaml = chain[last]
        self.dkim_tempdir = tempfile.TemporaryDirectory()
        dir = self.dkim_tempdir.name
        self.dkim_privkey = dir + '/privkey'
        self.dkim_pubkey = dir + '/pubkey'
        dknewkey.GenRSAKeys(self.dkim_privkey)
        dknewkey.ExtractRSADnsPublicKey(self.dkim_privkey, self.dkim_pubkey)
        filter_yaml['key'] = self.dkim_privkey

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
        gateway_listener_yaml['session_uri'] = 'http://localhost:%d' % self.gateway_rest_port

        # TODO we might want to generate this on the fly and enable https?
        for k in ['cert', 'key']:
            if k in gateway_listener_yaml:
                del gateway_listener_yaml[k]
        gateway_yaml['smtp_listener']['services'][0]['addr'] = [
            'localhost', self.gateway_mx_port]
        gateway_yaml['smtp_listener']['services'][1]['addr'] = [
            'localhost', self.gateway_msa_port]

        self.router_rest_port = self._find_free_port()
        self.router_base_url = 'http://localhost:%d/' % self.router_rest_port
        self.router_config = Config()
        self.router_config.load_yaml('config/local-test/router.yaml')
        router_yaml = self.router_config.root_yaml
        router_listener_yaml = router_yaml['rest_listener']
        router_listener_yaml['addr'] = ['localhost', self.router_rest_port]
        router_listener_yaml['session_uri'] = (
            'http://localhost:%d' % self.router_rest_port)
        for k in ['cert', 'key']:
            if k in router_listener_yaml:
                del router_listener_yaml[k]
        self.pg, self.pg_url = postgres_test_utils.setup_postgres()
        router_yaml['storage']['url'] = self.pg_url

        self.fake_smtpd_port = self._find_free_port()

        for endpoint in gateway_yaml['rest_output']:
            endpoint['endpoint'] = self.router_base_url
            del endpoint['verify']

        for endpoint in router_yaml['endpoint']:
            if endpoint['name'] == 'msa-output':
                self._update_dkim(endpoint['chain'])
            for filter in endpoint['chain']:
                self._update_router(filter)
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
        self.receiver = Receiver(close_files=False)
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
                  'alice@example.com', ['bob@example.com'],
                  'hello, world!')

        for handler in self.fake_smtpd.handlers:
            # smtpd machinery constructs extra handlers during startup?
            if handler.ehlo is None:
                logging.debug('empty handler? %s', handler)
                continue
            self.assertEqual(handler.ehlo, 'localhost')
            self.assertEqual(handler.mail_from, 'alice@example.com')
            self.assertEqual(len(handler.mail_options), 1)
            self.assertTrue(handler.mail_options[0].startswith('SIZE='))
            self.assertEqual(handler.rcpt_to, ['bob@example.com'])
            self.assertEqual(handler.rcpt_options, [[]])
            self.assertIn(b'hello, world!', handler.data)
            break
        else:
            self.fail('didn\'t receive message')

    # mx smtp -> rest
    def test_rest_receiving(self):
        send_smtp('localhost', self.gateway_mx_port, 'end2end_test',
                  'alice@example.com', ['bob@rest-application.example.com'],
                  'hello, world!\n')
        for tx_id,tx in self.receiver.transactions.items():
            logging.debug('test_rest_receiving %s', tx_id)
            if tx.tx_json['mail_from']['m'] == 'alice@example.com':
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

        tx.body_file.seek(0)
        body = tx.body_file.read()
        logging.debug('raw %s', body)
        self.assertIn(b'Received:', body)

        parsed = tx.message_json

        self.assertIn(
            [ "from", [{ "display_name": "",
                         "address": "alice@example.com"} ] ],
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
        sender = Sender(self.router_base_url,
                        'msa-output',
                        'alice@example.com',
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
        message_builder_spec = {
            "headers": [
                ["from", [{"display_name": "alice a",
                           "address": "alice@example.com"}]],
                ["to", [{"address": "bob@example.com"}]],
                ["subject", "hello"],
                ["date", {"unix_secs": 1709750551, "tz_offset": -28800}],
                ["message-id", ["abc@xyz"]],
            ],
           "text_body": [{
               "content_type": "text/plain",
               "content_uri": "my_plain_body",
               "file_content": "examples/send_message/body.txt"
           }]
        }

        sender = Sender(self.router_base_url,
                        'msa-output',
                        'alice@example.com',
                        message_builder=message_builder_spec)
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
            self.assertIn(b'from: alice a <alice@example.com>', handler.data)
            self.assertIn(b'DKIM-Signature:', handler.data)
            self.assertIn(b'Received:', handler.data)
            self.assertIn(b'aGVsbG8sIHdvcmxkIQo=', handler.data)

        self.assertEqual(2, len(handlers))

    # submission rest -> short circuit mx


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d %(message)s')

    unittest.main()
