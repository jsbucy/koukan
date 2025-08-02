# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional
import unittest
import logging
import socketserver
import json
import time
from functools import partial
import asyncio
from dkim import dknewkey
import tempfile
import yaml

from koukan.gateway import SmtpGateway
from koukan.router_service import Service
from koukan.fake_smtpd import FakeSmtpd
from koukan.ssmtp import main as send_smtp
from koukan.executor import Executor
from koukan.hypercorn_main import run
import koukan.postgres_test_utils as postgres_test_utils

from examples.send_message.send_message import Sender
from examples.receiver.receiver import Receiver
from examples.receiver.fastapi_receiver import create_app

def setUpModule():
    postgres_test_utils.setUpModule()

def tearDownModule():
    postgres_test_utils.tearDownModule()

def _get_router_endpoint_yaml(router_yaml : dict, name : str
                              ) -> Optional[dict]:
    for endpoint in router_yaml['endpoint']:
        if endpoint['name'] == name:
            return endpoint
    return None

def _add_filter_after(endpoint_yaml : dict, filter_name : str,
                      filter_yaml : dict):
    for i,filter in enumerate(endpoint_yaml['chain']):
        if filter['filter'] == filter_name:
            break
    else:
        assert False
    endpoint_yaml['chain'].insert(i + 1, filter_yaml)


class End2EndTest(unittest.TestCase):
    dkim_tempdir = None
    receiver_tempdir = None

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
                'max_inline': 8 }

    def _update_router(self, filter):
        if filter['filter'] != 'router':
            return
        policy = filter['policy']
        logging.debug('_update_router policy %s', policy)
        if policy['name'] in ['dest_domain', 'address_list']:
            self._update_address_list(policy)

    def _update_dkim(self, chain):
        return

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
        if not self.dkim_tempdir:
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

        with open('config/local-test/gateway.yaml', 'r') as f:
            self.gateway_config_yaml =  yaml.load(f, Loader=yaml.CLoader)
        gateway_yaml = self.gateway_config_yaml
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
        with open('config/local-test/router.yaml', 'r') as f:
            self.router_yaml = yaml.load(f, Loader=yaml.CLoader)

        router_yaml = self.router_yaml
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
            if endpoint['name'] in ['msa-output', 'submission']:
                self._update_dkim(endpoint['chain'])
            for filter in endpoint['chain']:
                self._update_router(filter)
                if filter['filter'] == 'dns_resolution':
                    filter['static_hosts'] = [
                        {'host': '127.0.0.1', 'port': self.fake_smtpd_port}]
                elif filter['filter'] == 'rest_output':
                    if filter.get('static_endpoint', None) == 'http://localhost:8002':
                        filter['static_endpoint'] = self.receiver_base_url
                    if 'verify' in filter:
                        del filter['verify']

        logging.debug('gateway yaml %s', json.dumps(gateway_yaml, indent=2))
        logging.debug('router_yaml %s', json.dumps(router_yaml, indent=2))
        logging.debug('fake smtpd port %d', self.fake_smtpd_port)
        logging.debug('rest receiver port %d', self.receiver_rest_port)


    def _run(self):
        self.gateway = SmtpGateway(self.gateway_config_yaml)
        self.router = Service(root_yaml=self.router_yaml)
        self.fake_smtpd = FakeSmtpd("localhost", self.fake_smtpd_port)

        self.gateway_main_fut = self.executor.submit(
            partial(self.gateway.main, alive=self.executor.ping_watchdog))
        self.router_main_fut = self.executor.submit(
            partial(self.router.main, alive=self.executor.ping_watchdog))
        self.fake_smtpd.start()
        self.hypercorn_shutdown = asyncio.Event()
        self.receiver_tempdir = tempfile.TemporaryDirectory()
        self.receiver = Receiver(
            self.receiver_tempdir.name,
            gc_interval=0)  # gc on every access
        self.executor.submit(
            partial(run, [('localhost', self.receiver_rest_port)], None, None,
                    create_app(self.receiver),
                    self.hypercorn_shutdown,
                    self.executor.ping_watchdog))

    def setUp(self):
        self.executor = Executor(
            inflight_limit = 10, watchdog_timeout=10, debug_futures=True)


    def _configure_and_run(self):
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
        for d in [d for d in [self.dkim_tempdir, self.receiver_tempdir]
                  if d is not None]:
            d.cleanup()
        logging.debug('End2EndTest.tearDown done')


    # mx smtp -> smtp
    def test_smoke(self):
        self._configure_and_run()
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
        self._configure_and_run()

        with open('testdata/multipart.msg', 'rb') as f:
            msg = f.read()

        send_smtp('localhost', self.gateway_mx_port, 'end2end_test',
                  'alice@example.com', ['bob@rest-application.example.com'],
                  raw=msg)
        for tx_id,tx in self.receiver.transactions.items():
            logging.debug('test_rest_receiving %s', tx_id)
            if tx.tx_json['mail_from']['m'] == 'alice@example.com':
                break
        else:
            self.fail('didn\'t receive message')

        logging.debug(json.dumps(tx.tx_json, indent=2))
        logging.debug(json.dumps(tx.message_json, indent=2))
        blob_content = {}
        for blob_id,path in tx.blob_paths.items():
            with open(path, 'rb') as f:
              content = f.read()
            logging.debug('blob %s %s', blob_id, content)
            self.assertNotIn(blob_id, blob_content)
            blob_content[blob_id] = content

        with open(tx.body_path, 'rb') as f:
            body = f.read()
        logging.debug('raw %s', body)
        self.assertIn(b'Received:', body)

        parsed = tx.message_json

        self.assertIn(
            [ "from", [{ "display_name": "alice a",
                         "address": "alice@example.com"} ] ],
            parsed['parts']['headers'])

        # mixed -> related -> alternative
        self.assertEqual('text/plain', parsed['parts']['parts'][0]['parts'][0]['parts'][0]['content_type'])
        self.assertTrue(parsed['parts']['parts'][0]['parts'][0]['parts'][0]['content']['filename'].endswith('inline0'))

        self.assertEqual('text/html', parsed['parts']['parts'][0]['parts'][0]['parts'][1]['content_type'])
        self.assertTrue(parsed['parts']['parts'][0]['parts'][0]['parts'][1]['content']['filename'].endswith('.0'))

        self.assertEqual('image/png', parsed['parts']['parts'][1]['content_type'])
        self.assertTrue(parsed['parts']['parts'][0]['parts'][1]['content']['filename'].endswith('.1'))

        self.assertEqual('image/png', parsed['parts']['parts'][1]['content_type'])
        self.assertTrue(parsed['parts']['parts'][1]['content']['filename'].endswith('.2'))


        self.assertEqual('text/plain', parsed['text_body'][0]['content_type'])
        self.assertTrue(parsed['text_body'][0]['content']['filename'].endswith('inline1'))

        self.assertEqual('text/html', parsed['text_body'][1]['content_type'])
        self.assertTrue(parsed['text_body'][1]['content']['filename'].endswith('.0'))

        self.assertEqual('image/png', parsed['related_attachments'][0]['content_type'])
        self.assertTrue(parsed['related_attachments'][0]['content']['filename'].endswith('.1'))
        self.assertEqual('xyz', parsed['related_attachments'][0]['content_id'])

        self.assertEqual('image/png', parsed['file_attachments'][0]['content_type'])
        self.assertTrue(parsed['file_attachments'][0]['content']['filename'].endswith('.2'))
        self.assertEqual('funny cats.png', parsed['file_attachments'][0]['filename'])

        self.assertEqual(blob_content['inline0'], b'hello')
        self.assertEqual(blob_content['inline1'], b'hello')
        self.assertEqual(blob_content['0'], b'<b>hello</b>')
        self.assertEqual(blob_content['1'], b'yolocat')
        self.assertEqual(blob_content['2'], b'yolocat2')


    # submission smtp -> smtp

    # submission rest w/mime -> smtp
    # w/payload reuse
    def test_submission_mime(self):
        self._configure_and_run()

        sender = Sender(self.router_base_url,
                        'submission',
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
        self._configure_and_run()

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
               "content": {
                   "create_id": "my_plain_body",
                   "filename": "examples/send_message/body.txt"}
             },
             {
               "content_type": "text/html",
               "content": {"inline": "<b>hello</b>" }
             }]
        }

        sender = Sender(self.router_base_url,
                        'submission',
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
            # self.assertIn(b'DKIM-Signature:', handler.data)
            # self.assertIn(b'Received:', handler.data)
            self.assertIn(b'aGVsbG8sIHdvcmxkIQo=', handler.data)

        self.assertEqual(2, len(handlers))

    # submission rest -> short circuit mx

    def _test_add_route(self, filter_yaml):
        self._configure()
        endpoint_yaml = _get_router_endpoint_yaml(self.router_yaml, 'mx-out')
        _add_filter_after(endpoint_yaml, 'message_parser', filter_yaml)

        self._run()
        time.sleep(1)

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

        for tx_id,tx in self.receiver.transactions.items():
            logging.debug('test_rest_receiving %s', tx_id)
            if tx.tx_json['mail_from']['m'] == 'alice@example.com':
                break
        else:
            self.fail('didn\'t receive message')

    def test_add_route_sync(self):
        return self._test_add_route({'filter': 'add_route',
                                     'output_chain': 'sink'})

    def test_add_route_async(self):
        return self._test_add_route({'filter': 'add_route',
                                     'output_chain': 'sink',
                                     'store_and_forard': True})

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d %(message)s')

    unittest.main()
