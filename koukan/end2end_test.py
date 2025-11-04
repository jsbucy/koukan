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
from urllib.parse import urljoin

from koukan.gateway import SmtpGateway
from koukan.router_service import Service
from koukan.fake_smtpd import FakeSmtpd
from koukan.ssmtp import main as send_smtp
from koukan.executor import Executor
import koukan.uvicorn_main as uvicorn_main
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

class End2EndTest(unittest.TestCase):
    dkim_tempdir = None
    receiver_tempdir = None
    http_server : Optional[uvicorn_main.Server] = None
    # gw -> router
    gw_path = '/senders/gateway/transactions'
    # router -> gw, receiver
    router_path = '/senders/router/transactions'
    # router -> router (egress -> ingress)
    short_circuit_path = '/senders/short_circuit/transactions'

    def _find_free_port(self):
        with socketserver.TCPServer(("localhost", 0), lambda x,y,z: None) as s:
            return s.server_address[1]

    def _update_rest_endpoint(self, yaml):
        for e in yaml:
            if e['name'] == 'gateway':
                e['endpoint'] = urljoin(self.gateway_base_url, self.router_path)
            elif e['name'] == 'short_circuit':
                e['endpoint'] = urljoin(self.router_base_url, self.short_circuit_path)
            elif e['name'] == 'sink':
                e['endpoint'] = urljoin(self.receiver_base_url, self.router_path)


    def _update_address_list(self, policy):
        logging.debug('_update_address_list %s', policy)
        if (dest := policy.get('destination', None)) is None:
            return
        logging.debug(dest)

        endpoint = dest.get('endpoint', None)
        if endpoint == 'gateway':
            dest['host_list'] = [
                {'host': 'fake_smtpd', 'port': self.fake_smtpd_port}]
        elif endpoint == 'sink':
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
        last = 0
        dkim = None
        for i,f in enumerate(chain):
            if f['filter'] == 'dkim':
                dkim = i
                break
        filter_yaml = chain[dkim]
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
        self.gateway_base_url = 'http://localhost:%d/' % self.gateway_rest_port + self.router_path

        self.receiver_rest_port = self._find_free_port()
        self.receiver_base_url = 'http://localhost:%d' % self.receiver_rest_port + self.router_path

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
        self.router_submission_url = self.router_base_url + '/senders/submission/transactions'
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
            endpoint['endpoint'] = urljoin(self.router_base_url, self.gw_path)
            del endpoint['verify']

        self._update_rest_endpoint(router_yaml['rest_endpoint'])

        for endpoint in router_yaml['endpoint']:
            if endpoint['name'] in ['msa-output', 'submission']:
                self._update_dkim(endpoint['chain'])
            for filter in endpoint['chain']:
                self._update_router(filter)
                if filter['filter'] == 'dns_resolution':
                    filter['static_hosts'] = [
                        {'host': '127.0.0.1', 'port': self.fake_smtpd_port}]
                elif filter['filter'] == 'rest_output':
                    if filter.get('static_endpoint', None) == 'http://localhost:8002/senders/router/transactions':
                        filter['static_endpoint'] = urljoin(self.receiver_base_url, self.router_path)
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
        self.receiver_tempdir = tempfile.TemporaryDirectory()
        self.receiver = Receiver(
            self.receiver_tempdir.name,
            gc_interval=0)  # gc on every access
        self.http_server = uvicorn_main.Server(
            create_app(self.receiver),
            ('localhost', self.receiver_rest_port),
            self.executor.ping_watchdog)
        self.executor.submit(self.http_server.run)

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
        if self.http_server is not None:
            self.http_server.shutdown()
        self.assertTrue(self.executor.shutdown(timeout=60))
        for d in [d for d in [self.dkim_tempdir, self.receiver_tempdir]
                  if d is not None]:
            d.cleanup()
        logging.debug('End2EndTest.tearDown done')


    # mx smtp -> smtp
    def test_smoke(self):
        self._configure_and_run()
        rcpt_resp, final_resp = send_smtp(
            'localhost', self.gateway_mx_port, 'end2end_test',
            'alice@example.com', ['bob@nowhere.com', 'bob@example.com'],
            'hello, world!')
        self.assertEqual(550, rcpt_resp[0][0])
        self.assertEqual(250, rcpt_resp[1][0])

        for handler in self.fake_smtpd.handlers:
            # smtpd machinery constructs extra handlers during startup?
            if handler.ehlo is None:
                logging.debug('empty handler? %s', handler)
                continue
            logging.debug(handler)
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

        with open(tx.tx_json_path, 'rb') as tx_json_file:
            tx_json = json.load(tx_json_file)
            logging.debug(tx_json)
        self.assertEqual('bob@rest-application.example.com',
                         tx_json['rcpt_to'][0]['m'])

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

        sender = Sender(self.router_submission_url,
                        'submission',
                        'alice@example.com',
                        body_filename='testdata/trivial.msg')
        sender.send('bob@example.com')
        sender.force_reuse = True
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

        sender = Sender(self.router_submission_url,
                        'submission',
                        'alice@example.com',
                        message_builder=message_builder_spec)
        sender.send('bob@example.com')
        sender.force_reuse = True
        sender.send('bob2@example.com')

        handlers = {}
        for handler in self.fake_smtpd.handlers:
            logging.debug(handler)
            if len(handler.rcpt_to) != 1:
                logging.debug(handler.rcpt_to)
                continue
            rcpt = handler.rcpt_to[0]
            if rcpt not in ['bob@example.com', 'bob2@example.com']:
                logging.debug(rcpt)
                continue
            self.assertNotIn(rcpt, handlers)
            handlers[rcpt] = handler
            self.assertIn(b'from: alice a <alice@example.com>', handler.data)
            self.assertIn(b'DKIM-Signature:', handler.data)
            self.assertIn(b'Received:', handler.data)
            self.assertIn(b'aGVsbG8sIHdvcmxkIQo=', handler.data)

        self.assertEqual(2, len(handlers))

    # submission rest -> short circuit mx

    def _test_add_route(self, filter_yaml):
        self._configure()
        endpoint_yaml = _get_router_endpoint_yaml(self.router_yaml, 'mx-out')

        add_route_yaml = next(
            y for y in endpoint_yaml['chain'] if y['filter'] == 'add_route')
        add_route_yaml.update(filter_yaml)

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
        return self._test_add_route({'output_chain': 'sink'})

    def test_add_route_async(self):
        return self._test_add_route({'output_chain': 'sink',
                                     'store_and_forard': True})

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d %(message)s')

    unittest.main()
