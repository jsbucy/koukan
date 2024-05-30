from typing import Any, List, Optional, Tuple
from threading import Lock, Condition
import logging
import unittest
import socketserver
import time

from requests.exceptions import ConnectionError

from router_service import Service
from rest_endpoint import RestEndpoint
from response import Response
from blob import CompositeBlob, InlineBlob
from config import Config
from fake_endpoints import SyncEndpoint
from filter import Filter, HostPort, Mailbox, TransactionMetadata
from executor import Executor

root_yaml = {
    'global': {
        'use_gunicorn': False,
        'use_hypercorn': False,
        'tx_idle_timeout': 5,
        'gc_interval': None,
        'dequeue': False,
        'mailer_daemon_mailbox': 'mailer-daemon@d',
        'rest_id_entropy': 2
    },
    'rest_listener': {
    },
    'endpoint': [
        {
            'name': 'smtp-msa',
            'msa': True,
            'output_handler': {
                'downstream_env_timeout': 1,
                'downstream_data_timeout': 1,
                'retry_params': {
                    'max_attempts': 3,
                    'min_attempt_time': 1,
                    'max_attempt_time': 1,
                    'backoff_factor': 0,
                    'deadline': 300,
                }
            },
            'chain': [{'filter': 'exploder',
                       'output_chain': 'submission',
                       'msa': True,
                       'rcpt_timeout': 1,
                       'data_timeout': 1,
                       'default_notification': {
                           'host': 'submission'
                       }}]
        },
        {
            'name': 'submission',
            'msa': True,
            'output_handler': {
                'downstream_env_timeout': 1,
                'downstream_data_timeout': 1,
                'retry_params': {
                    'max_attempts': 3,
                    'min_attempt_time': 1,
                    'max_attempt_time': 1,
                    'backoff_factor': 0,
                    'deadline': 300,
                }
            },
            'chain': [{'filter': 'sync'}]
        },
        {
            'name': 'smtp-in',
            'msa': True,
            'output_handler': {
                'downstream_env_timeout': 1,
                'downstream_data_timeout': 1,
            },
            'chain': [{'filter': 'exploder',
                       'output_chain': 'inbound-gw',
                       'msa': False,
                       'default_notification': {
                           'host': 'inbound-gw'
                       }}]
        },
        {
            'name': 'inbound-gw',
            'msa': True,
            'output_handler': {
                'downstream_env_timeout': 1,
                'downstream_data_timeout': 1
            },
            'chain': [{'filter': 'sync'}]
        },
    ],
    'storage': {
        'engine': 'sqlite_memory'
    }
}

class RouterServiceTest(unittest.TestCase):
    lock : Lock
    cv : Condition
    endpoints : List[SyncEndpoint]
    executor : Executor

    def get_endpoint(self):
        with self.lock:
            self.cv.wait_for(lambda: bool(self.endpoints))
            return self.endpoints.pop(0)

    def add_endpoint(self, endpoint):
        with self.lock:
            self.endpoints.append(endpoint)
            self.cv.notify_all()

    def setUp(self):
        self.lock = Lock()
        self.cv = Condition(self.lock)

        self.executor = Executor(inflight_limit=10, watchdog_timeout=300)

        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s [%(thread)d] %(message)s')

        # find a free port
        with socketserver.TCPServer(("localhost", 0), lambda x,y,z: None) as s:
            self.port = s.server_address[1]
        root_yaml['rest_listener']['addr'] = ('127.0.0.1', self.port)
        self.router_url = 'http://localhost:%d' % self.port
        self.endpoints = []
        self.config = Config()
        self.config.inject_yaml(root_yaml)
        self.config.inject_filter(
            'sync', lambda yaml, next: self.get_endpoint(), Filter)
        self.service = Service(config=self.config,
                               executor = self.executor)
        self.executor.submit(lambda: self.service.main())

        self.assertTrue(self.service.wait_started(1))

        # probe for startup
        s = SyncEndpoint()
        s.set_mail_response(Response())
        self.add_endpoint(s)

        for i in range(1,3):
            logging.info('RouterServiceTest.setUp probe %d', i)
            rest_endpoint = RestEndpoint(
                static_base_url=self.router_url,
                http_host='submission',
                timeout_start=1, timeout_data=1)
            tx = TransactionMetadata(
                mail_from = Mailbox('probe-from%d' % i))
            t = self.start_tx_update(rest_endpoint, tx)
            self.service._dequeue()
            self.join_tx_update(t)
            logging.info('RouterServiceTest.setUp %s', tx.mail_response)
            if tx.mail_response.ok():
                break
            time.sleep(0.1)
        else:
            self.fail('service not ready')

        self.assertFalse(self.endpoints)

        # gc the probe request so it doesn't cause confusion later
        self.assertTrue(self.service._gc(0))

    def tearDown(self):
        # TODO this should verify that there are no open tx attempts in storage
        # e.g. some exception path failed to tx_cursor.finalize_attempt()
        self.service.shutdown()
        self.executor.shutdown(timeout=5)

    def _update_tx(self, endpoint, tx, timeout=None):
        logging.debug('RouterServiceTest._update_tx')
        endpoint.on_update(tx, timeout)

    def start_tx_update(self, rest_endpoint, tx, timeout=5):
        return self.executor.submit(
            lambda: self._update_tx(rest_endpoint, tx, timeout))

    def join_tx_update(self, t, timeout=5):
        logging.debug('RouterServiceTest.join_tx_update')
        try:
            t.result(timeout)
        except Exception:
            logging.exception('RouterServiceTest.join_tx_update exception')
        logging.debug('RouterServiceTest.join_tx_update done')

    def dump_db(self):
        with self.service.storage.conn() as conn:
            for l in conn.connection.iterdump():
                logging.debug(l)

    def assertRcptCodesEqual(self, responses : List[Optional[Response]],
                             expected_codes):
        self.assertEqual([r.code if r else None for r in responses],
                         expected_codes)

    def assertRcptJsonCodesEqual(self, resp_json, expected_codes):
        self.assertRcptCodesEqual(
            [Response.from_json(r) for r in resp_json.get('rcpt_response', [])],
            expected_codes)

    # native rest
    # first attempt tempfails at rcpt
    # retry+success
    def test_retry(self):
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, http_host='submission',
            timeout_start=5, timeout_data=5)
        tx = TransactionMetadata(retry={})
        rest_endpoint.on_update(tx)
        tx_json = rest_endpoint.get_json()
        logging.debug('RouterServiceTest.test_retry create %s',
                      tx_json)
        self.assertEqual(tx_json, {
            'retry': {}
        })

        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        rest_endpoint.on_update(tx)
        tx_json = rest_endpoint.get_json()
        logging.debug('RouterServiceTest.test_retry patch mail_from %s',
                      tx_json)
        self.assertEqual(tx_json, {
            'retry': {},
            'mail_from': {}
        })

        tx_json = rest_endpoint.get_json(5)
        logging.debug('RouterServiceTest.test_retry get after mail_from %s',
                      tx_json)
        tx = TransactionMetadata(rcpt_to = [Mailbox('bob')])
        rest_endpoint.on_update(tx)
        tx_json = rest_endpoint.get_json()
        logging.debug('RouterServiceTest.test_retry patch rcpt_to %s',
                      tx_json)
        self.assertEqual(tx_json, {
            'retry': {},
            'mail_from': {},
            'rcpt_to': [{}]
        })

        tx_json = rest_endpoint.get_json(5)
        logging.debug('RouterServiceTest.test_retry get after rcpt_to %s',
                      tx_json)

        body_blob = CompositeBlob()
        b = InlineBlob(b'hello')
        body_blob.append(b, 0, b.len())
        tx = TransactionMetadata(body_blob=body_blob)
        rest_endpoint.on_update(tx)
        self.assertIsNone(tx.data_response)

        b = InlineBlob(b'world')
        body_blob.append(b, 0, b.len(), True)
        tx = TransactionMetadata(body_blob=body_blob)
        rest_endpoint.on_update(tx)
        self.assertTrue(tx.data_response.temp())

        logging.debug('RouterServiceTest.test_retry get after set body')
        tx_json = rest_endpoint.get_json(5)
        logging.debug('RouterServiceTest.test_retry get after set body %s',
                      tx_json)
        self.assertEqual(tx_json, {
            'retry': {},
            'mail_from': {},
            'rcpt_to': [{}],
            'body': {}
        })

        upstream_endpoint = SyncEndpoint()
        self.add_endpoint(upstream_endpoint)
        self.assertTrue(self.service._dequeue())

        # set upstream responses so output tempfails at rcpt
        upstream_endpoint.set_mail_response(Response(201))
        upstream_endpoint.add_rcpt_response(Response(456))

        tx_json = rest_endpoint.get_json(5)

        logging.debug('RouterServiceTest.test_retry get after first attempt '
                      '%s', tx_json)
        self.assertEqual(tx_json, {
            'attempt_count': 1,
            'retry': {},
            'mail_from': {},
            'rcpt_to': [{}],
            'body': {},
            'mail_response': {'code': 201, 'message': 'ok'},
            'rcpt_response': [{'code': 456, 'message': 'ok'}],
        })

        upstream_endpoint = SyncEndpoint()
        self.add_endpoint(upstream_endpoint)
        time.sleep(2)  # max_attempt_time
        self.assertTrue(self.service._dequeue())

        # set upstream responses so output (retry) succeeds
        # upstream success, retry succeeds, propagates down to rest
        upstream_endpoint.set_mail_response(Response(201))
        upstream_endpoint.add_rcpt_response(Response(202))
        upstream_endpoint.add_data_response(Response(203))

        tx_json = rest_endpoint.get_json(5)
        logging.debug('RouterServiceTest.test_retry get after second attempt '
                      '%s', tx_json)
        self.assertEqual(tx_json, {
            'attempt_count': 1,
            'retry': {},
            'mail_from': {},
            'rcpt_to': [{}],
            'body': {},
            'mail_response': {'code': 201, 'message': 'ok'},
            'rcpt_response': [{'code': 202, 'message': 'ok'}],
            'data_response': {'code': 203, 'message': 'ok'},
        })

    # xxx need non-exploder test w/filter api, problems in
    # post-exploder/upstream tx won't be reported out synchronously,
    # would potentially bounce
    # and/or get ahold of those tx IDs and verify the status directly

    def test_exploder_multi_rcpt(self):
        logging.info('testExploderMultiRcpt')
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, http_host='smtp-msa')

        logging.info('testExploderMultiRcpt start tx')
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            remote_host=HostPort('1.2.3.4', 12345))
        t = self.start_tx_update(rest_endpoint, tx)

        time.sleep(0.1)
        # exploder tx
        self.assertTrue(self.service._dequeue())

        self.join_tx_update(t)

        # no rcpt -> buffered
        self.assertEqual(tx.mail_response.code, 250)

        logging.info('testExploderMultiRcpt patch rcpt1')
        tx = TransactionMetadata(rcpt_to=[Mailbox('bob')])
        t = self.start_tx_update(rest_endpoint, tx)

        # upstream tx #1
        upstream_endpoint = SyncEndpoint()
        self.add_endpoint(upstream_endpoint)
        self.assertTrue(self.service._dequeue())

        upstream_endpoint.set_mail_response(Response(250))
        upstream_endpoint.add_rcpt_response(Response(202))

        self.join_tx_update(t)
        self.assertRcptCodesEqual(tx.rcpt_response, [202])

        logging.info('testExploderMultiRcpt patch rcpt2')
        tx = TransactionMetadata(rcpt_to=[Mailbox('bob2')])
        t = self.start_tx_update(rest_endpoint, tx)

        # upstream tx #2
        upstream_endpoint2 = SyncEndpoint()
        self.add_endpoint(upstream_endpoint2)
        self.assertTrue(self.service._dequeue())

        upstream_endpoint2.set_mail_response(Response(250))
        upstream_endpoint2.add_rcpt_response(Response(203))

        self.join_tx_update(t)
        self.assertRcptCodesEqual(tx.rcpt_response, [203])

        # output of exploder tx buffers whole payload?
        upstream_endpoint.add_data_response(Response(204))
        upstream_endpoint2.add_data_response(Response(204))

        logging.info('testExploderMultiRcpt patch body_blob')

        tx = TransactionMetadata(
            body_blob=InlineBlob(b'Hello, World!'))
        rest_endpoint.on_update(tx)
        self.assertEqual(tx.data_response.code, 204)

    def test_notification(self):
        logging.info('test_notification')
        rest_endpoint = RestEndpoint(
            static_base_url=self.router_url, http_host='smtp-msa')


        logging.info('test_notification start tx')
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')],
            body_blob=InlineBlob(b'Hello, World!'),
            remote_host=HostPort('1.2.3.4', 12345))
        t = self.start_tx_update(rest_endpoint, tx, 10)
        # exploder tx
        self.assertTrue(self.service._dequeue())
        self.join_tx_update(t, 10)
        logging.debug('RouterServiceTest.test_notification after update %s',
                      tx)
        # msa store and forward, upstream temp errors upgraded to 250 downstream
        self.assertEqual(tx.mail_response.code, 250)
        self.assertEqual([r.code for r in tx.rcpt_response], [250])
        self.assertEqual(tx.data_response.code, 250)

        for i in range(0,3):
            logging.debug('test_notification upstream tx %d', i)
            upstream_endpoint = SyncEndpoint()
            upstream_endpoint.set_mail_response(Response(250))
            upstream_endpoint.add_rcpt_response(Response(450))

            self.add_endpoint(upstream_endpoint)
            for j in range(0,3):
                if self.service._dequeue():
                    break
                time.sleep(1)
            else:
                self.fail('failed to dequeue')

        dsn_endpoint = SyncEndpoint()
        dsn_endpoint.set_mail_response(Response(250))
        dsn_endpoint.add_rcpt_response(Response(250))
        dsn_endpoint.add_data_response(Response(250))
        self.add_endpoint(dsn_endpoint)
        self.assertTrue(self.service._dequeue())

        # xxx kludge, fix SyncEndpoint to wait on this
        for i in range(0,5):
            if dsn_endpoint.body_blob is not None:
                break
            time.sleep(1)
        else:
            self.fail('didn\'t get dsn')
        self.assertEqual(dsn_endpoint.mail_from.mailbox, '')
        self.assertEqual([m.mailbox for m in dsn_endpoint.rcpt_to],
                         ['alice'])
        self.assertIn(b'subject: Delivery Status Notification',
                      dsn_endpoint.body_blob.read(0))

if __name__ == '__main__':
    unittest.removeHandler()
    unittest.main(catchbreak=False)
