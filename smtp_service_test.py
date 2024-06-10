from typing import List, Optional, Tuple

import smtplib

import logging
import unittest

import time
import socketserver

from aiosmtpd.controller import Controller

from filter import TransactionMetadata
from smtp_service import SmtpHandler, service
from response import Response
from fake_endpoints import FakeSyncFilter

def find_unused_port() -> int:
    with socketserver.TCPServer(("localhost", 0), lambda x,y,z: None) as s:
        return s.server_address[1]

class SmtpServiceTest(unittest.TestCase):
    def setUp(self):
        self.endpoint = FakeSyncFilter()

        logging.basicConfig(level=logging.DEBUG)
        endpoint_factory = lambda: self.endpoint
        self.controller = service(endpoint_factory, port=find_unused_port())

        logging.debug('controller port %d', self.controller.port)

        self.smtp_client = smtplib.SMTP(
            self.controller.hostname, self.controller.port)

    def tearDown(self):
        self.smtp_client.quit()
        self.controller.stop()
        self.controller = None

    def trans(self,
              mail_from,
              tx_mail_resp : Response,
              exp_mail_resp : Response,
              # (mailbox, tx rcpt resp, exp resp code)
              rcpts : List[Tuple[str,Optional[Response],Response]] = [],
              tx_data_resp : Optional[Response] = None, exp_data_resp = None):
        resp = self.smtp_client.ehlo('gargantua1')
        self.assertEqual(resp[0], 250)

        def exp(tx, tx_delta):
            upstream_delta=TransactionMetadata()
            if tx_mail_resp:
                upstream_delta.mail_response = tx_mail_resp
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        self.endpoint.add_expectation(exp)
        resp = self.smtp_client.mail('alice')
        self.assertEqual(resp[0], exp_mail_resp.code)

        for i,(rcpt, tx_rcpt_resp, exp_rcpt_resp) in enumerate(rcpts):
            def exp(tx, tx_delta):
                upstream_delta=TransactionMetadata()
                updated_tx = tx.copy()
                if tx_rcpt_resp:
                    updated_tx.rcpt_response.append(tx_rcpt_resp)
                upstream_delta = tx.delta(updated_tx)
                self.assertIsNotNone(tx.merge_from(upstream_delta))
                return upstream_delta
            self.endpoint.add_expectation(exp)
            resp = self.smtp_client.rcpt(rcpt)
            self.assertEqual(resp[0], exp_rcpt_resp.code)

        # smtplib.SMTP.data throws SMTPDataError if no valid rcpt
        if exp_data_resp is None:
            return

        def exp(tx, tx_delta):
            upstream_delta=TransactionMetadata()
            updated_tx = tx.copy()
            if tx_data_resp:
                updated_tx.data_response = tx_data_resp
            upstream_delta = tx.delta(updated_tx)
            self.assertIsNotNone(tx.merge_from(upstream_delta))
            return upstream_delta
        self.endpoint.add_expectation(exp)
        resp = self.smtp_client.data(b'hello')
        self.assertEqual(resp[0], exp_data_resp.code)

    def test_mail_fail(self):
        self.trans('alice', Response(550), Response(550),
                   [('bob', None, Response(503))])

    def test_rcpt_fail(self):
        self.trans('alice', Response(250), Response(250),
                   [('bob', Response(550), Response(550))])

    def test_data_fail(self):
        self.trans('alice', Response(250), Response(250),
                   [('bob', Response(550), Response(550)),
                    ('bob2', Response(250), Response(250))],
                   Response(450), Response(450))

    def test_success(self):
        self.trans('alice', Response(250), Response(250),
                   [('bob', Response(550), Response(550)),
                    ('bob2', Response(250), Response(250))],
                   Response(250), Response(250))


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')
    unittest.main()
