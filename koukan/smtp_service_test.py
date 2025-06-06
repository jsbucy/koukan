# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import List, Optional, Tuple

import smtplib

import logging
import unittest

import time
import socketserver

from aiosmtpd.controller import Controller

from koukan.filter import TransactionMetadata
from koukan.smtp_service import SmtpHandler, service
from koukan.response import Response
from koukan.fake_endpoints import FakeSyncFilter
from koukan.blob import InlineBlob
from koukan.executor import Executor

def find_unused_port() -> int:
    with socketserver.TCPServer(("localhost", 0), lambda x,y,z: None) as s:
        return s.server_address[1]

class SmtpServiceTest(unittest.TestCase):
    def setUp(self):
        self.endpoint = FakeSyncFilter()

        logging.basicConfig(level=logging.DEBUG)
        handler_factory = lambda: SmtpHandler(
            endpoint_factory = lambda: self.endpoint,
            executor = Executor(inflight_limit=100, watchdog_timeout=3600),
            chunk_size = 16
        )
        self.controller = service(
            port=find_unused_port(),
            smtp_handler_factory = handler_factory)

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

        b = b''
        for i in range(0,10):
            b += b'hello, world! %d\r\n' % i
        logging.debug(b)

        body = InlineBlob(b'')
        def exp_body(tx, tx_delta):
            upstream_delta=TransactionMetadata()
            logging.debug(tx.body)
            if tx.body:
                body.append(tx.body.pread(body.len()))
                logging.debug(body.pread(0))
            if tx.body.finalized() and tx_data_resp:
                self.assertEqual(b, body.pread(0))
                upstream_delta.data_response = tx_data_resp
            self.assertIsNotNone(tx.merge_from(upstream_delta))
            return upstream_delta
        for i in range(0,11):
            self.endpoint.add_expectation(exp_body)
        resp = self.smtp_client.data(b)
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
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d %(message)s')
    unittest.main()
