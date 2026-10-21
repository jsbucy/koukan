# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import List, Optional, Tuple

from koukan_cpython_smtplib.smtplib import SMTP as SmtpClient

import logging
import unittest

import time
import socketserver

from aiosmtpd.controller import Controller

from koukan.filter import TransactionMetadata
from koukan.smtp_service import SmtpHandler, service
from koukan.response import Response
from koukan.fake_endpoints import FakeFilter
from koukan.blob import InlineBlob
from koukan.executor import Executor
from koukan.filter_chain import FilterChain

def find_unused_port() -> int:
    with socketserver.TCPServer(("localhost", 0), lambda x,y,z: socketserver.BaseRequestHandler(x,y,z)) as s:
        return s.server_address[1]

class SmtpServiceTest(unittest.TestCase):
    def setUp(self):
        self.endpoint = FakeFilter()
        self.chain = FilterChain([self.endpoint])
        self.executor = Executor(inflight_limit=100, watchdog_timeout=10)

        logging.basicConfig(level=logging.DEBUG)
        handler_factory = lambda: SmtpHandler(
            chain_factory = lambda: self.chain,
            timeout_mail=4,
            timeout_rcpt=4,
            timeout_data=4,
            executor = self.executor,
            sender = 'gateway',
            tag = 'mx',
            chunk_size = 64
        )
        self.controller = service(
            port=find_unused_port(),
            smtp_handler_factory = handler_factory)
        # TODO setting chunk_size depends on our patches to aiosmtpd
        # and there isn't an easy way to detect that

        logging.debug('controller port %d', self.controller.port)

        self.smtp_client = SmtpClient(
            self.controller.hostname, self.controller.port)

    def tearDown(self):
        if self.smtp_client:
            self.smtp_client.quit()
        self.controller.stop()
        self.controller = None
        self.executor.shutdown(10)

    def trans(self,
              mail_from,
              tx_mail_resp : Response,
              exp_mail_resp : Response,
              # (mailbox, tx rcpt resp, exp resp code)
              rcpts : List[Tuple[str,Optional[Response],Response]] = [],
              tx_data_resp : Optional[Response] = None, exp_data_resp = None,
              mail_timeout = False,
              rcpt_timeout = False,
              data_timeout = False):
        resp = self.smtp_client.ehlo('gargantua1')
        self.assertEqual(resp[0], 250)

        def exp_cancel(tx, tx_delta):
            logging.debug('cancelled')
            self.assertTrue(tx.cancelled)

        def exp_timeout(tx, tx_delta):
            logging.debug(tx)
            logging.debug('timeout')
            time.sleep(5)

        def exp_mail(tx, tx_delta):
            logging.debug(tx)
            if tx_mail_resp:
                tx.mail_response = tx_mail_resp
        if mail_timeout:
            self.endpoint.add_expectation(exp_timeout)
            self.endpoint.add_expectation(exp_cancel)
        else:
            self.endpoint.add_expectation(exp_mail)
        resp = self.smtp_client.mail('alice')
        self.assertEqual(resp[0], exp_mail_resp.code)

        for i,(rcpt, tx_rcpt_resp, exp_rcpt_resp) in enumerate(rcpts):
            def exp_rcpt(tx, tx_delta):
                logging.debug(tx)
                if tx_rcpt_resp:
                    tx.rcpt_response.append(tx_rcpt_resp)
            if rcpt_timeout:
                self.endpoint.add_expectation(exp_timeout)
                self.endpoint.add_expectation(exp_cancel)
            else:
                self.endpoint.add_expectation(exp_rcpt)
            resp = self.smtp_client.rcpt(rcpt)
            self.assertEqual(resp[0], exp_rcpt_resp.code)

        # smtplib.SMTP.data throws SMTPDataError if no valid rcpt
        if exp_data_resp is None:
            self.endpoint.add_expectation(exp_cancel)
            return

        b = b''
        for i in range(0,10):
            b += b'hello, world! %d\r\n' % i
        logging.debug(b)

        body = InlineBlob(b'')
        def exp_body(tx, tx_delta):
            logging.debug(tx)
            if tx.body:
                body.append(tx.body.pread(body.len()))
                logging.debug(body.pread(0))
            if tx.body.finalized() and tx_data_resp:
                self.assertEqual(b, body.pread(0))
                tx.data_response = tx_data_resp
        if data_timeout:
            self.endpoint.add_expectation(exp_timeout)
            self.endpoint.add_expectation(exp_cancel)
        else:
            for i in range(0, 11):
                self.endpoint.add_expectation(exp_body)
        resp = self.smtp_client.data(b)
        self.assertEqual(resp[0], exp_data_resp.code)

    def test_mail_fail(self):
        self.trans('alice', Response(550), Response(550),
                   [('bob', None, Response(503))])

    def test_mail_timeout(self):
        self.trans('alice', None, Response(450),
                   [('bob', None, Response(503))],
                   mail_timeout=True)

    def test_rcpt_fail(self):
        self.trans('alice', Response(250), Response(250),
                   [('bob', Response(550), Response(550))])

    def test_rcpt_timeout(self):
        self.trans('alice', Response(250), Response(250),
                   [('bob', Response(550), Response(450))],
                   rcpt_timeout=True)

    def test_data_fail(self):
        self.trans('alice', Response(250), Response(250),
                   [('bob', Response(550), Response(550)),
                    ('bob2', Response(250), Response(250))],
                   Response(450), Response(450))

    def test_data_timeout(self):
        self.trans('alice', Response(250), Response(250),
                   [('bob', Response(550), Response(550)),
                    ('bob2', Response(250), Response(250))],
                   Response(450), Response(450), data_timeout=True)

    def test_success(self):
        self.trans('alice', Response(250), Response(250),
                   [('bob', Response(550), Response(550)),
                    ('bob2', Response(250), Response(250))],
                   Response(250), Response(250))

    def test_helo_rset(self):
        resp = self.smtp_client.helo('gargantua1')
        self.assertEqual(resp[0], 250)

        def exp_mail(tx, tx_delta):
            logging.debug(tx)
            tx.mail_response = Response(250)
        self.endpoint.add_expectation(exp_mail)
        resp = self.smtp_client.mail('alice')
        self.assertEqual(resp[0], 250)

        def exp_cancel(tx, tx_delta):
            logging.debug('cancelled')
            self.assertTrue(tx.cancelled)

        self.endpoint.add_expectation(exp_cancel)
        resp = self.smtp_client.rset()
        self.assertEqual(250, resp[0])

        resp = self.smtp_client.quit()
        self.assertEqual(221, resp[0])
        self.smtp_client = None

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d %(message)s')
    unittest.main()
