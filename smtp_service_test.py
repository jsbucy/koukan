from typing import List, Optional, Tuple

import smtplib

import logging
import unittest

import time

from aiosmtpd.controller import Controller

from filter import TransactionMetadata
from smtp_service import SmtpHandler
from response import Response
from fake_endpoints import SyncEndpoint

class SmtpServiceTest(unittest.TestCase):
    def setUp(self):
        self.endpoint = SyncEndpoint()

        logging.basicConfig(level=logging.DEBUG)
        self.handler = SmtpHandler(lambda: self.endpoint)
        self.controller = Controller(self.handler)
        self.controller.start()

        self.smtp_client = smtplib.SMTP(
            self.controller.hostname, self.controller.port)


    def tearDown(self):
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

        if tx_mail_resp:
            self.endpoint.set_mail_response(tx_mail_resp)
        resp = self.smtp_client.mail('alice')
        self.assertEqual(resp[0], exp_mail_resp.code)

        for i,(rcpt, tx_rcpt_resp, exp_rcpt_resp) in enumerate(rcpts):
            if tx_rcpt_resp:
                self.endpoint.add_rcpt_response(tx_rcpt_resp)
            resp = self.smtp_client.rcpt(rcpt)
            self.assertEqual(resp[0], exp_rcpt_resp.code)

        # smtplib.SMTP.data throws SMTPDataError if no valid rcpt
        if exp_data_resp is None:
            return

        if tx_data_resp:
            self.endpoint.add_data_response(tx_data_resp)
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
