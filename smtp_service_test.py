

from smtp_service import SmtpHandler

from aiosmtpd.controller import Controller

import smtplib

import logging
import unittest

import time

from response import Response

class FakeRestEndpoint:
    def __init__(self):
        self.rcpt = None
        self.durable = None
        self.last = False

    def start(self, local, remote, mail_from, transaction_esmtp,
              rcpt, rcpt_esmtp):
        self.rcpt = rcpt
        if rcpt.startswith('rcpttemp'):
            return Response(code=400)
        elif rcpt.startswith('rcptperm'):
            return Response(code=500)
        return Response()

    def append_data(self, last, blob, mx_multi_rcpt):
        logging.info('FakeRestEndpoint.append_data')
        assert(not(self.last))
        if last:
            self.last = True
        if self.rcpt.startswith('datatemp'):
            return Response(code=400)
        elif self.rcpt.startswith('dataperm'):
            return Response(code=500)
        return Response()

    def set_durable(self):
        # TODO for the time being, set_durable being called
        # before/concurrently with append (which returns
        # instantaneously) *in this test* is a bug even though the api
        # allows it. Revisit when we add testing for slow/timeout
        # upstream.
        assert(self.last)
        self.durable = True
        return Response()


class SmtpServiceTest(unittest.TestCase):
    def add_endpoint(self):
        e = FakeRestEndpoint()
        self.endpoints.append(e)
        return e

    def setUp(self):
        self.endpoints = []

        logging.basicConfig(level=logging.DEBUG)
        self.handler = SmtpHandler(
            lambda: self.add_endpoint(),
            msa=True, max_rcpt=100)
        self.controller = Controller(self.handler)
        self.controller.start()

        self.smtp_client = smtplib.SMTP(
            self.controller.hostname, self.controller.port)


    def tearDown(self):
        self.controller.stop()
        self.controller = None

    def trans(self, rcpts, exp_data_resp, durable=None):
        resp = self.smtp_client.ehlo('gargantua1')
        self.assertEqual(resp[0], 250)
        resp = self.smtp_client.mail('alice')
        self.assertEqual(resp[0], 250)

        for (rcpt, exp_rcpt_resp) in rcpts:
            resp = self.smtp_client.rcpt(rcpt)
            self.assertEqual(resp[0], exp_rcpt_resp)

        if exp_data_resp is None: return

        resp = self.smtp_client.data(b'hello')
        self.assertEqual(resp[0], exp_data_resp)

        self.assertEqual(len(self.endpoints), len(rcpts))
        for (i,e) in enumerate(self.endpoints):
            rcpt_resp = rcpts[i][1]
            if rcpt_resp != 500:
                self.assertEqual(durable, e.durable)
            else:
                self.assertIsNone(e.durable)


    def test_msa_success(self):
        self.trans([('bob', 250)],
                   200)

    def test_msa_multi_success(self):
        self.trans([('bob', 250),
                    ('bob2', 250)],
                   200)

    def test_msa_rcpt_temp(self):
        self.trans([('rcpttemp', 250)],
                   250,
                   durable=True)

    def test_msa_rcpt_perm(self):
        self.trans([('rcptperm', 500)],
                   None)

    def test_msa_data_perm(self):
        self.trans([('dataperm1', 250),
                    ('dataperm2', 250)],
                   500)

    # msa swallows rcpt temp errors but propagates perm errors
    def test_msa_mixed(self):
        self.trans([('bob', 250),
                    ('rcpttemp', 250),
                    ('rcptperm', 500),
                    ('datatemp', 250),
                    ('dataperm', 250)],
                   250,
                   durable=True)

    def mx_single(self, rcpt, exp_rcpt_resp, exp_data_resp):
        self.handler.msa = False
        self.trans([(rcpt, exp_rcpt_resp)], exp_data_resp)

    def test_mx_single_success(self):
        self.mx_single('bob', 250, 200)

    def test_mx_single_rcpttemp(self):
        self.mx_single('rcpttemp', 400, None)

    def test_mx_single_rcptperm(self):
        self.mx_single('rcptperm', 500, None),

    def test_mx_single_datatemp(self):
        self.mx_single('datatemp', 250, 400),

    def test_mx_single_dataperm(self):
        self.mx_single('dataperm', 250, 500)

    def mx_multi(self, rcpt1, exp_rcpt1_resp, rcpt2, exp_rcpt2_resp,
                 exp_data_resp):
        self.handler.msa = False
        self.trans([(rcpt1, exp_rcpt1_resp),
                    (rcpt2, exp_rcpt2_resp)], exp_data_resp)

    def test_mx_multi_success(self):
        self.mx_multi('bob1', 250, 'bob2', 250, 200)

    def test_mx_multi_temp(self):
        self.mx_multi('datatemp1', 250, 'datatemp2', 250, 400)

    def test_mx_multi_perm(self):
        self.mx_multi('dataperm1', 250, 'dataperm2', 250, 500)

    def test_mx_multi_mixed(self):
        self.handler.msa = False
        self.trans([('bob', 250),
                    ('datatemp', 250),
                    ('dataperm', 250)],
                    250,
                   durable=True)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')
    unittest.main()
