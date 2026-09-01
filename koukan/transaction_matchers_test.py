# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Callable, List, Optional
import logging
import unittest

from koukan.filter import (
    HostPort,
    Mailbox,
    TransactionGroup,
    TransactionMetadata )
from koukan.transaction_matchers import (
    match_invalid_mail_from,
    match_invalid_rcpt_to,
    match_network_address,
    match_num_rcpts,
    match_smtp_auth,
    match_smtp_tls )
from koukan.matcher_result import MatcherResult
from koukan.response import Response
from koukan.fake_endpoints import FakeTxGroup

class NetworkAddressMatcherTest(unittest.TestCase):
    def test_smoke(self):
        tx = TransactionMetadata()
        self.assertEqual(MatcherResult.PRECONDITION_UNMET,
                         match_network_address({'cidr': '1.0.0.0/8'}, tx,
                                               rcpt_num=None))
        tx.remote_host = HostPort('1.2.3.4', 8000)
        self.assertEqual(MatcherResult.MATCH,
                         match_network_address({'cidr': '1.0.0.0/8'}, tx,
                                               rcpt_num=None))
        self.assertEqual(
            MatcherResult.NO_MATCH,
            match_network_address({'cidr': '1.1.0.0/16'}, tx, rcpt_num=None))

class TlsMatcherTest(unittest.TestCase):
    def test_smoke(self):
        tx = TransactionMetadata()
        self.assertEqual(MatcherResult.PRECONDITION_UNMET,
                         match_smtp_tls({}, tx, rcpt_num=None))
        tx.smtp_meta = {'tls': True}
        self.assertEqual(MatcherResult.MATCH, match_smtp_tls({}, tx,
                                                             rcpt_num=None))

class SmtpAuthMatcherTest(unittest.TestCase):
    def test_smoke(self):
        self.assertEqual(
            MatcherResult.PRECONDITION_UNMET,
            match_smtp_auth({}, TransactionMetadata(), rcpt_num=None))
        self.assertEqual(
            MatcherResult.NO_MATCH,
            match_smtp_auth({}, TransactionMetadata(smtp_meta={}),
                            rcpt_num=None))
        self.assertEqual(
            MatcherResult.NO_MATCH,
            match_smtp_auth({}, TransactionMetadata(smtp_meta={'auth': False}),
                            rcpt_num=None))
        self.assertEqual(
            MatcherResult.MATCH,
            match_smtp_auth({}, TransactionMetadata(smtp_meta={'auth': True}),
                            rcpt_num=None))


class NumRcptsMatcherTest(unittest.TestCase):
    def test_smoke(self) -> None:
        tx0 = TransactionMetadata(
            mail_from=Mailbox('alice@example.com'),
            mail_response=Response(550),
            rcpt_to=[Mailbox('bob0@example.com')],
            rcpt_response=[None],  # inflight
            group_index=0)
        tx1 = TransactionMetadata(
            rcpt_to=[Mailbox('bob1@example.com')],
            rcpt_response=[None],
            group_index=1)
        tx2 = TransactionMetadata(
            rcpt_to=[Mailbox('bob2@example.com')],
            rcpt_response=[None],
            group_index=1)

        # retries: no group
        self.assertEqual(
            MatcherResult.NO_MATCH,
            match_num_rcpts({'max_rcpts': 1}, tx1, rcpt_num = 0))

        tx0.group = tx1.group = tx2.group = FakeTxGroup([tx0, tx1, tx2])

        # prev tx err
        self.assertEqual(
            MatcherResult.NO_MATCH,
            match_num_rcpts({'max_rcpts': 1}, tx1, rcpt_num = 0))
        tx0.mail_response = Response()

        # prev rcpt still inflight
        self.assertEqual(
            MatcherResult.PRECONDITION_UNMET,
            match_num_rcpts({'max_rcpts': 1}, tx1, rcpt_num = 0))

        # prev rcpt err
        tx0.rcpt_response[0] = Response(500)
        self.assertEqual(
            MatcherResult.NO_MATCH,
            match_num_rcpts({'max_rcpts': 1}, tx1, rcpt_num = 0))

        # prev rcpt success
        tx0.rcpt_response[0] = Response()
        self.assertEqual(
            MatcherResult.MATCH,
            match_num_rcpts({'max_rcpts': 1}, tx1, rcpt_num=0))

    # ~Exploder downstream
    def test_single_tx(self) -> None:
        logging.debug('no group')
        tx0 = TransactionMetadata(
            mail_from=Mailbox('alice@example.com'),
            mail_response=Response(),
            rcpt_to=[Mailbox('bob0@example.com'),
                     Mailbox('bob1@example.com')],
            rcpt_response=[None, None],  # inflight
            group_index=0)
        tx0.group = FakeTxGroup([tx0])

        self.assertEqual(
            MatcherResult.NO_MATCH,
            match_num_rcpts({'max_rcpts': 1}, tx0, rcpt_num = 0))
        tx0.rcpt_response[0] = Response()
        self.assertEqual(
            MatcherResult.NO_MATCH,
            match_num_rcpts({'max_rcpts': 1}, tx0, rcpt_num = 0))
        self.assertEqual(
            MatcherResult.MATCH,
            match_num_rcpts({'max_rcpts': 1}, tx0, rcpt_num = 1))


class MatchInvalidMailFromTest(unittest.TestCase):
    def test_smoke(self):
        self.assertEqual(
            MatcherResult.PRECONDITION_UNMET,
            match_invalid_mail_from(
                {}, TransactionMetadata(), None))

        self.assertEqual(
            MatcherResult.MATCH,
            match_invalid_mail_from(
                {}, TransactionMetadata(mail_from=Mailbox('alice')), None))

        self.assertEqual(
            MatcherResult.NO_MATCH,
            match_invalid_mail_from(
                {}, TransactionMetadata(mail_from=Mailbox('')), None))

        self.assertEqual(
            MatcherResult.NO_MATCH,
            match_invalid_mail_from(
                {}, TransactionMetadata(
                    mail_from=Mailbox('alice@example.com')), None))

class MatchInvalidRcptToTest(unittest.TestCase):
    def test_smoke(self):
        self.assertEqual(
            MatcherResult.MATCH,
            match_invalid_rcpt_to(
                {}, TransactionMetadata(rcpt_to=[Mailbox('alice')]), 0))

        self.assertEqual(
            MatcherResult.NO_MATCH,
            match_invalid_rcpt_to(
                {}, TransactionMetadata(
                    rcpt_to=[Mailbox('alice@example.com')]), 0))


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d %(message)s')
    unittest.main()
