# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import logging
import unittest

from koukan.filter import HostPort, Mailbox, TransactionMetadata
from koukan.transaction_matchers import (
    match_invalid_mail_from,
    match_invalid_rcpt_to,
    match_network_address,
    match_num_rcpts,
    match_smtp_auth,
    match_smtp_tls )
from koukan.matcher_result import MatcherResult
from koukan.response import Response

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
    def test_smoke(self):
        self.assertEqual(
            MatcherResult.NO_MATCH,
            match_num_rcpts(
                {'max_rcpts', 1},
                TransactionMetadata(
                    rcpt_to=[Mailbox('bob@example.com'),
                             Mailbox('bob2@example.com')],
                    rcpt_response=[Response(500), None]),
                1))
        self.assertEqual(
            MatcherResult.MATCH,
            match_num_rcpts(
                {'max_rcpts': 1},
                TransactionMetadata(
                    rcpt_to=[Mailbox('bob@example.com'),
                             Mailbox('bob2@example.com')],
                    rcpt_response=[Response(), None]),
                1))

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
