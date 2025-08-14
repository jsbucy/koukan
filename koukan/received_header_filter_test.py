# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import unittest
import logging
from datetime import datetime, timezone

from koukan.blob import InlineBlob
from koukan.filter import (
    EsmtpParam,
    HostPort,
    Mailbox,
    Response,
    TransactionMetadata )
from koukan.filter_chain import FilterResult
from koukan.received_header_filter import ReceivedHeaderFilter

class ReceivedHeaderFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
            '%(message)s')

    def test_smoke(self):
        delta = TransactionMetadata(
            remote_host=HostPort('1.2.3.4', port=25000),
            mail_from=Mailbox('alice'))
        delta.remote_hostname = 'gargantua1'
        delta.fcrdns = True

        delta.smtp_meta = {
            'ehlo_host': 'gargantua1',
            'esmtp': True,
            'tls': True
        }

        filter = ReceivedHeaderFilter(
            received_hostname = 'gargantua1',
            inject_time = datetime.fromtimestamp(1234567890, timezone.utc))
        tx = TransactionMetadata()
        filter.wire_downstream(tx)
        filter.wire_upstream(TransactionMetadata())

        tx.merge_from(delta)
        result = filter.on_update(delta)
        self.assertIsNotNone(filter.upstream_tx.smtp_meta)
        self.assertIsNone(result.downstream_delta)

        delta.rcpt_to.append(Mailbox('bob@domain'))
        body = (b'From: <alice>\r\n'
                b'To: <bob>\r\n'
                b'Received: from somewhere-else.example.com with ESMTP;\r\n'
                b'\tFri, 13 Feb 2009 23:31:29 +0000\r\n'
                b'\r\n'
                b'hello\r\n')
        delta.body = InlineBlob(body[0:20], len(body))
        tx.merge_from(delta)
        result = filter.on_update(delta)
        self.assertIsNone(result.downstream_delta)

        tx.body = InlineBlob(body[0:30], len(body))
        tx_delta = TransactionMetadata(body = tx.body)

        result = filter.on_update(tx_delta)
        self.assertIsNone(result.downstream_delta)


        tx.body = tx_delta.body = InlineBlob(body, len(body))
        result = filter.on_update(tx_delta)
        self.assertEqual(
            filter.upstream_tx.body.pread(0),
            b'Received: from gargantua1 (gargantua1 [1.2.3.4])\r\n'
            b'\tby gargantua1\r\n'
            b'\twith ESMTPS\r\n'
            b'\tfor bob@domain;\r\n'
            b'\tFri, 13 Feb 2009 23:31:30 +0000\r\n'
            b'From: <alice>\r\n'
            b'To: <bob>\r\n'
            b'Received: from somewhere-else.example.com with ESMTP;\r\n'
            b'\tFri, 13 Feb 2009 23:31:29 +0000\r\n'
            b'\r\n'
            b'hello\r\n')

        self.assertIsNone(result.downstream_delta)
        # self.assertEqual(tx.mail_response.code, 201)
        # self.assertEqual([r.code for r in tx.rcpt_response], [202])

    def test_smtputf8(self):
        filter = ReceivedHeaderFilter(
            received_hostname = 'gargantua1',
            inject_time = datetime.fromtimestamp(1234567890, timezone.utc))
        tx = TransactionMetadata(
            remote_host=HostPort('1.2.3.4', port=25000),
            mail_from=Mailbox('alice', [EsmtpParam('smtputf8')]),
            body=InlineBlob(b'From: <alice>\r\n\r\nhello\r\n',
                                 last=True))
        tx.remote_hostname = 'gargantua1'
        tx.fcrdns = True
        tx.smtp_meta = {
            'ehlo_host': 'gargantua1',
            'esmtp': True,
            'tls': True
        }
        filter.wire_downstream(tx)
        self.assertEqual(
            'Received: from gargantua1 (gargantua1 [1.2.3.4])\r\n'
            '\tby gargantua1\r\n'
            '\twith UTF8SMTPS;\r\n'
            '\tFri, 13 Feb 2009 23:31:30 +0000\r\n',
            filter._format_received())

    def test_max_received_headers(self):
        filter = ReceivedHeaderFilter(
            received_hostname = 'gargantua1',
            inject_time = datetime.fromtimestamp(1234567890, timezone.utc),
            max_received_headers = 1)
        tx = TransactionMetadata()
        filter.wire_downstream(tx)
        filter.wire_upstream(TransactionMetadata())

        delta = TransactionMetadata(
            remote_host=HostPort('1.2.3.4', port=25000),
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob@domain')])
        delta.remote_hostname = 'gargantua1'
        delta.fcrdns = True
        delta.body = InlineBlob(
            b'Received: from time-becomes-a-loop.example.com with ESMTP;\r\n'
            b'\tFri, 13 Feb 2009 23:31:29 +0000\r\n'
            b'Received: from somewhere-else.example.com with ESMTP;\r\n'
            b'\tFri, 13 Feb 2009 23:31:28 +0000\r\n'
            b'\r\n'
            b'hello\r\n',
            last=True)

        tx.merge_from(delta)
        result = filter.on_update(delta)
        self.assertEqual(result.downstream_delta.data_response.code, 550)
        self.assertTrue(result.downstream_delta.data_response.message.startswith('5.4.6'))

if __name__ == '__main__':
    unittest.util._MAX_LENGTH = 1024
    unittest.main()
