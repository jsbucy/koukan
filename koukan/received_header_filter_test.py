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
from koukan.received_header_filter import ReceivedHeaderFilter

class ReceivedHeaderFilterTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
            '%(message)s')

    async def test_smoke(self):
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

        async def upstream():
            assert filter.upstream.smtp_meta
            return TransactionMetadata()
        tx.merge_from(delta)
        await filter.on_update(delta, upstream)

        delta.rcpt_to.append(Mailbox('bob@domain'))
        body = (b'From: <alice>\r\n'
                b'To: <bob>\r\n'
                b'Received: from somewhere-else.example.com with ESMTP;\r\n'
                b'\tFri, 13 Feb 2009 23:31:29 +0000\r\n'
                b'\r\n'
                b'hello\r\n')
        delta.body = InlineBlob(body[0:20], len(body))
        tx.merge_from(delta)
        async def upstream():
            tx = filter.upstream
            self.assertIsNotNone(tx.mail_from)
            self.assertEqual(len(tx.rcpt_to), 1)
            delta = TransactionMetadata(
                mail_response=Response(201),
                rcpt_response=[Response(202)])
            self.assertIsNotNone(tx.merge_from(delta))
            self.assertIsNone(tx.body)
            return delta
        await filter.on_update(delta, upstream)

        tx.body = InlineBlob(body[0:30], len(body))
        tx_delta = TransactionMetadata(body = tx.body)

        async def upstream():
            self.assertIsNone(filter.upstream.body)
            return TransactionMetadata()
        await filter.on_update(tx_delta, upstream)


        tx.body = tx_delta.body = InlineBlob(body, len(body))
        async def exp():
            logging.debug(filter.upstream)  #.body.pread(0).decode('us-ascii'))
            self.assertEqual(
                filter.upstream.body.pread(0),
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
            delta = TransactionMetadata(
                mail_response = Response(201),
                rcpt_response = [],
                data_response = Response(203))
            filter.upstream.merge_from(delta)
            return delta
        await filter.on_update(tx_delta, exp)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])
        self.assertEqual(tx.data_response.code, 203)

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

    async def test_max_received_headers(self):
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

        async def exp():
            tx = filter.upstream
            self.assertIsNotNone(tx.mail_from)
            self.assertEqual(len(tx.rcpt_to), 1)
            self.assertIsNone(tx.body)
            delta = TransactionMetadata(
                mail_response = Response(201),
                rcpt_response = [Response(202)])
            tx.merge_from(delta)
            return delta
        tx.merge_from(delta)
        await filter.on_update(delta, exp)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])
        self.assertEqual(tx.data_response.code, 550)
        self.assertTrue(tx.data_response.message.startswith('5.4.6'))

if __name__ == '__main__':
    unittest.util._MAX_LENGTH = 1024
    unittest.main()
