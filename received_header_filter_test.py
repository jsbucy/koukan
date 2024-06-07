import unittest
import logging
from datetime import datetime, timezone

from blob import InlineBlob
from filter import HostPort, Mailbox, Response, TransactionMetadata
from received_header_filter import ReceivedHeaderFilter
from fake_endpoints import FakeSyncFilter

class ReceivedHeaderFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

    def testBasic(self):
        upstream = FakeSyncFilter()

        tx = TransactionMetadata(
            remote_host=HostPort('1.2.3.4', port=25000),
            mail_from=Mailbox('alice'))
        tx.remote_hostname = 'gargantua1'
        tx.fcrdns = True

        tx.smtp_meta = {
            'ehlo_host': 'gargantua1',
            'esmtp': True,
            'tls': True
        }

        filter = ReceivedHeaderFilter(
            upstream = upstream,
            received_hostname = 'gargantua1',
            inject_time = datetime.fromtimestamp(1234567890, timezone.utc))

        def exp(tx, tx_delta):
            assert tx_delta.smtp_meta
            return TransactionMetadata()
        upstream.add_expectation(exp)
        filter.on_update(tx, tx_delta=tx)

        tx.rcpt_to.append(Mailbox('bob@domain'))
        body = (b'From: <alice>\r\n'
                b'To: <bob>\r\n'
                b'Received: from somewhere-else.example.com with ESMTP;\r\n'
                b'\tFri, 13 Feb 2009 23:31:29 +0000\r\n'
                b'\r\n'
                b'hello\r\n')
        tx.body_blob = InlineBlob(body[0:20], len(body))
        tx_delta = TransactionMetadata(
            rcpt_to = [tx.rcpt_to[0]],
            body_blob = tx.body_blob)
        def exp(tx, tx_delta):
            self.assertIsNotNone(tx.mail_from)
            self.assertEqual(len(tx.rcpt_to), 1)
            tx.mail_response = Response(201)
            tx.rcpt_response = [Response(202)]
            self.assertIsNone(tx.body_blob)
            return TransactionMetadata(
                mail_response=tx.mail_response,
                rcpt_response=tx.rcpt_response)
        upstream.add_expectation(exp)
        filter.on_update(tx, tx_delta)

        tx.body_blob = InlineBlob(body[0:30], len(body))
        tx_delta = TransactionMetadata(body_blob = tx.body_blob)
        # no expectation: should not be called: only delta is incomplete body
        filter.on_update(tx, tx_delta)

        tx.body_blob = tx_delta.body_blob = InlineBlob(body, len(body))
        def exp(tx, tx_delta):
            logging.debug(tx_delta.body_blob.read(0).decode('us-ascii'))
            self.assertEqual(
                tx_delta.body_blob.read(0),
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
            tx.data_response = Response(203)
            return TransactionMetadata(data_response=tx.data_response)
        upstream.add_expectation(exp)
        upstream_delta = filter.on_update(tx, tx_delta=tx_delta)
        self.assertFalse(upstream.expectation)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])
        self.assertEqual(tx.data_response.code, 203)

    def test_max_received_headers(self):
        upstream = FakeSyncFilter()

        filter = ReceivedHeaderFilter(
            upstream = upstream,
            received_hostname = 'gargantua1',
            inject_time = datetime.fromtimestamp(1234567890, timezone.utc),
            max_received_headers = 1)

        tx = TransactionMetadata(
            remote_host=HostPort('1.2.3.4', port=25000),
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob@domain')])
        tx.remote_hostname = 'gargantua1'
        tx.fcrdns = True
        tx.body_blob = InlineBlob(
            b'Received: from time-becomes-a-loop.example.com with ESMTP;\r\n'
            b'\tFri, 13 Feb 2009 23:31:29 +0000\r\n'
            b'Received: from somewhere-else.example.com with ESMTP;\r\n'
            b'\tFri, 13 Feb 2009 23:31:28 +0000\r\n'
            b'\r\n'
            b'hello\r\n')

        def exp(tx, tx_delta):
            self.assertIsNotNone(tx.mail_from)
            self.assertEqual(len(tx.rcpt_to), 1)
            self.assertIsNone(tx.body_blob)
            tx.mail_response = Response(201)
            tx.rcpt_response = [Response(202)]
            return TransactionMetadata(
                mail_response = Response(201),
                rcpt_response = [Response(202)] )
        upstream.add_expectation(exp)
        filter.on_update(tx, tx)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])
        self.assertEqual(tx.data_response.code, 550)
        self.assertTrue(tx.data_response.message.startswith('5.4.6'))

if __name__ == '__main__':
    unittest.main()
