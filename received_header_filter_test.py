import unittest
import logging
from datetime import datetime, timezone

from blob import InlineBlob
from filter import HostPort, Mailbox, Response, TransactionMetadata
from received_header_filter import ReceivedHeaderFilter
from fake_endpoints import SyncEndpoint

class ReceivedHeaderFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

    def testBasic(self):
        next = SyncEndpoint()
        next.set_mail_response(Response(201))
        next.add_rcpt_response(Response(202))
        next.add_data_response(Response(203))

        tx = TransactionMetadata(
            remote_host=HostPort('1.2.3.4', port=25000),
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob@domain')])
        tx.remote_hostname = 'gargantua1'
        tx.fcrdns = True

        tx.smtp_meta = {
            'ehlo_host': 'gargantua1',
            'esmtp': True,
            'tls': True
        }

        filter = ReceivedHeaderFilter(
            next = next,
            received_hostname = 'gargantua1',
            inject_time = datetime.fromtimestamp(1234567890, timezone.utc))
        filter.on_update(tx)

        tx = TransactionMetadata()
        tx.body_blob = InlineBlob(
            b'From: <alice>\r\n'
            b'To: <bob>\r\n'
            b'\r\n'
            b'hello\r\n')
        filter.on_update(tx)
        self.assertEqual(
            next.body_blob.read(0),
            b'Received: from gargantua1 (gargantua1 [1.2.3.4])\r\n'
            b'\tby gargantua1\r\n'
            b'\twith ESMTPS\r\n'
            b'\tfor bob@domain;\r\n'
            b'\tFri, 13 Feb 2009 23:31:30 +0000\r\n'
            b'From: <alice>\r\n'
            b'To: <bob>\r\n'
            b'\r\n'
            b'hello\r\n')


if __name__ == '__main__':
    unittest.main()
