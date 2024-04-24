import unittest
import logging
from datetime import datetime, timezone

from blob import InlineBlob
from recipient_router_filter import RecipientRouterFilter, RoutingPolicy
from dest_domain_policy import DestDomainPolicy
from filter import HostPort, Mailbox, TransactionMetadata
from fake_endpoints import SyncEndpoint
from response import Response

class RecipientRouterFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

    def test_basic(self):
        next = SyncEndpoint()
        policy = DestDomainPolicy()
        r = RecipientRouterFilter(
            policy, next,
            received_hostname = 'gargantua1',
            inject_time = datetime.fromtimestamp(1234567890, timezone.utc))

        next.set_mail_response(Response(201))
        next.add_rcpt_response(Response(202))
        next.add_data_response(Response(203))
        tx = TransactionMetadata(
            remote_host=HostPort('1.2.3.4', port=25000),
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob@domain')])
        tx.body_blob = InlineBlob(
            b'From: <alice>\r\n'
            b'To: <bob>\r\n'
            b'\r\n'
            b'hello\r\n')
        tx.smtp_meta = {
            'ehlo_host': 'gargantua1',
            'esmtp': True,
            'tls': True
        }

        r.on_update(tx)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])
        self.assertEqual(tx.data_response.code, 203)
        self.assertEqual(
            next.body_blob.read(0),
            b'Received: from gargantua1 ([1.2.3.4])\r\n'
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
