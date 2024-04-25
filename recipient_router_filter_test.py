from typing import Optional, Tuple
import unittest
import logging

from blob import InlineBlob
from recipient_router_filter import RecipientRouterFilter, RoutingPolicy
from filter import HostPort, Mailbox, TransactionMetadata
from response import Response
from fake_endpoints import SyncEndpoint
from response import Response

class SuccessPolicy(RoutingPolicy):
    def endpoint_for_rcpt(self, rcpt) -> Tuple[
            Optional[str], Optional[HostPort], Optional[Response]]:
        return 'http://gateway', HostPort('example.com', 1234), None

class FailurePolicy(RoutingPolicy):
    def endpoint_for_rcpt(self, rcpt) -> Tuple[
            Optional[str], Optional[HostPort], Optional[Response]]:
        return None, None, Response(500, 'not found')


class RecipientRouterFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

    def test_success(self):
        next = SyncEndpoint()
        r = RecipientRouterFilter(SuccessPolicy(), next)

        next.set_mail_response(Response(201))
        next.add_rcpt_response(Response(202))
        next.add_data_response(Response(203))
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob@domain')])

        tx.body_blob = InlineBlob(
            b'From: <alice>\r\n'
            b'To: <bob>\r\n'
            b'\r\n'
            b'hello\r\n')

        r.on_update(tx)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])
        self.assertEqual(tx.data_response.code, 203)

        self.assertEqual(next.tx.rest_endpoint, 'http://gateway')
        self.assertEqual(next.tx.remote_host, HostPort('example.com', 1234))

    # TODO: exercise "buffer mail"

    def test_failure(self):
        next = SyncEndpoint()
        r = RecipientRouterFilter(FailurePolicy(), next)

        next.set_mail_response(Response(201))
        next.add_rcpt_response(Response(202))
        next.add_data_response(Response(203))
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob@domain')])

        tx.body_blob = InlineBlob(
            b'From: <alice>\r\n'
            b'To: <bob>\r\n'
            b'\r\n'
            b'hello\r\n')

        r.on_update(tx)
        self.assertEqual(tx.mail_response.code, 250)
        self.assertEqual([r.code for r in tx.rcpt_response], [500])
        self.assertIsNone(tx.data_response)

        # upstream never invoked
        self.assertIsNone(next.tx)


if __name__ == '__main__':
    unittest.main()
