# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional, Tuple
import unittest
import logging

from koukan.blob import InlineBlob
from koukan.recipient_router_filter import (
    Destination,
    RecipientRouterFilter,
    RoutingPolicy )
from koukan.filter import HostPort, Mailbox, TransactionMetadata
from koukan.response import Response

class SuccessPolicy(RoutingPolicy):
    def endpoint_for_rcpt(self, rcpt) -> Tuple[
            Optional[Destination], Optional[Response]]:
        return Destination(
            'http://localhost:8001', 'gateway',
            [HostPort('example.com', 1234)]), None

class FailurePolicy(RoutingPolicy):
    def endpoint_for_rcpt(self, rcpt) -> Tuple[
            Optional[Destination], Optional[Response]]:
        return None, Response(500, 'not found')

class RecipientRouterFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
            '%(message)s')

    def test_success(self):
        router = RecipientRouterFilter(SuccessPolicy())
        router.wire_downstream(TransactionMetadata())
        tx = router.downstream

        delta = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob@domain')])

        delta.body = InlineBlob(
            b'From: <alice>\r\n'
            b'To: <bob>\r\n'
            b'\r\n'
            b'hello\r\n')
        tx.merge_from(delta)

        router.on_update(delta)
        self.assertEqual(router.upstream.rest_endpoint, 'http://localhost:8001')
        self.assertEqual(router.upstream.upstream_http_host, 'gateway')
        self.assertEqual(router.upstream.resolution.hosts,
                         [HostPort('example.com', 1234)])


    # TODO: exercise "buffer mail"

    def test_failure(self):
        router = RecipientRouterFilter(FailurePolicy())
        tx = TransactionMetadata()
        router.wire_downstream(tx)

        delta = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob@domain')],
            body = InlineBlob(
                b'From: <alice>\r\n'
                b'To: <bob>\r\n'
                b'\r\n'
                b'hello\r\n'))

        tx.merge_from(delta)
        router.on_update(delta)
        self.assertEqual([r.code for r in tx.rcpt_response], [500])


if __name__ == '__main__':
    unittest.main()
