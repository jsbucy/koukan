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

class Policy(RoutingPolicy):
    def endpoint_for_rcpt(self, rcpt) -> Tuple[
            Optional[Destination], Optional[Response]]:
        if rcpt == 'good':
            return Destination(
                'http://localhost:8001', 'gateway',
                [HostPort('example.com', 1234)]), None
        if rcpt == 'bad':
            return None, Response(500, 'not found')

class RecipientRouterFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
            '%(message)s')

    def test_success(self):
        router = RecipientRouterFilter(Policy())
        router.wire_downstream(TransactionMetadata())
        router.wire_upstream(TransactionMetadata())
        tx = router.downstream_tx

        prev = tx.copy()
        tx.mail_from=Mailbox('alice')
        tx.rcpt_to=[Mailbox('good')]
        tx.body = InlineBlob(
            b'From: <alice>\r\n'
            b'To: <bob>\r\n'
            b'\r\n'
            b'hello\r\n')
        delta = prev.delta(tx)

        router.on_update(delta)
        logging.debug(router.downstream_tx)
        logging.debug(router.upstream_tx)
        # self.assertEqual(['good'], [m.mailbox for m in router.upstream_tx.rcpt_to])
        self.assertEqual(router.upstream_tx.rest_endpoint,
                         'http://localhost:8001')
        self.assertEqual(router.upstream_tx.upstream_http_host, 'gateway')
        self.assertEqual(router.upstream_tx.resolution.hosts,
                         [HostPort('example.com', 1234)])


    def test_failure(self):
        router = RecipientRouterFilter(Policy())
        tx = TransactionMetadata()
        router.wire_downstream(tx)
        router.wire_upstream(TransactionMetadata())

        prev = tx.copy()
        tx.mail_from=Mailbox('alice')
        tx.rcpt_to=[Mailbox('bad')]
        tx.body = InlineBlob(
            b'From: <alice>\r\n'
            b'To: <bob>\r\n'
            b'\r\n'
            b'hello\r\n')
        delta = prev.delta(tx)

        router.on_update(delta)
        # self.assertEqual(201, tx.mail_response.code)
        self.assertEqual([500], [r.code for r in tx.rcpt_response])


    def test_mixed(self):
        router = RecipientRouterFilter(Policy())
        tx = TransactionMetadata()
        router.wire_downstream(tx)
        router.wire_upstream(TransactionMetadata())

        prev = tx.copy()
        tx.mail_from=Mailbox('alice')
        tx.rcpt_to=[Mailbox('bad'),
                    Mailbox('good')]
        tx.body = InlineBlob(
            b'From: <alice>\r\n'
            b'To: <bob>\r\n'
            b'\r\n'
            b'hello\r\n')
        delta = prev.delta(tx)

        router.on_update(delta)
        logging.debug(tx)
        # self.assertEqual(201, tx.mail_response.code)
        self.assertEqual(500, tx.rcpt_response[0].code)
        # self.assertIsNone(tx.rcpt_response[1])



if __name__ == '__main__':
    unittest.main()
