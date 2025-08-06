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

class RecipientRouterFilterTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
            '%(message)s')

    async def test_success(self):
        router = RecipientRouterFilter(SuccessPolicy())
        router.wire_downstream(TransactionMetadata())
        tx = router.downstream

        async def upstream():
            self.assertEqual(tx.rest_endpoint, 'http://localhost:8001')
            self.assertEqual(tx.upstream_http_host, 'gateway')
            self.assertEqual(tx.resolution.hosts,
                             [HostPort('example.com', 1234)])

            upstream_delta = TransactionMetadata(
                mail_response = Response(201),
                rcpt_response = [Response(202)],
                data_response = Response(203))
            self.assertIsNotNone(tx.merge_from(upstream_delta))
            return upstream_delta

        delta = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob@domain')])

        delta.body = InlineBlob(
            b'From: <alice>\r\n'
            b'To: <bob>\r\n'
            b'\r\n'
            b'hello\r\n')
        tx.merge_from(delta)

        await router.on_update(delta, upstream)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])
        self.assertEqual(tx.data_response.code, 203)


    # TODO: exercise "buffer mail"

    async def test_failure(self):
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

        def unexpected_upstream():
            self.fail()

        tx.merge_from(delta)
        await router.on_update(delta, unexpected_upstream)
        self.assertEqual(tx.mail_response.code, 250)
        self.assertEqual([r.code for r in tx.rcpt_response], [500])
        self.assertIsNone(tx.data_response)

        # noop/heartbeat update: should return without calling
        # upstream or mutating transaction
        prev = tx.copy()
        await router.on_update(TransactionMetadata(), unexpected_upstream)
        self.assertFalse(prev.delta(tx))

if __name__ == '__main__':
    unittest.main()
