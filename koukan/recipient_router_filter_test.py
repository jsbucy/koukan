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
from koukan.fake_endpoints import FakeSyncFilter
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
        upstream = FakeSyncFilter()
        router = RecipientRouterFilter(SuccessPolicy(), upstream)

        def exp(tx, delta):
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

        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob@domain')])

        tx.body = InlineBlob(
            b'From: <alice>\r\n'
            b'To: <bob>\r\n'
            b'\r\n'
            b'hello\r\n')

        upstream.add_expectation(exp)
        upstream_delta = router.on_update(tx, tx.copy())
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])
        self.assertEqual(tx.data_response.code, 203)


    # TODO: exercise "buffer mail"

    def test_failure(self):
        upstream = FakeSyncFilter()
        router = RecipientRouterFilter(FailurePolicy(), upstream)

        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob@domain')])

        tx.body = InlineBlob(
            b'From: <alice>\r\n'
            b'To: <bob>\r\n'
            b'\r\n'
            b'hello\r\n')

        # no expectation on upstream: should not be called
        upstream_delta = router.on_update(tx, tx.copy())
        self.assertEqual(tx.mail_response.code, 250)
        self.assertEqual([r.code for r in tx.rcpt_response], [500])
        self.assertIsNone(tx.data_response)

        # noop/heartbeat update: should return without calling
        # upstream or mutating transaction
        prev = tx.copy()
        upstream_delta = router.on_update(tx, TransactionMetadata())
        self.assertFalse(upstream_delta)
        self.assertFalse(prev.delta(tx))

if __name__ == '__main__':
    unittest.main()
