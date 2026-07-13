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
from koukan.sender import Sender

class Policy(RoutingPolicy):
    def endpoint_for_rcpt(
            self, rcpt
    ) -> Tuple[Optional[Destination], Optional[Response]]:
        if rcpt == 'good':
            return Destination(
                'http://localhost:8001', Sender('router', 'gateway'),
                [HostPort('example.com', 1234)]), None

        return None, Response(500, 'not found')

class RecipientRouterFilterTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
            '%(message)s')

    async def test_success(self):
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
        tx.sender = Sender('ingress', 'smtp-mx')
        delta = prev.delta(tx)
        logging.debug(delta)
        async def upstream():
            return TransactionMetadata()
        await router.on_update(delta, upstream)
        logging.debug(router.downstream_tx)
        logging.debug(router.upstream_tx)
        self.assertEqual(
            ['good'], [m.mailbox for m in router.upstream_tx.rcpt_to])
        self.assertEqual(router.upstream_tx.rest_endpoint,
                         'http://localhost:8001')
        self.assertEqual(router.downstream_tx.sender, router.upstream_tx.sender)
        self.assertEqual(router.upstream_tx.rest_upstream_sender.tag, 'gateway')
        self.assertEqual(router.upstream_tx.resolution.hosts,
                         [HostPort('example.com', 1234)])


    async def test_failure(self):
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
        async def upstream():
            return TransactionMetadata()
        await router.on_update(delta, upstream)
        self.assertFalse(router.upstream_tx.rcpt_to)
        self.assertEqual([500], [r.code for r in tx.rcpt_response])

    async def test_buffer_mail_err(self):
        router = RecipientRouterFilter(Policy())
        router.wire_downstream(TransactionMetadata())
        router.wire_upstream(TransactionMetadata())
        dtx = router.downstream_tx
        utx = router.upstream_tx

        async def upstream():
            nonlocal dtx, utx
            logging.debug(utx)
            prev = utx.copy()
            if utx.mail_from:
                utx.mail_response = Response(501, 'bad')
            # for ProxyFilter, FilterChain copies rcpts from dtx to
            # utx that haven't already failed
            if dtx.rcpt_to:
                utx.rcpt_response = [Response(502, 'failed precondition')]
            return prev.delta(utx)

        prev = dtx.copy()
        dtx.mail_from=Mailbox('alice')
        # dtx.rcpt_to=[Mailbox('good')]
        # dtx.body = InlineBlob(b'hello, world!')
        dtx.sender = Sender('ingress', 'smtp-mx')
        delta = prev.delta(dtx)
        logging.debug(delta)
        await router.on_update(delta, upstream)
        logging.debug(dtx)
        logging.debug(utx)
        self.assertEqual(250, dtx.mail_response.code)
        self.assertIsNone(utx.mail_from)
        self.assertIsNone(utx.rest_endpoint)
        self.assertEqual(dtx.sender, utx.sender)
        self.assertIsNone(utx.rest_upstream_sender)
        self.assertIsNone(utx.resolution)

        prev = dtx.copy()
        dtx.rcpt_to=[Mailbox('good')]

        await router.on_update(prev.delta(dtx), upstream)
        logging.debug(dtx)
        self.assertEqual([501], [r.code for r in dtx.rcpt_response])

    async def test_mixed(self):
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
        async def upstream():
            return TransactionMetadata()
        await router.on_update(delta, upstream)
        logging.debug(tx)
        self.assertEqual(
            ['good'], [m.mailbox for m in router.upstream_tx.rcpt_to])
        self.assertEqual(500, tx.rcpt_response[0].code)
        self.assertTrue(len(tx.rcpt_response) < 2 or
                        tx.rcpt_response[1] is None)



if __name__ == '__main__':
    unittest.main()
