# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import List, Optional

import unittest
import logging
from functools import partial

from koukan.add_route_filter import AddRouteFilter
from koukan.fake_endpoints import FakeSyncFilter
from koukan.filter import (Mailbox, TransactionMetadata)
from koukan.response import Response
from koukan.blob import InlineBlob

class AddRouteFilterTest(unittest.TestCase):
    def test_smoke(self):
        upstream = FakeSyncFilter()
        add_route = FakeSyncFilter()

        b = 'hello, world!'
        def exp(i, tx, tx_delta):
            self.assertEqual(b, tx.body.pread(0))
            upstream_delta=TransactionMetadata(
                mail_response=Response(201 + i),
                rcpt_response=[Response(203 + i)],
                data_response=Response(205 + i))
            tx.merge_from(upstream_delta)
            return upstream_delta

        upstream.add_expectation(partial(exp, 0))
        add_route.add_expectation(partial(exp, 1))

        filter = AddRouteFilter(add_route, 'add-route', upstream)

        tx = TransactionMetadata(mail_from=Mailbox('alice'),
                                 rcpt_to=[Mailbox('bob')],
                                 body=InlineBlob(b, len(b)))
        filter.on_update(tx, tx.copy())
        self.assertEqual(201, tx.mail_response.code)
        self.assertEqual([203], [r.code for r in tx.rcpt_response])
        self.assertEqual(205, tx.data_response.code)

    def test_add_route_err(self):
        upstream = FakeSyncFilter()
        add_route = FakeSyncFilter()

        def exp(tx, tx_delta):
            upstream_delta=TransactionMetadata(
                mail_response=Response(201),
                rcpt_response=[Response(203)],
                data_response=Response(205))
            tx.merge_from(upstream_delta)
            return upstream_delta

        upstream.add_expectation(exp)

        def exp_err(tx, tx_delta):
            upstream_delta=TransactionMetadata(
                mail_response=Response(401),
                rcpt_response=[Response(403)],
                data_response=Response(405))
            tx.merge_from(upstream_delta)
            return upstream_delta

        add_route.add_expectation(exp_err)

        filter = AddRouteFilter(add_route, 'add-route', upstream)

        b = 'hello, world!'
        tx = TransactionMetadata(mail_from=Mailbox('alice'),
                                 rcpt_to=[Mailbox('bob')],
                                 body=InlineBlob(b, len(b)))
        filter.on_update(tx, tx.copy())
        logging.debug(tx)
        self.assertEqual(401, tx.mail_response.code)
        self.assertEqual([403], [r.code for r in tx.rcpt_response])
        self.assertEqual(405, tx.data_response.code)



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s [%(thread)d] '
                        '%(filename)s:%(lineno)d %(message)s')

    unittest.main()
