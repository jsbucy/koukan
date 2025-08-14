# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import List, Optional

import unittest
import logging
from functools import partial

from koukan.add_route_filter import AddRouteFilter
from koukan.fake_endpoints import FakeFilter
from koukan.filter import Mailbox, TransactionMetadata
from koukan.filter_chain import FilterChain
from koukan.response import Response
from koukan.blob import InlineBlob

class AddRouteFilterTest(unittest.TestCase):
    def test_smoke(self):
        add_route = FakeFilter()
        chain = FilterChain([add_route])

        b = 'hello, world!'
        def exp(tx, tx_delta):
            self.assertEqual(b, tx.body.pread(0))
            upstream_delta=TransactionMetadata(
                mail_response=Response(202),
                rcpt_response=[Response(204)],
                data_response=Response(206))
            tx.merge_from(upstream_delta)
            return upstream_delta
        add_route.add_expectation(exp)

        filter = AddRouteFilter(chain, 'add-route')
        filter.wire_downstream(TransactionMetadata())

        delta = TransactionMetadata(mail_from=Mailbox('alice'),
                                    rcpt_to=[Mailbox('bob')],
                                    body=InlineBlob(b, len(b)))
        tx = filter.downstream_tx
        tx.merge_from(delta)
        filter_result = filter.on_update(delta)
        self.assertIsNone(filter_result.downstream_delta)

    def test_add_route_err(self):
        add_route = FakeFilter()
        chain = FilterChain([add_route])
        filter = AddRouteFilter(chain, 'add-route')
        tx = TransactionMetadata()
        filter.wire_downstream(tx)

        def exp_add_route_err(tx, tx_delta):
            upstream_delta=TransactionMetadata(
                mail_response=Response(401),
                rcpt_response=[Response(403)],
                data_response=Response(405))
            tx.merge_from(upstream_delta)
            return upstream_delta

        add_route.add_expectation(exp_add_route_err)

        b = 'hello, world!'
        delta = TransactionMetadata(mail_from=Mailbox('alice'),
                                    rcpt_to=[Mailbox('bob')],
                                    body=InlineBlob(b, len(b)))
        tx.merge_from(delta)
        filter_result = filter.on_update(delta)
        logging.debug(tx)

        self.assertEqual(401, tx.mail_response.code)
        self.assertEqual([403], [r.code for r in tx.rcpt_response])
        self.assertEqual(405, tx.data_response.code)



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s [%(thread)d] '
                        '%(filename)s:%(lineno)d %(message)s')

    unittest.main()
