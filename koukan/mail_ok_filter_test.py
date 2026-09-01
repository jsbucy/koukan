# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional, Tuple
import unittest
import logging

from koukan.mail_ok_filter import MailOkFilter
from koukan.filter import Mailbox, TransactionMetadata
from koukan.response import Response


class MailOkFilterTest(unittest.IsolatedAsyncioTestCase):
    async def test_smoke(self):
        filter = MailOkFilter()
        tx = TransactionMetadata()
        filter.wire_downstream(tx)
        upstream_tx = TransactionMetadata()
        filter.wire_upstream(upstream_tx)

        async def upstream():
            nonlocal upstream_tx
            self.assertIsNone(upstream_tx.mail_from)
            return TransactionMetadata()

        prev = tx.copy()
        tx.mail_from = Mailbox('alice@example.com')
        await filter.on_update(prev.delta(tx), upstream)
        self.assertEqual(250, tx.mail_response.code)

        prev = tx.copy()
        tx.rcpt_to.append(Mailbox('bob@example.com'))

        async def upstream2():
            nonlocal upstream_tx
            logging.debug(upstream_tx)
            self.assertIsNotNone(upstream_tx.mail_from)
            prev = upstream_tx.copy()
            upstream_tx.mail_response = Response(550, 'bad')
            upstream_tx.rcpt_response = [Response(503, '5.1.1')]
            return prev.delta(upstream_tx)

        await filter.on_update(prev.delta(tx), upstream2)
        self.assertEqual(250, tx.mail_response.code)
        self.assertEqual([550], [r.code for r in tx.rcpt_response])

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
        '%(message)s')
    unittest.main()
