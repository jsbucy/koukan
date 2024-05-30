import unittest
import logging

from filter_adapters import DeltaToFullAdapter, FullToDeltaAdapter
from fake_endpoints import (
    FakeSyncFilter,
    SyncEndpoint as FakeEndpoint)
from filter import Mailbox, TransactionMetadata
from response import Response

class ReceivedHeaderFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

    def test_delta_to_full(self):
        upstream = FakeSyncFilter()
        adapter = DeltaToFullAdapter(upstream)

        def exp(tx, tx_delta):
            self.assertEqual(tx.mail_from.mailbox, "alice")
            tx.mail_response=Response(201)
            return TransactionMetadata(mail_response=tx.mail_response)
        upstream.add_expectation(exp)
        delta = TransactionMetadata(mail_from=Mailbox('alice'))
        adapter.on_update(delta)
        self.assertEqual(delta.mail_response.code, 201)

        def exp(tx, tx_delta):
            self.assertEqual(tx.mail_from.mailbox, "alice")
            self.assertEqual([r.mailbox for r in tx.rcpt_to], ["bob1"])
            tx.rcpt_response.append(Response(202))
            return TransactionMetadata(rcpt_response=[tx.rcpt_response[0]])
        upstream.add_expectation(exp)
        delta = TransactionMetadata(rcpt_to=[Mailbox('bob1')])
        adapter.on_update(delta)
        self.assertIsNone(delta.mail_response)
        self.assertEqual([r.code for r in delta.rcpt_response], [202])

        def exp(tx, tx_delta):
            self.assertEqual(tx.mail_from.mailbox, "alice")
            self.assertEqual([r.mailbox for r in tx.rcpt_to], ["bob1", "bob2"])
            tx.rcpt_response.append(Response(203))
            return TransactionMetadata(rcpt_response=[tx.rcpt_response[1]])
        upstream.add_expectation(exp)
        delta = TransactionMetadata(rcpt_to=[Mailbox('bob2')])
        adapter.on_update(delta)
        self.assertIsNone(delta.mail_response)
        self.assertEqual([r.code for r in delta.rcpt_response], [203])

    def test_full_to_delta(self):
        upstream = FakeEndpoint()
        adapter = FullToDeltaAdapter(upstream)

        upstream.set_mail_response(Response(201))

        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        upstream_delta = adapter.on_update(tx, tx)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual(upstream_delta.mail_response.code, 201)

        upstream.add_rcpt_response(Response(202))
        tx.rcpt_to.append(Mailbox('bob1'))
        upstream_delta = adapter.on_update(
            tx, TransactionMetadata(rcpt_to=[tx.rcpt_to[0]]))
        self.assertEqual(tx.mail_response.code, 201)
        self.assertIsNone(upstream_delta.mail_response)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])
        self.assertEqual([r.code for r in upstream_delta.rcpt_response], [202])

        upstream.add_rcpt_response(Response(203))
        tx.rcpt_to.append(Mailbox('bob2'))
        upstream_delta = adapter.on_update(
            tx, TransactionMetadata(rcpt_to=[tx.rcpt_to[1]]))
        self.assertEqual(tx.mail_response.code, 201)
        self.assertIsNone(upstream_delta.mail_response)
        self.assertEqual([r.code for r in tx.rcpt_response], [202, 203])
        self.assertEqual([r.code for r in upstream_delta.rcpt_response], [203])


if __name__ == '__main__':
    unittest.main()
