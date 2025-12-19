import logging
import unittest

from koukan.filter import Mailbox, TransactionMetadata
from koukan.policy_filter import IngressPolicy
from koukan.blob import InlineBlob

from koukan.remote_host_filter import RemoteHostFilter, RemoteHostFilterResult
from koukan.received_header_filter import ReceivedHeaderFilter, ReceivedHeaderFilterResult

class PolicyFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d %(message)s')

    def test_remote_host_smoke(self):
        filter = IngressPolicy()
        filter.wire_downstream(TransactionMetadata())
        tx = filter.downstream_tx
        tx.filter_output = {}
        prev = tx.copy()
        tx.mail_from = Mailbox('alice')
        filter.on_update(prev.delta(tx))
        self.assertEqual(450, tx.mail_response.code)

    def test_remote_host_ok(self):
        filter = IngressPolicy()
        filter.wire_downstream(TransactionMetadata())
        tx = filter.downstream_tx
        tx.smtp_meta = {'ehlo_host': 'example.com'}
        rh = RemoteHostFilterResult(
            RemoteHostFilterResult.Status.OK, 'example.com', True)

        tx.add_filter_output(RemoteHostFilter.fullname(), rh)
        tx.add_filter_output(ReceivedHeaderFilter.fullname(), ReceivedHeaderFilterResult(1))

        prev = tx.copy()
        tx.mail_from = Mailbox('alice')
        filter.on_update(prev.delta(tx))
        self.assertIsNone(tx.mail_response)

    def test_hop_count_fail(self):
        filter = IngressPolicy()
        filter.wire_downstream(TransactionMetadata())
        tx = filter.downstream_tx
        tx.smtp_meta = {'ehlo_host': 'example.com'}
        rh = RemoteHostFilterResult(
            RemoteHostFilterResult.Status.OK, 'example.com', True)

        tx.add_filter_output(RemoteHostFilter.fullname(), rh)
        tx.add_filter_output(ReceivedHeaderFilter.fullname(),
                             ReceivedHeaderFilterResult(50))

        prev = tx.copy()
        tx.body = InlineBlob(b'hello', last=True)
        filter.on_update(prev.delta(tx))
        self.assertEqual(550, tx.data_response.code)

if __name__ == '__main__':
    unittest.main()
