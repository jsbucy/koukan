import unittest
import logging

from koukan.filter import (
    HostPort,
    Mailbox,
    TransactionMetadata )
from koukan.blob import InlineBlob
from koukan.deadline import Deadline
from koukan.response import Response

from koukan.fake_endpoints import MockAsyncFilter
from koukan.async_filter_wrapper import AsyncFilterWrapper
from koukan.storage_schema import VersionConflictException


class AsyncFilterWrapperTest(unittest.TestCase):
    def test_basic(self):
        async_filter = MockAsyncFilter()

        wrapper = AsyncFilterWrapper(async_filter, 1)

        def exp(tx, tx_delta):
            return TransactionMetadata()
        async_filter.expect_update(exp)

        # TODO see comment in implementation, this is what you
        # actually get with StorageWriterFilter today
        async_filter.expect_get(TransactionMetadata(
            mail_from=Mailbox('alice')))
        async_filter.expect_get(TransactionMetadata(
            mail_from=Mailbox('alice')))
        async_filter.expect_get(TransactionMetadata(
            mail_from=Mailbox('alice'),
            mail_response=Response(250)))

        tx = TransactionMetadata(
            mail_from=Mailbox('alice'))
            #body_blob=InlineBlob(b'hello', last=True))
        upstream_delta = wrapper.update(tx, tx.copy())
        self.assertIsNotNone(upstream_delta)

    def test_update_conflict(self):
        async_filter = MockAsyncFilter()
        wrapper = AsyncFilterWrapper(async_filter, 1)

        def exp_conflict(tx, tx_delta):
            raise VersionConflictException()
        async_filter.expect_update(exp_conflict)

        def exp(tx, tx_delta):
            return TransactionMetadata(version=2)
        async_filter.expect_update(exp)
        async_filter.expect_get(TransactionMetadata())
        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        upstream_delta = wrapper.update(tx, tx.copy())
        self.assertIsNotNone(upstream_delta)
        # XXX mock doesn't implement
        #self.assertEqual(2, wrapper.version())

    def disabled_test_store_and_forward(self):
        async_filter = MockAsyncFilter()
        wrapper = AsyncFilterWrapper(async_filter, 1, store_and_forward=True)
        b = b'hello, world!'
        tx = TransactionMetadata(mail_from=Mailbox('alice'),
                                 rcpt_to=[Mailbox('bob')],
                                 body_blob=InlineBlob(b, len(b)))
        def exp_update(tx, delta):
            return TransactionMetadata()
        async_filter.expect_update(exp_update)
        upstream_tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            mail_response=Response(450),
            rcpt_to=[Mailbox('bob')],
            body_blob=InlineBlob(b, len(b)))
        async_filter.expect_get(upstream_tx)
        wrapper.update(tx, tx.copy())

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s:%(lineno)d '
                        '%(message)s')
    unittest.main()
