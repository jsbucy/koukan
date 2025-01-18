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
    def test_async_smoke(self):
        async_filter = MockAsyncFilter()

        wrapper = AsyncFilterWrapper(async_filter, 1)

        def exp(tx, tx_delta):
            tx.version=1
            return TransactionMetadata(version=1)
        async_filter.expect_update(exp)

        async_filter.expect_get(TransactionMetadata(
            mail_from=Mailbox('alice'),
            version=1))
        async_filter.expect_get(TransactionMetadata(
            mail_from=Mailbox('alice'),
            version=1))
        async_filter.expect_get(TransactionMetadata(
            mail_from=Mailbox('alice'),
            mail_response=Response(250),
            version=2))
        # hack: mock wait succeeds if outstanding expectation
        async_filter.expect_get(TransactionMetadata(
            mail_from=Mailbox('alice'),
            mail_response=Response(250),
            version=2))

        tx = TransactionMetadata(mail_from=Mailbox('alice'))

        upstream_delta = wrapper.update(tx, tx.copy())
        self.assertEqual(1, wrapper.version())

        for i in range(0,2):
            self.assertTrue(wrapper.wait(1, 1))
            self.assertEqual(1, wrapper.version())
            upstream_tx = wrapper.get()
            self.assertIsNone(upstream_tx.mail_response)

        self.assertTrue(wrapper.wait(1, 1))
        upstream_tx = wrapper.get()
        # xxx fidelity problem with mock, version updated by get, not wait
        self.assertEqual(2, wrapper.version())
        self.assertEqual(250, upstream_tx.mail_response.code)


    def test_update_conflict(self):
        async_filter = MockAsyncFilter()
        wrapper = AsyncFilterWrapper(async_filter, 1)

        def exp_conflict(tx, tx_delta):
            raise VersionConflictException()
        async_filter.expect_update(exp_conflict)

        def exp(tx, tx_delta):
            return TransactionMetadata(version=2)
        async_filter.expect_update(exp)
        async_filter.expect_get(TransactionMetadata(version=2))
        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        upstream_delta = wrapper.update(tx, tx.copy())
        self.assertIsNotNone(upstream_delta)
        self.assertEqual(2, tx.version)

    def test_upstream_timeout(self):
        async_filter = MockAsyncFilter()
        wrapper = AsyncFilterWrapper(async_filter, 1)

        def exp_update(tx, tx_delta):
            upstream_delta = TransactionMetadata(version=1)
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        async_filter.expect_update(exp_update)

        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        upstream_delta = wrapper.update(tx, tx.copy())
        self.assertEqual(1, tx.version)

        self.assertFalse(wrapper.wait(1, 1))
        upstream_tx = wrapper.get()
        self.assertEqual(1, upstream_tx.version)
        self.assertEqual(450, upstream_tx.mail_response.code)
        self.assertEqual('upstream timeout (AsyncFilterWrapper)',
                         upstream_tx.mail_response.message)


    def test_store_and_forward(self):
        async_filter = MockAsyncFilter()
        wrapper = AsyncFilterWrapper(async_filter, 1, store_and_forward=True)
        b = b'hello, world!'
        tx = TransactionMetadata(mail_from=Mailbox('alice'),
                                 rcpt_to=[Mailbox('bob')],
                                 body_blob=InlineBlob(b, len(b)))
        def exp_update(tx, delta):
            return TransactionMetadata(version=1)
        async_filter.expect_update(exp_update)
        upstream_tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            mail_response=Response(450),
            rcpt_to=[Mailbox('bob')],
            body_blob=InlineBlob(b, len(b)),
            version=2)
        async_filter.expect_get(upstream_tx)
        def exp_sf(tx, delta):
            self.assertIsNotNone(delta.retry)
            upstream_delta=TransactionMetadata(version=3)
            tx.merge_from(upstream_delta)
            return upstream_delta
        async_filter.expect_update(exp_sf)
        wrapper.update(tx, tx.copy())
        logging.debug(tx)
        self.assertTrue(wrapper.wait(wrapper.version(), 1))
        tx = wrapper.get()
        logging.debug(tx)

    def test_early_data_err(self):
        async_filter = MockAsyncFilter()
        wrapper = AsyncFilterWrapper(async_filter, 1, store_and_forward=True)

        def exp_update(tx, tx_delta):
            upstream_delta = TransactionMetadata(
                mail_response=Response(),
                rcpt_response=[Response()],
                data_response=Response(450),
                version=1)

            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        async_filter.expect_update(exp_update)

        b = b'hello, world!'
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')],
            body_blob=InlineBlob(b[0:7], len(b)))

        upstream_delta = wrapper.update(tx, tx.copy())
        self.assertEqual(1, tx.version)

        def exp_data_last(tx, tx_delta):
            self.assertTrue(tx.body_blob.finalized())
            return TransactionMetadata()
        async_filter.expect_update(exp_data_last)

        def exp_retry(tx, tx_delta):
            self.assertIsNotNone(tx.retry)
            return TransactionMetadata()
        async_filter.expect_update(exp_retry)

        tx.body_blob = InlineBlob(b, len(b))
        upstream_delta = wrapper.update(
            tx, TransactionMetadata(body_blob=tx.body_blob))

    def test_sync_smoke(self):
        async_filter = MockAsyncFilter()

        wrapper = AsyncFilterWrapper(async_filter, 1)

        def exp(tx, tx_delta):
            tx.version=1
            return TransactionMetadata(version=1)
        async_filter.expect_update(exp)

        # TODO see comment in implementation, this is what you
        # actually get with StorageWriterFilter today
        async_filter.expect_get(TransactionMetadata(
            mail_from=Mailbox('alice'),
            version=1))
        async_filter.expect_get(TransactionMetadata(
            mail_from=Mailbox('alice'),
            version=1))
        async_filter.expect_get(TransactionMetadata(
            mail_from=Mailbox('alice'),
            mail_response=Response(250),
            version=2))
        # hack: mock wait succeeds if outstanding expectation
        async_filter.expect_get(TransactionMetadata(
            mail_from=Mailbox('alice'),
            mail_response=Response(250),
            version=2))

        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        upstream_delta = wrapper.on_update(tx, tx.copy())
        self.assertEqual(250, tx.mail_response.code)
        self.assertEqual(2, wrapper.version())


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s:%(lineno)d '
                        '%(message)s')
    unittest.main()
