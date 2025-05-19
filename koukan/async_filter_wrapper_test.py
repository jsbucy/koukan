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
            delta = TransactionMetadata(version=2)
            tx.merge_from(delta)
            return delta
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
        async_filter.expect_get(
            TransactionMetadata(mail_from=Mailbox('alice'), version=2))

        upstream_tx = wrapper.get()
        self.assertEqual(2, upstream_tx.version)
        self.assertEqual(450, upstream_tx.mail_response.code)
        self.assertEqual('upstream timeout (AsyncFilterWrapper)',
                         upstream_tx.mail_response.message)

    # the following scenario is unlikely but not impossible:
    # msa with relatively short exploder rcpt timeout (say 5-10s)
    # upstream takes 30s to return 500 rcpt_resp
    # exploder times out and returns 250 s&f rcpt resp downstream
    # downstream may finish sending the data before or after
    # upstream/OH rcpt resp
    # if before: accept&bounce (expected/unavoidable)
    # if after: will get rcpt error as data precondition failure
    def test_upstream_resp_after_timeout(self):
        async_filter = MockAsyncFilter()
        wrapper = AsyncFilterWrapper(async_filter, 1, store_and_forward=True)

        def exp_update(tx, tx_delta):
            upstream_delta = TransactionMetadata(version=1)
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        async_filter.expect_update(exp_update)

        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        upstream_delta = wrapper.update(tx, tx.copy())
        self.assertEqual(1, tx.version)

        self.assertFalse(wrapper.wait(1, 1))
        async_filter.expect_get(
            TransactionMetadata(mail_from=Mailbox('alice'), version=2))

        tx = wrapper.get()
        self.assertEqual(250, tx.mail_response.code)
        self.assertEqual('MAIL ok (AsyncFilterWrapper store&forward)',
                         tx.mail_response.message)


        def exp_rcpt(tx, tx_delta):
            upstream_delta = TransactionMetadata(version=2)
            assert tx.merge_from(upstream_delta) is not None
            return upstream_delta
        async_filter.expect_update(exp_rcpt)

        tx_delta = TransactionMetadata(rcpt_to=[Mailbox('bob')])
        tx.merge_from(tx_delta)
        logging.debug('set rcpt')
        wrapper.update(tx, tx_delta)

        #self.assertFalse(wrapper.wait(1, 1))
        upstream_tx = tx.copy()
        upstream_tx.mail_response = Response(550)
        upstream_tx.version=3
        async_filter.expect_get(upstream_tx)


        tx = wrapper.get()
        # no change
        self.assertEqual(250, tx.mail_response.code)
        self.assertEqual('MAIL ok (AsyncFilterWrapper store&forward)',
                         tx.mail_response.message)
        self.assertEqual([550], [r.code for r in tx.rcpt_response])
        self.assertEqual(['RCPT failed precondition MAIL (AsyncFilterWrapper)'],
                         [r.message for r in tx.rcpt_response])



    def test_store_and_forward(self):
        async_filter = MockAsyncFilter()
        wrapper = AsyncFilterWrapper(async_filter, 1, store_and_forward=True)
        b = b'hello, world!'
        tx = TransactionMetadata(mail_from=Mailbox('alice'),
                                 rcpt_to=[Mailbox('bob')],
                                 body=InlineBlob(b, len(b)))
        def exp_update(tx, delta):
            return TransactionMetadata(version=1)
        async_filter.expect_update(exp_update)
        upstream_tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            mail_response=Response(450),
            rcpt_to=[Mailbox('bob')],
            body=InlineBlob(b, len(b)),
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
