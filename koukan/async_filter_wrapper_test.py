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
        upstream_delta = wrapper.on_update(tx, tx.copy())
        self.assertIsNotNone(upstream_delta)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s:%(lineno)d '
                        '%(message)s')
    unittest.main()
