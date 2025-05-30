# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import List, Optional, Tuple
import unittest
import logging
import io
import json
import time
from threading import Condition, Lock

from werkzeug.datastructures import ContentRange
from werkzeug.http import parse_content_range_header

from fastapi import (
    Request as FastApiRequest,
    Response as FastApiResponse )

from httpx import Response as HttpxResponse

from koukan.blob import InlineBlob
from koukan.sync_filter_adapter import SyncFilterAdapter
from koukan.fake_endpoints import FakeSyncFilter, MockAsyncFilter
from koukan.executor import Executor
from koukan.filter import Mailbox, TransactionMetadata, WhichJson
from koukan.response import Response
from koukan.executor import Executor
from koukan.storage_schema import VersionConflictException

class SyncFilterAdapterTest(unittest.TestCase):
    def setUp(self):
        self.executor = Executor(inflight_limit=10, watchdog_timeout=5)

    def tearDown(self):
        self.executor.shutdown(timeout=5)

    def test_basic(self):
        upstream = FakeSyncFilter()
        sync_filter_adapter = SyncFilterAdapter(
            self.executor, upstream, 'rest_id')

        def exp_mail(tx, tx_delta):
            self.assertEqual(tx.mail_from.mailbox, 'alice')
            upstream_delta=TransactionMetadata(
                mail_response = Response(201))
            self.assertIsNotNone(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream.add_expectation(exp_mail)

        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        sync_filter_adapter.update(tx, tx.copy())
        sync_filter_adapter.wait(tx.version, 1)
        upstream_tx = sync_filter_adapter.get()
        self.assertEqual(upstream_tx.mail_response.code, 201)

        def exp_rcpt(tx, tx_delta):
            self.assertEqual(tx.mail_from.mailbox, 'alice')
            self.assertEqual([r.mailbox for r in tx.rcpt_to], ['bob'])
            upstream_delta=TransactionMetadata(
                rcpt_response = [Response(202)])
            self.assertIsNotNone(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream.add_expectation(exp_rcpt)

        delta = TransactionMetadata(rcpt_to=[Mailbox('bob')])
        tx.merge_from(delta)
        tx.version = sync_filter_adapter.version()
        sync_filter_adapter.update(tx, delta)
        for i in range(0,3):
            sync_filter_adapter.wait(tx.version, 1)
            tx = sync_filter_adapter.get()
            if [r.code for r in tx.rcpt_response] == [202]:
                break
            self.assertFalse(sync_filter_adapter.done)
        else:
            self.fail('expected rcpt_response')

        body = b'hello, world!'

        def exp_body(tx, tx_delta):
            logging.debug(tx.body)
            if not tx.body.finalized():
                return
            self.assertEqual(body, tx.body.pread(0))
            upstream_delta=TransactionMetadata(
                data_response = Response(203))
            self.assertIsNotNone(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream.add_expectation(exp_body)

        blob_writer = sync_filter_adapter.get_blob_writer(
            create=True, blob_rest_id=None, tx_body=True)
        b=b'hello, '
        b2=b'world!'
        blob_writer.append_data(0, b, None)
        blob_writer = sync_filter_adapter.get_blob_writer(
            create=False, blob_rest_id=None, tx_body=True)
        blob_writer.append_data(len(b), b2, len(b) + len(b2))

        for i in range(0,3):
            tx = sync_filter_adapter.get()
            logging.debug(tx)
            if tx is not None and tx.data_response is not None and tx.data_response.code == 203:
                break
            sync_filter_adapter.wait(tx.version, 1)
        else:
            self.fail('expected data response')

        for i in range(0,3):
            sync_filter_adapter.wait(tx.version, 1)
            if sync_filter_adapter.done:
                break
            tx = sync_filter_adapter.get()
        else:
            self.fail('expected done')
        self.assertTrue(sync_filter_adapter.idle(time.time(), 0, 0))

    def test_upstream_filter_exceptions(self):
        upstream = FakeSyncFilter()
        sync_filter_adapter = SyncFilterAdapter(
            self.executor, upstream, 'rest_id')

        def exp(tx,delta):
            raise ValueError()
        upstream.add_expectation(exp)

        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        sync_filter_adapter.update(tx, tx.copy())
        sync_filter_adapter.wait(tx.version, 1)
        upstream_tx = sync_filter_adapter.get()
        self.assertEqual(450, upstream_tx.mail_response.code)
        self.assertIn('unexpected exception', upstream_tx.mail_response.message)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d %(message)s')
    unittest.main()
