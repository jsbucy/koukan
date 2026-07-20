# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Dict, List, Optional

import unittest
import logging
from threading import Thread
import time
from enum import IntEnum
from functools import partial

from koukan.storage import Storage, TransactionCursor
from koukan.storage_schema import BlobSpec, VersionConflictException
from koukan.response import Response
from koukan.filter import Mailbox, TransactionMetadata
from koukan.rest_schema import BlobUri

from koukan.blob import Blob, InlineBlob

from koukan.storage_writer_filter import StorageWriterFilter

from koukan.sqlite_test_utils import create_temp_sqlite_for_test

from koukan.message_builder import MessageBuilderSpec
from koukan.sender import Sender
from koukan.deadline import Deadline
from koukan.executor import Executor

endpoint_yaml_downstream_timeouts : Dict[str, Any] = {
    'downstream': {
        'mail_timeout': 1,
        'rcpt_timeout': 1,
        'data_timeout': 1,
    }
}

class Stage(IntEnum):
    MAIL = 0
    RCPT = 1
    DATA = 2

class Result(IntEnum):
    TEMP = 0
    PERM = 1
    TIMEOUT = 2
    SUCCESS = 3  # only for DATA

class Recipient:
    stage : Stage
    upstream_result : Result
    expect_sf : bool
    def __init__(self, s, ru, expect_sf=False):
        self.stage = s
        self.upstream_result = ru
        self.expect_sf = expect_sf

class Test:
    rcpt : List[Recipient]
    stage : Stage
    result : Result  # expected downstream
    sf_mode : str  # unavail | mixed

    def __init__(self, rcpt, stage, result, sf_mode):
        self.rcpt = rcpt
        self.stage = stage
        self.result = result
        self.sf_mode = sf_mode

    # stage is max across rcpts
    # - if sf_unavail and temp/timeout -> 250 s&f
    # - if timeout -> 450 upstream temp
    # - if all same major -> return that
    # - else mixed: return 250 s&f

class StorageWriterFilterTest(unittest.TestCase):
    def setUp(self):
        self.db_dir, self.db_url = create_temp_sqlite_for_test()
        self.storage = Storage.connect(
            self.db_url, 'http://storage_writer_filter_test')
        self.executor = Executor(inflight_limit=10)

    def tearDown(self):
        self.executor.shutdown()
        self.db_dir.cleanup()

    def dump_db(self):
        with self.storage.begin_transaction() as db_tx:
            for l in db_tx.connection.iterdump():
                logging.debug('%s', l)

    def update(self, filter, tx, tx_delta):
        for i in range(0, 5):
            try:
                upstream_delta = filter.update(tx, tx_delta)
                self.assertTrue(len(upstream_delta.rcpt_response) <=
                                len(tx.rcpt_to))
                break
            except VersionConflictException:
                logging.debug('VersionConflictException')
                if i == 4:
                    raise
                time.sleep(0.3)
                filter.get()

    def start_update(self, filter, tx, tx_delta):
        logging.debug('start_update')
        fut = self.executor.submit(partial(self.update, filter, tx, tx_delta))
        time.sleep(0.1)  # xxx  still need this?
        return fut

    def join(self, fut, timeout=1):
        # t.join(timeout=timeout)
        # self.assertFalse(t.is_alive())
        fut.result(timeout=timeout)

    # TODO coverage:
    # check_cache()
    # check()

    def create_filter(
            self, endpoint_yaml, create_id=None, update_id=None,
            handler=None):
        kwargs = {}
        return StorageWriterFilter(
            self.storage,
            rest_id_factory = lambda: create_id,
            rest_id = update_id,
            create_leased = True,
            sender=Sender('ingress'),
            endpoint_yaml = lambda sender: endpoint_yaml,
            tx_handler=handler)

    def test_smoke(self):
        endpoint_yaml = {
            'sf_mode': 'upstream_unavailability'
        }
        filter = self.create_filter(endpoint_yaml, create_id='tx_rest_id')
        tx = TransactionMetadata(
            sender=Sender('ingress'),
            mail_from=Mailbox('alice'))
        filter.update(tx, tx.copy())
        upstream_cursor = filter.release_transaction_cursor(0)
        self.assertEqual(upstream_cursor.rest_id, 'tx_rest_id')

        filter = self.create_filter(endpoint_yaml, update_id='tx_rest_id')
        prev = filter.get()
        self.assertIsNotNone(prev)
        tx = prev.copy()
        tx.rcpt_to = [Mailbox('bob@example.com')]
        filter.update(tx, prev.delta(tx))

        upstream_sender = None
        upstream_cursor2 = None
        def upstream(sender, cursor):
            nonlocal upstream_sender, upstream_cursor2
            upstream_sender = sender
            upstream_cursor2 = cursor
            return True

        filter = self.create_filter(endpoint_yaml, update_id='tx_rest_id',
                                    handler=upstream)
        prev = filter.get()
        logging.debug(prev.sender)
        tx = prev.copy()
        tx.rcpt_to.append(Mailbox('bob2@example.com'))
        filter.update(tx, prev.delta(tx))

        tx = filter.get()
        self.assertEqual(
            ['bob@example.com', 'bob2@example.com'],
            [r.mailbox for r in tx.rcpt_to])
        self.assertIsNotNone(upstream_sender)
        self.assertIsNotNone(u2 := upstream_cursor2())
        self.assertEqual(['bob2@example.com'],
                         [r.mailbox for r in u2.tx.rcpt_to])

    def test_store_and_forward_unavailability(self):
        endpoint_yaml = dict(endpoint_yaml_downstream_timeouts)
        endpoint_yaml.update({
            'sf_timeout': 1,
            'sf_mode': 'upstream_unavailability'})
        filter = self.create_filter(endpoint_yaml, create_id='tx_rest_id')
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'))
        filter.update(tx, tx.copy())
        tx = filter.get()
        self.assertIsNone(tx.mail_response)
        time.sleep(1)
        tx = filter.get()
        self.assertEqual(250, tx.mail_response.code)

        prev = tx.copy()
        tx.rcpt_to.append(Mailbox('bob@example.com'))
        filter.update(tx, prev.delta(tx))
        tx = filter.get()
        self.assertEqual([], tx.rcpt_response)
        time.sleep(1)
        tx = filter.get()
        self.assertEqual([250], [r.code for r in tx.rcpt_response])

        # add body, rcpt s&f -> immediate data s&f
        prev = tx.copy()
        tx.body = InlineBlob(b'Hello, world!', last=True)
        filter.update(tx, prev.delta(tx))
        tx = filter.get()
        self.assertEqual(250, tx.data_response.code)
        self.assertIn('store&forward', tx.data_response.message)

        cursor = self.storage.get_transaction_cursor()
        tx = cursor.load(rest_id='tx_rest_id')
        self.assertEqual({}, tx.notification)
        self.assertEqual({}, tx.retry)

    def test_body_blob(self):
        # from creation, gets handed off to OH
        # create/handoff to upstream
        endpoint_yaml = {}
        filter = self.create_filter(endpoint_yaml, create_id='tx_rest_id')
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'), rcpt_to=[Mailbox('bob')])
        filter.update(tx, tx.copy())
        upstream_cursor = filter.release_transaction_cursor(0)

        # create body
        logging.debug('create body')
        filter = self.create_filter(endpoint_yaml, update_id='tx_rest_id')
        blob_writer = filter.get_blob_writer(
            create=True, tx_body=True)
        d = b'hello, world!'
        chunk1 = 7
        blob_writer.append_data(0, d[0:chunk1])

        # append to body
        logging.debug('append body')
        filter = self.create_filter(endpoint_yaml, update_id='tx_rest_id')
        blob_writer = filter.get_blob_writer(
            create=False, tx_body=True)
        blob_writer.append_data(chunk1, d[chunk1:], len(d))

        tx = upstream_cursor.load()
        self.assertEqual(d, tx.body.pread(0))
        self.assertTrue(tx.body.finalized())
        self.assertTrue(upstream_cursor.input_done)

    def test_cancel(self):
        filter = self.create_filter(endpoint_yaml={}, create_id='tx_rest_id')
        tx = TransactionMetadata(sender=Sender('gateway'))
        filter.update(tx, tx.copy())
        tx = TransactionMetadata(cancelled = True)
        filter.update(tx, tx.copy())

        cursor = self.storage.get_transaction_cursor()
        cursor.load(rest_id='tx_rest_id')
        self.assertEqual(cursor.final_attempt_reason, 'downstream cancelled')

    def test_cancel_noop(self):
        orig_tx_cursor = self.storage.get_transaction_cursor()
        orig_tx = TransactionMetadata(
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob')])
        orig_tx_cursor.create('tx_rest_id', orig_tx, create_leased=True)
        orig_tx_cursor.start_attempt()
        attempt_delta = TransactionMetadata(mail_response = Response(550))
        orig_tx_cursor.write_envelope(
            tx_delta = TransactionMetadata(),
            attempt_delta = attempt_delta,
            finalize_attempt=True,
            final_attempt_reason='upstream permfail')

        filter = self.create_filter(endpoint_yaml={}, update_id='tx_rest_id')
        tx = filter.get()
        self.assertIsNotNone(tx.final_attempt_reason)
        prev = tx.copy()
        tx.cancelled = True
        filter.update(tx, prev.delta(tx))

        orig_tx_cursor.load()
        self.assertIsNone(orig_tx_cursor.tx.cancelled)

    # representative of add_route which writes body_blob=BlobCursor
    def test_body_blob_cursor(self):
        orig_tx = TransactionMetadata(
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob')])

        endpoint_yaml = {
            'sf_mode': 'mixed_data_response'
        }

        orig_filter = self.create_filter(
            endpoint_yaml,
            create_id='orig_tx_rest_id')

        orig_filter.update(orig_tx, orig_tx.copy())
        blob_writer = orig_filter.get_blob_writer(
            create=True, tx_body=True)

        d = b'hello, '
        blob_writer.append_data(0, d)

        filter = self.create_filter(endpoint_yaml, create_id='tx_rest_id')
        tx = TransactionMetadata(sender=Sender('exploder'))
        filter.update(tx, tx.copy())

        upstream_cursor = filter.release_transaction_cursor(0)
        upstream_cursor.start_attempt()

        tx = TransactionMetadata(mail_from = Mailbox('alice'))
        t = self.start_update(filter, tx, tx.copy())

        for i in range(0,5):
            if upstream_cursor.tx.mail_from is not None:
                break
            upstream_cursor.wait(1)
            upstream_cursor.load()
        else:
            self.fail('no mail_from')
        for i in range(0, 5):
            try:
                upstream_cursor.write_envelope(
                    tx_delta = TransactionMetadata(),
                    attempt_delta=TransactionMetadata(
                        mail_response=Response(201)))
            except VersionConflictException:
                logging.debug('VersionConflictException')
                if i == 4:
                    raise
                time.sleep(0.3)
                upstream_cursor.load()

        self.join(t)
        for i in range(0,5):
            tx = filter.get()
            if tx.mail_response:
                break
            filter.wait(1)
        else:
            self.fail('no mail_response')
        self.assertEqual(tx.mail_response.code, 201)

        tx = filter.get()
        tx_delta = TransactionMetadata(rcpt_to = [Mailbox('bob')])
        tx.merge_from(tx_delta)
        t = self.start_update(filter, tx, tx_delta)
        for i in range(0,5):
            if len(upstream_cursor.tx.rcpt_to) == 1:
                break
            upstream_cursor.wait(1)
            upstream_cursor.load()
        else:
            self.fail('no rcpt')
        upstream_cursor.write_envelope(
            tx_delta=TransactionMetadata(),
            attempt_delta=TransactionMetadata(rcpt_response=[Response(202)]))
        self.join(t)

        tx = filter.get()
        self.assertEqual(
            [rr.code for rr in tx.rcpt_response], [202])

        # update w/incomplete blob ->noop
        tx_delta = TransactionMetadata()
        tx_delta.body = orig_filter.get().body
        tx.merge_from(tx_delta)
        with self.assertRaises(ValueError):
            filter.update(tx, tx_delta)

        d2 = b'world!'
        appended, length, content_length = blob_writer.append_data(
            blob_writer.len(), d2, blob_writer.len() + len(d2))
        self.assertTrue(appended)
        self.assertEqual(length, content_length)

        tx_delta = TransactionMetadata(body=orig_filter.get().body)
        tx.merge_from(tx_delta)
        t = self.start_update(filter, tx, tx_delta)

        for i in range(0,5):
            if upstream_cursor.tx.body is not None and upstream_cursor.tx.body.finalized():
                self.assertEqual(d + d2, upstream_cursor.tx.body.pread(0))
                break
            upstream_cursor.wait(1)
            upstream_cursor.load()
        else:
            self.fail('no body')
        upstream_cursor.write_envelope(
            tx_delta=TransactionMetadata(),
            attempt_delta=TransactionMetadata(data_response=Response(203)))

        self.join(t)

        tx = filter.get()
        self.assertEqual(tx.data_response.code, 203)

    def test_message_builder_blob_reuse(self):
        message_builder_json = {
            "text_body": [{
                "content_type": "text/plain",
                "content": {"create_id": "test_message_builder_blob"}
            }]
        }

        orig_filter = self.create_filter(endpoint_yaml={},
                                         create_id='test_message_builder')
        orig_tx = TransactionMetadata()
        orig_tx.body = MessageBuilderSpec(message_builder_json)
        orig_tx.body.parse_blob_specs()
        orig_filter.update(orig_tx, orig_tx.copy())

        logging.debug(orig_filter.tx_group.tx_cursors[0].blobs)
        blob_writer = orig_filter.get_blob_writer(
            create=False, blob_rest_id='test_message_builder_blob')
        b1 = b'hello, '
        blob_writer.append_data(0, b1)
        blob_writer = orig_filter.get_blob_writer(
            create=False, blob_rest_id='test_message_builder_blob')
        b2 = b'world!'
        blob_writer.append_data(len(b1), b2, len(b1) + len(b2))

        # now do it again reusing the same blob
        filter = self.create_filter(
            endpoint_yaml={}, create_id='test_message_builder_reuse')
        message_builder_json['text_body'][0]['content'] = {
            'reuse_uri': '/transactions/test_message_builder/'
                         'blob/test_message_builder_blob'}
        tx = TransactionMetadata(
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob')],
            body = MessageBuilderSpec(message_builder_json))
        tx.body.parse_blob_specs()
        upstream_delta = filter.update(tx, tx.copy())
        self.assertIsNone(upstream_delta.data_response)
        upstream_cursor = self.storage.get_transaction_cursor()
        upstream_cursor.load(rest_id='test_message_builder_reuse')
        logging.debug(upstream_cursor.tx.body.json)
        self.assertEqual(
            upstream_cursor.tx.body.json['text_body'][0]['content']['create_id'],
            'test_message_builder_blob')


    def test_timeout_mail(self):
        filter = self.create_filter(endpoint_yaml_downstream_timeouts,
                                    create_id='tx_rest_id')
        filter._create(TransactionMetadata())

        tx = TransactionMetadata(mail_from = Mailbox('alice'))
        t = self.start_update(filter, tx, tx)
        self.join(t, 3)
        self.assertIsNone(tx.mail_response)


    def test_tx_body_inline_reuse(self):
        filter = self.create_filter(endpoint_yaml={}, create_id='inline')
        b = b'hello, world!'
        tx = TransactionMetadata(
            body = InlineBlob(b, last=True))
        # create w/ tx.inline_body
        filter.update(tx, tx.copy())

        self.dump_db()

        filter2 = self.create_filter(endpoint_yaml={}, create_id='reuse')
        tx2 = TransactionMetadata(
                body = BlobSpec(reuse_uri=BlobUri('inline', tx_body=True)))
        # create w/ body blob uri
        filter2.update(tx2, tx2.copy())

        tx_reader = self.storage.get_transaction_cursor()
        tx_reader.load(rest_id='inline')
        self.assertTrue(isinstance(tx_reader.tx.body, Blob))
        self.assertEqual(tx_reader.tx.body.pread(0), b)

    def test_create_leased(self):
        filter = self.create_filter(endpoint_yaml={}, create_id='inline')
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'))
        filter.update(tx, tx.copy())

        self.assertIsNone(self.storage.load_one())

    # XXX: verify notify/retry on db tx
    def _run_test(self, t : Test):
        endpoint_yaml = dict(endpoint_yaml_downstream_timeouts)
        endpoint_yaml.update({
            'sf_mode': t.sf_mode})
        filter = self.create_filter(
            endpoint_yaml,
            create_id='tx_rest_id',
            handler = lambda x,y: True)

        def to_response(r : Result) -> Optional[Response]:
            if r == Result.TEMP:
                return Response(450)
            elif r == Result.PERM:
                return Response(550)
            elif r == Result.SUCCESS:
                return Response(250)
            return None

        # actual, expectation
        def stage_resp(s, ss, r) -> Optional[Response]:
            if s < ss:
                return to_response(Result.SUCCESS)
            elif s == ss:
                return to_response(r)
            return None

        def upstream(cursor, rcpt):
            tx = cursor.load()
            logging.debug(tx)
            attempt_delta = TransactionMetadata()
            if tx.mail_from and not tx.mail_response:
                attempt_delta.mail_response = stage_resp(
                    Stage.MAIL, rcpt.stage, rcpt.upstream_result)
            if tx.rcpt_to and not tx.rcpt_response and rcpt.stage >= Stage.RCPT and (rcpt.stage > Stage.RCPT or rcpt.upstream_result != Result.TIMEOUT):
                attempt_delta.rcpt_response = [
                    stage_resp(Stage.RCPT, rcpt.stage, rcpt.upstream_result)]
            if tx._body_last() and not tx.data_response:
                attempt_delta.data_response = stage_resp(
                    Stage.DATA, rcpt.stage, rcpt.upstream_result)
            logging.debug(attempt_delta)
            cursor.write_envelope(TransactionMetadata(),
                                  attempt_delta=attempt_delta)

        tx = TransactionMetadata(
            sender=Sender('ingress'),
            mail_from=Mailbox('alice'))
        filter.update(tx, tx.copy())

        cursor = filter.release_transaction_cursor(0)
        assert cursor is not None
        cursor.start_attempt()
        upstream_cursors = [cursor]
        upstream(cursor, t.rcpt[0])

        def get_downstream():
            for i in range(0,20):
                tx = filter.get()
                logging.debug(tx)
                if not tx.req_inflight():
                    return tx
                time.sleep(0.3)
            else:
                self.fail('upstream timeout')

        tx = get_downstream()

        for i,rcpt in enumerate(t.rcpt):
            prev = tx.copy()
            tx.rcpt_to.append(Mailbox('bob%d' % i))
            filter.update(tx, prev.delta(tx))
            if i > 0:
                cursor = filter.release_transaction_cursor(i)
                assert cursor is not None
                cursor.start_attempt()
                upstream_cursors.append(cursor)
            else:
                cursor = upstream_cursors[0]
            upstream(cursor, rcpt)

        txx = filter.get()
        assert txx is not None
        tx = txx
        prev = tx.copy()
        tx.body = InlineBlob(b'hello, world!', last=True)
        filter.update(tx, prev.delta(tx))

        for i,rcpt in enumerate(t.rcpt):
            upstream(upstream_cursors[i], rcpt)

        tx = get_downstream()

        def check_resp(stage, exp_stage, exp_result, exp_sf, resp):
            if exp_stage > stage:
                assert resp is not None
                self.assertTrue(resp.ok())
            elif exp_stage == stage:
                if exp_result == Result.PERM:
                    self.assertTrue(resp.perm())
                elif exp_sf:
                    self.assertTrue(resp.ok())
                else:
                    self.assertEqual(to_response(exp_result).code, resp.code)
            else:  # stage > exp_stage
                if exp_result != Result.PERM and exp_sf:
                    self.assertTrue(resp.ok())

        check_resp(Stage.MAIL, t.stage, t.result, False, tx.mail_response)
        for i,r in enumerate(t.rcpt):
            check_resp(Stage.RCPT, r.stage, r.upstream_result, r.expect_sf,
                       tx.rcpt_response[i] if i < len(tx.rcpt_response) else None)
        check_resp(Stage.DATA, t.stage, t.result, False, tx.data_response)


    # mail temp -> sf
    def test_single_rcpt_mail_temp(self):
        self._run_test(Test(
            rcpt = [Recipient(Stage.MAIL, Result.TEMP, expect_sf=True)],
            stage = Stage.DATA,
            result = Result.SUCCESS,
            sf_mode = 'upstream_unavailability'
        ))

    # mail perm -> perm
    def test_single_rcpt_mail_perm(self):
        self._run_test(Test(
            rcpt = [Recipient(Stage.MAIL, Result.PERM)],
            stage = Stage.MAIL,
            result = Result.PERM,
            sf_mode = 'upstream_unavailability'
        ))

    # mail timeout -> sf
    def test_single_rcpt_mail_timeout(self):
        self._run_test(Test(
            rcpt = [Recipient(Stage.MAIL, Result.TIMEOUT, expect_sf=True)],
            stage = Stage.DATA,
            result = Result.SUCCESS,
            sf_mode = 'upstream_unavailability'
        ))

    # rcpt temp -> sf
    def test_single_rcpt_rcpt_temp(self):
        self._run_test(Test(
            rcpt = [Recipient(Stage.RCPT, Result.TEMP, expect_sf=True)],
            stage = Stage.DATA,
            result = Result.SUCCESS,
            sf_mode = 'upstream_unavailability'
        ))

    # rcpt perm -> perm
    def test_single_rcpt_rcpt_perm(self):
        self._run_test(Test(
            rcpt = [Recipient(Stage.RCPT, Result.PERM)],
            stage = Stage.RCPT,
            result = Result.PERM,
            sf_mode = 'upstream_unavailability'
        ))

    # rcpt timeout -> sf
    def test_single_rcpt_rcpt_timeout(self):
        self._run_test(Test(
            rcpt = [Recipient(Stage.RCPT, Result.TIMEOUT, expect_sf=True)],
            stage = Stage.DATA,
            result = Result.SUCCESS,
            sf_mode = 'upstream_unavailability'
        ))

    # data temp -> sf
    def test_single_rcpt_data_temp(self):
        self._run_test(Test(
            rcpt = [Recipient(Stage.DATA, Result.TEMP, expect_sf=True)],
            stage = Stage.DATA,
            result = Result.SUCCESS,
            sf_mode = 'upstream_unavailability'
        ))

    # data perm -> perm
    def test_single_rcpt_data_perm(self):
        self._run_test(Test(
            rcpt = [Recipient(Stage.DATA, Result.PERM)],
            stage = Stage.DATA,
            result = Result.PERM,
            sf_mode = 'upstream_unavailability'
        ))

    # data timeout -> sf
    def test_single_rcpt_data_timeout(self):
        self._run_test(Test(
            rcpt = [Recipient(Stage.DATA, Result.TIMEOUT, expect_sf=True)],
            stage = Stage.DATA,
            result = Result.SUCCESS,
            sf_mode = 'upstream_unavailability'
        ))

    def test_single_rcpt_success(self):
        self._run_test(Test(
            rcpt = [Recipient(Stage.DATA, Result.SUCCESS)],
            stage = Stage.DATA,
            result = Result.SUCCESS,
            sf_mode = 'upstream_unavailability'
        ))

    def test_multi_rcpt_mixed_data(self):
        self._run_test(Test(
            rcpt = [Recipient(Stage.DATA, Result.SUCCESS),
                    Recipient(Stage.DATA, Result.PERM)],
            stage = Stage.DATA,
            result = Result.SUCCESS,
            sf_mode = 'upstream_unavailability'
        ))


    def test_multi_rcpt_success(self):
        self._run_test(Test(
            rcpt = [Recipient(Stage.DATA, Result.SUCCESS),
                    Recipient(Stage.DATA, Result.SUCCESS)],
            stage = Stage.DATA,
            result = Result.SUCCESS,
            sf_mode = 'upstream_unavailability'
        ))

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d %(message)s')
    unittest.main()
