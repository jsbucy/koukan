# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import List, Optional

import unittest
import logging
import time
from functools import partial

from koukan.deadline import Deadline
from koukan.storage import Storage, TransactionCursor
from koukan.response import Response
from koukan.fake_endpoints import FakeSyncFilter, MockAsyncFilter
from koukan.filter import Mailbox, TransactionMetadata
from koukan.executor import Executor

from koukan.blob import CompositeBlob, InlineBlob

from koukan.exploder import Exploder, Recipient

import koukan.sqlite_test_utils as sqlite_test_utils
from koukan.async_filter_wrapper import AsyncFilterWrapper

class Rcpt:
    addr : str
    mail_resp : Optional[Response]
    rcpt_resp : Optional[Response]
    data_resp : List[Optional[Response]]
    store_and_forward : bool

    def __init__(self, addr, m=None, r=None, d=None, store_and_forward=False):
        self.addr = addr
        self.mail_resp = m
        self.rcpt_resp = r
        self.data_resp = d
        self.store_and_forward = store_and_forward

    def set_endpoint(self, endpoint):
        self.endpoint = endpoint

        def exp(tx, tx_delta):
            upstream_delta = TransactionMetadata()
            if self.mail_resp:
                upstream_delta.mail_response = self.mail_resp
            if self.rcpt_resp:
                upstream_delta.rcpt_response=[self.rcpt_resp]
            assert tx.merge_from(upstream_delta)
            return upstream_delta
        self.endpoint.expect_update(exp)


    def set_data_response(self, parent, i):
        logging.debug('Rcpt.set_data_response %d %d', i, len(self.data_resp))
        def exp(tx, tx_delta):
            parent.assertIsNotNone(tx.body_blob)
            upstream_delta = TransactionMetadata()
            if i < len(self.data_resp) and self.data_resp[i]:
                upstream_delta.data_response = self.data_resp[i]
            tx.merge_from(upstream_delta)
            return upstream_delta
        self.endpoint.expect_update(exp)

    def expect_store_and_forward(self, parent):
        if not self.store_and_forward:
            return
        def exp(tx, tx_delta):
            parent.assertIsNotNone(tx.retry)
            parent.assertIsNotNone(tx.notification)
            return TransactionMetadata()
        self.endpoint.expect_update(exp)

class Test:
    mail_from : str
    rcpt : List[Rcpt]
    data : List[bytes]
    expected_mail_resp : Response
    expected_rcpt_resp : List[Response]
    expected_data_resp : List[Optional[Response]]
    def __init__(self, m, r, d, em, er, ed):
        self.mail_from = m
        self.rcpt = r
        self.data = d
        self.expected_mail_resp = em
        self.expected_rcpt_resp = er
        self.expected_data_resp = ed


vec_mx = [
    # single recipient
    # mail perm
    Test(
        'alice',
        [ Rcpt('bob', Response(501), Response(502), [],
               store_and_forward=False) ],
        [],
        Response(250),  # noop mail/injected
        [Response(501)],  # upstream
        None,
    ),

    # mail temp
    Test(
        'alice',
        [ Rcpt('bob', Response(401), Response(500), [],
               store_and_forward=False) ],
        [],
        Response(250),  # injected
        [Response(401)],  # upstream
        None,
    ),

    # mail success, rcpt perm
    Test(
        'alice',
        [ Rcpt('bob', Response(201), Response(501), [],
               store_and_forward=False) ],
        [],
        Response(250),  # injected
        [Response(501)],  # upstream
        None,
    ),

    # mail success, rcpt temp
    Test(
        'alice',
        [ Rcpt('bob', Response(201), Response(401), [],
               store_and_forward=False) ],
        [],
        Response(250),  # injected
        [Response(401)],  # upstreawm
        None,
    ),

    # mail, rcpt success, data !last perm
    Test(
        'alice',
        [ Rcpt('bob', Response(201), Response(202), [Response(501)],
               store_and_forward=False) ],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202)],  # upstream
        [Response(501)],  # upstream
    ),

    # mail, rcpt success, data !last temp
    Test(
        'alice',
        [ Rcpt('bob', Response(201), Response(202), [Response(401)],
               store_and_forward=False) ],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202)],  # upstream
        [Response(401)],  # upstream
    ),

    # mail, rcpt success, data !last success, last perm
    Test(
        'alice',
        [ Rcpt('bob', Response(201), Response(202), [None, Response(501)],
               store_and_forward=False) ],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202)],  # upstream
        [None, Response(501)],  # upstream
    ),

    # mail, rcpt success, data !last success, last temp
    Test(
        'alice',
        [ Rcpt('bob', Response(201), Response(202), [None, Response(401)]) ],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202)],  # upstream
        [None, Response(401)],  # upstream
    ),

    # mail, rcpt, !last success, last success
    Test(
        'alice',
        [ Rcpt('bob', Response(201), Response(202), [None, Response(203)],
               store_and_forward=False) ],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202)],  # upstream
        [None, Response(203)],  # upstream
    ),

    # multi-rcpt: all success/cutthrough
    Test(
        'alice',
        [ Rcpt('bob1', Response(201), Response(202), [None, Response(203)],
               store_and_forward=False),
          Rcpt('bob2', Response(204), Response(205), [None, Response(206)],
               store_and_forward=False) ],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202), Response(205)],  # upstream
        [None, Response(203)],  # upstream/cutthrough
    ),

    # multi-rcpt
    # first succeeds, second fails at rcpt
    Test(
        'alice',
        [ Rcpt('bob1', Response(201), Response(202), [None, Response(203)],
               store_and_forward=False),
          Rcpt('bob2', Response(204), Response(405), [],
               store_and_forward=False) ],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202), Response(405)],  # upstream
        [None, Response(203)],  # injected
    ),
]

vec_msa = [
    # single recipient
    # mail perm
    Test(
        'alice',
        [ Rcpt('bob', Response(501), None, [],
               store_and_forward=False) ],
        [],
        Response(250),  # noop mail/injected
        [Response(501)],  # upstream
        None,
    ),

    # mail temp
    Test(
        'alice',
        [ Rcpt('bob', Response(401), None, [],
               store_and_forward=True) ],
        ['hello, world!'],
        Response(250),  # noop mail/injected
        [Response(250)],  # injected/upgraded
        [Response(250)],
    ),

    # mail success, rcpt perm
    Test(
        'alice',
        [ Rcpt('bob', Response(201), Response(501), [],
               store_and_forward=False) ],
        [],
        Response(250),  # injected
        [Response(501)],  # upstream
        None,
    ),

    # mail success, rcpt temp
    Test(
        'alice',
        [ Rcpt('bob', Response(201), Response(401), [],
               store_and_forward=True) ],
        ['hello, world!'],
        Response(250),  # injected
        [Response(250)],  # injected/upgraded
        [Response(250)],
    ),

    # mail, rcpt success, data !last perm
    Test(
        'alice',
        [ Rcpt('bob', Response(201), Response(202), [Response(501)],
               store_and_forward=False) ],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202)],  # upstream
        [Response(501)],  # upstream
    ),

    # mail, rcpt success, data !last temp
    Test(
        'alice',
        [ Rcpt('bob', Response(201), Response(202), [Response(401)],
               store_and_forward=True) ],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202)],  # upstream
        [None, Response(250)],  # injected/upgraded
    ),

    # mail, rcpt success, data !last ok, last perm
    Test(
        'alice',
        [ Rcpt('bob', Response(201), Response(202), [None, Response(501)],
               store_and_forward=False) ],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202)],  # upstream
        [None, Response(501)],  # upstream
    ),

    # mail, rcpt success, data !last ok, last temp
    Test(
        'alice',
        [ Rcpt('bob', Response(201), Response(202), [None, Response(401)],
               store_and_forward=True) ],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202)],  # upstream
        [None, Response(250)],  # injected/upgraded
    ),

    # mail, rcpt, data success
    Test(
        'alice',
        [ Rcpt('bob', Response(201), Response(202), [None, Response(203)],
               store_and_forward=False) ],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202)],  # upstream
        [None, Response(203)],  # upstream
    ),

    # multi-rcpt
    # first recipient succeeds, second permfails after MAIL
    Test(
        'alice',
        [ Rcpt('bob1', Response(201), Response(202), [None, Response(203)],
               store_and_forward=False),
          Rcpt('bob2', Response(501), Response(502), [],
               store_and_forward=False)],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202), Response(501)],  # upstream mail err -> rcpt resp
        [None, Response(203)],  # same data resp
    ),

    # multi-rcpt
    # first recipient succeeds, second permfails after RCPT
    Test(
        'alice',
        [ Rcpt('bob1', Response(201), Response(202), [None, Response(203)],
               store_and_forward=False),
          Rcpt('bob2', Response(201), Response(501), [],
               store_and_forward=False)],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202), Response(501)],  # upstream
        [None, Response(203)],  # same data resp
    ),

    # multi-rcpt
    # first recipient succeeds, second permfails after !last data
    Test(
        'alice',
        [ Rcpt('bob1', Response(201), Response(202), [None, Response(203)],
               store_and_forward=False),
          Rcpt('bob2', Response(204), Response(205), [Response(501)],
               store_and_forward=True)],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202), Response(205)],  # upstream
        [None, Response(250)],  # 'async mixed upstream'
    ),

    # multi-rcpt
    # first recipient succeeds, second permfails after last data
    Test(
        'alice',
        [ Rcpt('bob1', Response(201), Response(202), [None, Response(203)],
               store_and_forward=False),
          Rcpt('bob2', Response(204), Response(205), [None, Response(501)],
               store_and_forward=True)],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202), Response(205)],  # upstream
        [None, Response(250)],  # 'async mixed upstream'
    ),

]

class ExploderTest(unittest.TestCase):
    executor : Executor

    def setUp(self):
        self.executor = Executor(inflight_limit=10, watchdog_timeout=30,
                                 debug_futures=True)

        self.db_dir, self.db_url = sqlite_test_utils.create_temp_sqlite_for_test()
        self.storage = Storage.connect(self.db_url, session_uri='http://exploder_test')
        self.upstream_endpoints = []

    def tearDown(self):
        self.executor.shutdown(timeout=5)
        self.db_dir.cleanup()

    def dump_db(self):
        for l in self.storage.db.iterdump():
            print(l)

    def add_endpoint(self):
        endpoint = MockAsyncFilter()
        self.upstream_endpoints.append(endpoint)
        return endpoint

    def factory(self):  #, host):
        return AsyncFilterWrapper(self.upstream_endpoints.pop(0), 5)

    # xxx all tests validate response message

    def _test_one(self, msa, t : Test):
        exploder = Exploder('output-chain',
                            self.factory,
                            executor=self.executor,
                            rcpt_timeout=2,
                            msa=msa,
                            default_notification={})

        for r in t.rcpt:
            endpoint = self.add_endpoint()
            r.set_endpoint(endpoint)

        tx = TransactionMetadata(mail_from=Mailbox(t.mail_from))
        exploder.on_update(tx, tx.copy())
        self.assertEqual(tx.mail_response.code, t.expected_mail_resp.code)
        for i,r in enumerate(t.rcpt):
            updated = tx.copy()
            updated.rcpt_to.append(Mailbox(r.addr))
            tx_delta = tx.delta(updated)
            tx = updated
            upstream_delta = exploder.on_update(tx, tx_delta)
            self.assertEqual(
                [rr.code for rr in upstream_delta.rcpt_response],
                [t.expected_rcpt_resp[i].code])

        self.assertEqual([rr.code for rr in tx.rcpt_response],
                         [rr.code for rr in t.expected_rcpt_resp])

        blob = None
        content_length = 0
        for d in t.data:
            content_length += len(d)

        for i,d in enumerate(t.data):
            if blob is None:
                blob = InlineBlob(d, content_length=content_length)
            else:
                blob.append(d)
            tx_delta = TransactionMetadata(body_blob=blob)
            tx.merge_from(tx_delta)
            for r in t.rcpt:
                r.set_data_response(self, i)
                if t.expected_data_resp[i] is not None:
                    r.expect_store_and_forward(self)
            exploder.on_update(tx, tx_delta)
            if t.expected_data_resp[i] is not None:
                self.assertEqual(tx.data_response.code,
                                 t.expected_data_resp[i].code)
                break
            else:
                self.assertIsNone(tx.data_response)

    def test_mx(self):
        for i,t in enumerate(vec_mx):
            logging.info('test_mx %d', i)
            self._test_one(False, t)

    def test_msa(self):
        self._test_one(True, vec_msa[1])
        for i,t in enumerate(vec_msa):
            logging.info('test_msa %d', i)
            self._test_one(True, t)

    # Several of these one-off tests exercise the multi-rcpt fan-out
    # in Exploder._on_rcpts() that is otherwise dead code until the gateway
    # implements SMTP PIPELINING
    def testSuccess(self):
        exploder = Exploder(self.factory,
                            'output-chain',
                            executor=self.executor)

        tx = TransactionMetadata()
        tx.mail_from = Mailbox('alice')
        exploder.on_update(tx, tx.copy())
        self.assertEqual(tx.mail_response.code, 250)

        up0 = self.add_endpoint()
        up1 = self.add_endpoint()

        def exp(i, tx, tx_delta):
            upstream_delta = TransactionMetadata(
                mail_response=Response(250),
                rcpt_response=[Response(201 + i)])
            tx.merge_from(upstream_delta)
            return upstream_delta
        up0.expect_update(partial(exp, 0))
        up1.expect_update(partial(exp, 1))

        tx_delta = TransactionMetadata(
            rcpt_to = [ Mailbox('bob'), Mailbox('bob2') ])
        assert tx.merge_from(tx_delta) is not None
        exploder.on_update(tx, tx_delta)

        body_blob = CompositeBlob()
        b = InlineBlob(b'hello, ')
        body_blob.append(b, 0, b.len())

        def exp_data(tx, delta):
            self.assertFalse(tx.body_blob and tx.body_blob.finalized())
            return TransactionMetadata()
        up0.expect_update(exp_data)
        up1.expect_update(exp_data)

        tx_delta = TransactionMetadata()
        tx_delta.body_blob = body_blob
        assert tx.merge_from(tx_delta) is not None
        exploder.on_update(tx, tx_delta)

        b = InlineBlob(b'world!')
        body_blob.append(b, 0, b.len(), True)

        def exp_data_last(tx, delta):
            self.assertTrue(tx.body_blob.finalized())
            upstream_delta = TransactionMetadata(
                data_response=Response(250))
            tx.merge_from(upstream_delta)
            return upstream_delta
        up0.expect_update(exp_data_last)
        up1.expect_update(exp_data_last)

        # same delta: body has grown
        exploder.on_update(tx, tx_delta)
        logging.debug(tx.data_response.message)
        self.assertEqual(tx.data_response.code, 250)
        for endpoint in self.upstream_endpoints:
            self.assertEqual(endpoint.body_blob.pread(0), b'hello, world!')

        # don't expect an additional update to enable retry/notification

    def testMxRcptTemp(self):
        exploder = Exploder('output-chain',
                            self.factory,
                            executor=self.executor,
                            rcpt_timeout=2, msa=False,
                            default_notification={'host': 'smtp-out'})

        # The vector tests cover the non-pipelined updates we expect
        # to get from the current gw implementation. This exercises
        # the multi-update case.
        tx = TransactionMetadata(
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob'),
                       Mailbox('bob2')])

        up0 = self.add_endpoint()
        up1 = self.add_endpoint()

        def exp(rcpt_code, tx, tx_delta):
            upstream_delta = TransactionMetadata(
                mail_response=Response(250),
                rcpt_response=[Response(rcpt_code)])
            tx.merge_from(upstream_delta)
            return upstream_delta
        up0.expect_update(partial(exp, 201))
        up1.expect_update(partial(exp, 450))
        exploder.on_update(tx, tx.copy())

        self.assertEqual(tx.mail_response.code, 250)
        self.assertEqual(tx.rcpt_response[0].code, 201)
        self.assertEqual(tx.rcpt_response[1].code, 450)

        def exp_data(data_code, tx, tx_delta):
            upstream_delta = TransactionMetadata(
                data_response=Response(data_code))
            tx.merge_from(upstream_delta)
            return upstream_delta
        up0.expect_update(partial(exp_data, 202))
        up1.expect_update(partial(exp_data, 400))

        tx_delta = TransactionMetadata(body_blob=InlineBlob(b'hello', last=True))
        tx.merge_from(tx_delta)
        exploder.on_update(tx, tx_delta)
        self.assertEqual(tx.data_response.code, 202)
        # don't expect an additional update to enable retry/notification:
        # first rcpt succeeded -> no retry
        # second failed at rcpt -> wasn't accepted -> no retry


class ExploderRecipientTest(unittest.TestCase):
    def assertEqualStatus(self, x, y):
        self.assertEqual(x is None, y is None)
        if x is None:
            return
        self.assertEqual(x.code, y.code)

    def _test(
            self,
            msa,
            upstream_mail_resp,
            upstream_rcpt_resp,
            exp_mail_resp,
            exp_rcpt_resp,
            exp_sf_after_env,
            exp_status_after_env,

            upstream_data_resp = None,
            exp_sf_after_data = None,
            exp_status_after_data = None,

            upstream_data_resp_last = None,
            exp_sf_after_data_last = None,
            exp_status_after_data_last = None):
        endpoint = FakeSyncFilter()

        downstream_tx = TransactionMetadata(
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob')])
        downstream_delta = downstream_tx.copy()

        rcpt = Recipient('smtp-out',
                         endpoint,
                         msa=msa,
                         rcpt=downstream_tx.rcpt_to[0])

        def exp_rcpt(tx, tx_delta):
            upstream_delta = TransactionMetadata(version=2)
            if upstream_mail_resp:
                upstream_delta.mail_response = upstream_mail_resp
            if upstream_rcpt_resp:
                upstream_delta.rcpt_response = [upstream_rcpt_resp]
            tx.merge_from(upstream_delta)
            return upstream_delta
        endpoint.add_expectation(exp_rcpt)

        rcpt._on_rcpt(downstream_tx, downstream_delta, Deadline(2))
        self.assertEqual(rcpt.mail_response.code, exp_mail_resp.code)
        self.assertEqual(rcpt.rcpt_response.code, exp_rcpt_resp.code)
        self.assertEqual(rcpt.store_and_forward, exp_sf_after_env)
        self.assertEqualStatus(rcpt.status, exp_status_after_env)

        if exp_mail_resp.err() or exp_rcpt_resp.err():
            return

        def exp_data(tx, tx_delta):
            logging.debug('exp_data %s', tx)
            self.assertIsNotNone(tx.body_blob)
            self.assertFalse(tx.body_blob.finalized())
            upstream_delta = TransactionMetadata(version=4)
            if upstream_data_resp:
                upstream_delta.data_response = upstream_data_resp
            tx.merge_from(upstream_delta)
            return upstream_delta
        endpoint.add_expectation(exp_data)

        d = 'hello, world!'
        rcpt._append_upstream(InlineBlob(d[0:7], len(d)), Deadline(2))
        self.assertEqual(rcpt.store_and_forward, exp_sf_after_data)
        self.assertEqualStatus(rcpt.status, exp_status_after_data)

        if exp_status_after_data is not None and exp_status_after_data.err():
            return

        def exp_data_last(tx, tx_delta):
            logging.debug('exp_data_last %s', tx)
            upstream_delta = TransactionMetadata()
            self.assertTrue(tx.body_blob.finalized())
            if upstream_data_resp_last:
                upstream_delta.data_response = upstream_data_resp_last
            tx.merge_from(upstream_delta)
            return upstream_delta
        endpoint.add_expectation(exp_data_last)
        blob = InlineBlob(d, len(d))
        self.assertTrue(blob.finalized())
        rcpt._append_upstream(blob, Deadline(2))
        self.assertEqual(rcpt.store_and_forward, exp_sf_after_data_last)
        self.assertEqualStatus(rcpt.status, exp_status_after_data_last)

        self.assertFalse(endpoint.expectation)

    def test_msa_store_and_forward_after_mail_timeout(self):
        self._test(
            msa=True,
            upstream_mail_resp = None,
            upstream_rcpt_resp = None,
            exp_mail_resp = Response(250),
            exp_rcpt_resp = Response(250),
            exp_sf_after_env = True,
            exp_status_after_env = None,
            upstream_data_resp = None,
            exp_sf_after_data = True,
            exp_status_after_data = None,
            upstream_data_resp_last = None,
            exp_sf_after_data_last = True,
            exp_status_after_data_last = Response(250))


    def test_msa_store_and_forward_after_mail_temp(self):
        self._test(
            msa=True,
            upstream_mail_resp = Response(401),
            upstream_rcpt_resp = None,
            exp_mail_resp = Response(250),
            exp_rcpt_resp = Response(250),
            exp_sf_after_env = True,
            exp_status_after_env = None,
            upstream_data_resp = None,
            exp_sf_after_data = True,
            exp_status_after_data = None,
            upstream_data_resp_last = None,
            exp_sf_after_data_last = True,
            exp_status_after_data_last = Response(250))

    def test_msa_store_and_forward_after_rcpt_timeout(self):
        self._test(
            msa=True,
            upstream_mail_resp = Response(201),
            upstream_rcpt_resp = None,
            exp_mail_resp = Response(201),
            exp_rcpt_resp = Response(250),
            exp_sf_after_env = True,
            exp_status_after_env = None,
            upstream_data_resp = None,
            exp_sf_after_data = True,
            exp_status_after_data = None,
            upstream_data_resp_last = None,
            exp_sf_after_data_last = True,
            exp_status_after_data_last = Response(250))

    def test_msa_store_and_forward_after_rcpt_temp(self):
        self._test(
            msa=True,
            upstream_mail_resp = Response(201),
            upstream_rcpt_resp = Response(401),
            exp_mail_resp = Response(201),
            exp_rcpt_resp = Response(250),
            exp_sf_after_env = True,
            exp_status_after_env = None,
            upstream_data_resp = None,
            exp_sf_after_data = True,
            exp_status_after_data = None,
            upstream_data_resp_last = None,
            exp_sf_after_data_last = True,
            exp_status_after_data_last = Response(250))

    def test_msa_store_and_forward_after_data_timeout(self):
        self._test(
            msa=True,
            upstream_mail_resp = Response(201),
            upstream_rcpt_resp = Response(202),
            exp_mail_resp = Response(201),
            exp_rcpt_resp = Response(202),
            exp_sf_after_env = False,
            exp_status_after_env = None,

            upstream_data_resp = None,
            exp_sf_after_data = False,
            exp_status_after_data = None,

            upstream_data_resp_last = None,
            exp_sf_after_data_last = True,
            exp_status_after_data_last = Response(250))

    def test_msa_store_and_forward_after_data_temp(self):
        self._test(
            msa=True,
            upstream_mail_resp = Response(201),
            upstream_rcpt_resp = Response(202),
            exp_mail_resp = Response(201),
            exp_rcpt_resp = Response(202),
            exp_sf_after_env = False,
            exp_status_after_env = None,

            upstream_data_resp = Response(401),
            exp_sf_after_data = True,
            exp_status_after_data = None,

            upstream_data_resp_last = None,
            exp_sf_after_data_last = True,
            exp_status_after_data_last = Response(250))

    def test_msa_store_and_forward_after_data_last(self):
        self._test(
            msa=True,
            upstream_mail_resp = Response(201),
            upstream_rcpt_resp = Response(202),
            exp_mail_resp = Response(201),
            exp_rcpt_resp = Response(202),
            exp_sf_after_env = False,
            exp_status_after_env = None,

            upstream_data_resp = None,
            exp_sf_after_data = False,
            exp_status_after_data = None,

            upstream_data_resp_last = Response(401),
            exp_sf_after_data_last = True,
            exp_status_after_data_last = Response(250))

    def test_mx_mail_fail(self):
        self._test(
            msa=False,
            upstream_mail_resp = Response(401),
            upstream_rcpt_resp = None,
            exp_mail_resp = Response(401),
            exp_rcpt_resp = Response(401),
            exp_sf_after_env = False,
            exp_status_after_env = Response(401))

    def test_mx_rcpt_fail(self):
        self._test(
            msa=False,
            upstream_mail_resp = Response(201),
            upstream_rcpt_resp = Response(401),
            exp_mail_resp = Response(201),
            exp_rcpt_resp = Response(401),
            exp_sf_after_env = False,
            exp_status_after_env = Response(401))

    def test_mx_data_fail(self):
        self._test(
            msa=False,
            upstream_mail_resp = Response(201),
            upstream_rcpt_resp = Response(202),
            exp_mail_resp = Response(201),
            exp_rcpt_resp = Response(202),
            exp_sf_after_env = False,
            exp_status_after_env = None,
            upstream_data_resp = Response(401),
            exp_sf_after_data = False,
            exp_status_after_data = Response(401))

    def test_mx_data_last_fail(self):
        self._test(
            msa=False,
            upstream_mail_resp = Response(201),
            upstream_rcpt_resp = Response(202),
            exp_mail_resp = Response(201),
            exp_rcpt_resp = Response(202),
            exp_sf_after_env = False,
            exp_status_after_env = None,
            upstream_data_resp = None,
            exp_sf_after_data = False,
            exp_status_after_data = None,
            upstream_data_resp_last = Response(401),
            exp_sf_after_data_last = False,
            exp_status_after_data_last = Response(401))



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s [%(thread)d] '
                        '%(filename)s:%(lineno)d %(message)s')

    unittest.main()
