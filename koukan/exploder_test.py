# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import List, Optional

import unittest
import logging
import time
from functools import partial
from threading import Thread

from koukan.deadline import Deadline
from koukan.storage import Storage, TransactionCursor
from koukan.response import Response
from koukan.filter import AsyncFilter, Mailbox, TransactionMetadata
from koukan.storage_writer_filter import StorageWriterFilter
from koukan.storage_schema import BlobSpec, VersionConflictException

from koukan.blob import CompositeBlob, InlineBlob

from koukan.exploder import Exploder, Recipient

import koukan.postgres_test_utils as postgres_test_utils

from koukan.async_filter_wrapper import AsyncFilterWrapper

from koukan.rest_schema import BlobUri

import traceback

def setUpModule():
    postgres_test_utils.setUpModule()

def tearDownModule():
    postgres_test_utils.tearDownModule()

next_rest_id = 0
def rest_id_factory():
    global next_rest_id
    rest_id = next_rest_id
    next_rest_id += 1
    return 'rest_id%d' % rest_id

class Rcpt:
    addr : str
    mail_resp : Optional[Response]
    rcpt_resp : Optional[Response]
    data_resp : List[Optional[Response]]
    store_and_forward : bool
    endpoint : Optional[AsyncFilter] = None

    def __init__(self, addr,  # rcpt_to
                 m=None, r=None, d=None,
                 store_and_forward=False):
        self.addr = addr
        self.mail_resp = m
        self.rcpt_resp = r
        self.data_resp = d
        self.store_and_forward = store_and_forward

    # ~fake OutputHandler for upstream
    def output(self, endpoint):
        logging.debug('output start')
        self.cursor = cursor = endpoint.release_transaction_cursor()
        cursor.load(start_attempt=True)
        logging.debug(cursor.attempt_id)
        tx = cursor.tx
        while True:
            logging.debug(tx)
            env_delta = TransactionMetadata()
            if self.mail_resp and tx.mail_from and not tx.mail_response:
                env_delta.mail_response=self.mail_resp
            if self.rcpt_resp and tx.rcpt_to and not tx.rcpt_response:
                env_delta.rcpt_response=[self.rcpt_resp]
            data_resp = None
            if self.data_resp and tx.body and tx.body.finalized() and not tx.data_response:
                data_resp = self.data_resp[-1]
                env_delta.data_response = data_resp

            if env_delta:
                logging.debug(env_delta)
                try:
                    # finalize_attempt=True if data_resp ??
                    cursor.write_envelope(env_delta)
                except VersionConflictException:
                    logging.debug('VersionConflictException')
                    time.sleep(0.3)
                    cursor.load()
                err = False
                for r in [self.mail_resp, self.rcpt_resp, data_resp]:
                    if (r is not None) and r.err():
                        err = True
                        break
                if err or data_resp:
                    break

            while tx.body and not tx.body.finalized():
                logging.debug('poll body')
                time.sleep(0.5)
                tx.body.load()

            cursor.wait(0.5)
            # test can finish as soon as we write the last response
            try:
                tx = cursor.load()
            except Exception:
                break
        logging.debug('output done')

    def set_endpoint(self, endpoint):
        self.endpoint = endpoint

    # discussion:
    # AsyncFilter.update() is supposed to return immediately so you
    # should not expect to get upstream responses from it; only after
    # wait() and get(). Moreover Exploder.on_update() will not
    # wait/get if !body.finalized() since !tx.req_inflight(). So
    # the "early data error" tests here aren't a perfect analogue of what
    # would happen in practice: you would get the previous error in
    # response to the next update. Though this is probably all moot anyway
    # since something downstream probably buffers the whole blob and
    # you will only see one update with the finalized blob.
    def set_data_response(self, parent, i : int, last : bool):
        logging.debug('Rcpt.set_data_response %d %d', i, len(self.data_resp))

    def check_store_and_forward(self):
        sf = self.store_and_forward
        tx = self.cursor.load()
        if sf != (tx.retry is not None):
            return False
        if sf != (tx.notification is not None):
            return False
        return True

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


class ExploderTest(unittest.TestCase):
    def setUp(self):
        self.pg, self.storage_url = postgres_test_utils.setup_postgres()
        self.storage = Storage.connect(self.storage_url, 'http://session_uri')
        self.upstream_endpoints = []

    def tearDown(self):
        self.storage._del_session()

    def add_endpoint(self):
        endpoint = StorageWriterFilter(
            self.storage, rest_id_factory=rest_id_factory, create_leased=True)
        self.upstream_endpoints.append(endpoint)
        return endpoint

    def factory(self, store_and_forward):
        logging.debug('%s', store_and_forward)
        notify = {} if store_and_forward else None
        return AsyncFilterWrapper(self.upstream_endpoints.pop(0),
                                  timeout=5,
                                  store_and_forward=store_and_forward,
                                  default_notification=notify)

    # xxx all tests validate response message

    def _test_one(self, msa, test : Test):
        logging.debug('_test_one()', stack_info=True)
        exploder = Exploder('output-chain',
                            partial(self.factory, msa),
                            rcpt_timeout=5,
                            default_notification={})

        output_threads = []
        for r in test.rcpt:
            endpoint = self.add_endpoint()
            r.set_endpoint(endpoint)
            output_thread = Thread(
                target=partial(r.output, endpoint),
                daemon=True)
            output_thread.start()
            output_threads.append(output_thread)

        tx = TransactionMetadata(mail_from=Mailbox(test.mail_from))
        downstream_cursor = self.storage.get_transaction_cursor()
        downstream_cursor.create('downstream_rest_id', tx)

        exploder.on_update(tx, tx.copy())
        self.assertEqual(tx.mail_response.code, test.expected_mail_resp.code)
        for i,r in enumerate(test.rcpt):
            updated = tx.copy()
            updated.rcpt_to.append(Mailbox(r.addr))
            tx_delta = tx.delta(updated)
            tx = updated
            upstream_delta = exploder.on_update(tx, tx_delta)
            self.assertEqual(
                [test.expected_rcpt_resp[i].code],
                [rr.code for rr in upstream_delta.rcpt_response])

        self.assertEqual([rr.code for rr in test.expected_rcpt_resp],
                         [rr.code for rr in tx.rcpt_response])

        if test.data:
            downstream_cursor.write_envelope(
                TransactionMetadata(body=BlobSpec(create_tx_body=True)))
            blob_uri = BlobUri('downstream_rest_id', tx_body=True)
            blob_writer = downstream_cursor.get_blob_for_append(blob_uri)
        for i,d in enumerate(test.data):
            last = i == (len(test.data) - 1)
            content_length = blob_writer.len() + len(d) if last else None
            blob_writer.append_data(blob_writer.len(), d, content_length)
            # xxx OutputHandler only invokes chain with finalized body
            if not last:
                continue
            tx_delta = TransactionMetadata(body=blob_writer)
            tx.merge_from(tx_delta)
            for r in test.rcpt:
                r.set_data_response(self, i, last)
            exploder.on_update(tx, tx_delta)
            if test.expected_data_resp[i] is not None:
                self.assertEqual(test.expected_data_resp[i].code,
                                 tx.data_response.code)
                break
            else:
                self.assertIsNone(tx.data_response)

        for t in output_threads:
            logging.debug('join %s', t)
            t.join()

        for r in test.rcpt:
            self.assertTrue(r.check_store_and_forward())

    def test_mx_single_rcpt_mail_perm(self):
        self._test_one(
            msa=False,
            test=Test('alice',
                 [ Rcpt('bob', Response(501), Response(502), [],
                        store_and_forward=False) ],
                 [],
                 Response(250),  # noop mail/injected
                 [Response(502)],  # upstream
                 None))

    # rcpt perm after mail temp doesn't make sense?
    def test_mx_single_rcpt_mail_temp(self):
        self._test_one(
            msa=False,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(401), None, #Response(402),
                       [],
                       store_and_forward=False) ],
                [],
                Response(250),  # injected
                [Response(401)],  # upstream
                None))

    def test_mx_single_rcpt_rcpt_perm(self):
        self._test_one(
            msa=False,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(201), Response(501), [],
                       store_and_forward=False) ],
                [],
                Response(250),  # injected
                [Response(501)],  # upstream
                None,
            ))

    def test_mx_single_rcpt_rcpt_temp(self):
        self._test_one(
            msa=False,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(201), Response(401), [],
                       store_and_forward=False) ],
                [],
                Response(250),  # injected
                [Response(401)],  # upstreawm
                None))

    # xxx early data resp
    def disabled_test_mx_single_rcpt_data_perm(self):
        self._test_one(
            msa=False,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(201), Response(202), [Response(501)],
                       store_and_forward=False) ],
                [b'hello, ', b'world!'],
                Response(250),  # injected
                [Response(202)],  # upstream
                [Response(501)],  # upstream
            ))

    # xxx early data response
    def disabled_test_mx_single_rcpt_data_temp(self):
        self._test_one(
            msa=False,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(201), Response(202), [Response(401)],
                       store_and_forward=False) ],
                [b'hello, ', b'world!'],
                Response(250),  # injected
                [Response(202)],  # upstream
                [Response(401)],  # upstream
            ))

    def test_mx_single_rcpt_data_last_perm(self):
        self._test_one(
            msa=False,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(201), Response(202),
                       [None, Response(501)],
                       store_and_forward=False) ],
                [b'hello, ', b'world!'],
                Response(250),  # injected
                [Response(202)],  # upstream
                [None, Response(501)],  # upstream
            ))

    def test_mx_single_rcpt_data_last_temp(self):
        self._test_one(
            msa=False,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(201), Response(202),
                       [None, Response(401)]) ],
                [b'hello, ', b'world!'],
                Response(250),  # injected
                [Response(202)],  # upstream
                [None, Response(401)],  # upstream
            ))

    def test_mx_single_rcpt_success(self):
        self._test_one(
            msa=False,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(201), Response(202),
                       [None, Response(203)],
                       store_and_forward=False) ],
                [b'hello, ', b'world!'],
                Response(250),  # injected
                [Response(202)],  # upstream
                [None, Response(203)],  # upstream
            ))

    def test_mx_multi_rcpt_success_cutthrough(self):
        self._test_one(
            msa=False,
            test=Test(
                'alice',
                [ Rcpt('bob1', Response(201), Response(202),
                       [None, Response(203)],
                       store_and_forward=False),
                  Rcpt('bob2', Response(204), Response(205),
                       [None, Response(206)],
                       store_and_forward=False) ],
                [b'hello, ', b'world!'],
                Response(250),  # injected
                [Response(202), Response(205)],  # upstream
                [None, Response(203)],  # upstream/cutthrough
            ))

    # first succeeds, second fails at rcpt
    def test_mx_multi_rcpt_rcpt_temp(self):
        self._test_one(
            msa=False,
            test=Test(
                'alice',
                [ Rcpt('bob1', Response(201), Response(202),
                       [None, Response(203)],
                       store_and_forward=False),
                  Rcpt('bob2', Response(204), Response(405), [],
                       store_and_forward=False) ],
                [b'hello, ', b'world!'],
                Response(250),  # injected
                [Response(202), Response(405)],  # upstream
                [None, Response(203)],  # injected
            ))


    def test_msa_single_rcpt_mail_perm(self):
        self._test_one(
            msa=True,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(501), None, [],
                       store_and_forward=False) ],
                [],
                Response(250),  # noop mail/injected
                [Response(501)],  # upstream
                None,
            ))

    def test_msa_single_rcpt_mail_temp(self):
        self._test_one(
            msa=True,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(401), None, [],
                       store_and_forward=True) ],
                [b'hello, world!'],
                Response(250),  # noop mail/injected
                [Response(250)],  # injected/upgraded
                [Response(250)],
            ))

    def test_msa_single_rcpt_rcpt_perm(self):
        self._test_one(
            msa=True,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(201), Response(501), [],
                       store_and_forward=False) ],
                [],
                Response(250),  # injected
                [Response(501)],  # upstream
                None,
            ))

    def test_msa_single_rcpt_rcpt_temp(self):
        self._test_one(
            msa=True,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(201), Response(401), [],
                       store_and_forward=True) ],
                [b'hello, world!'],
                Response(250),  # injected
                [Response(250)],  # injected/upgraded
                [Response(250)],
            ))

    # xxx early data resp
    def disabled_test_msa_single_rcpt_data_perm(self):
        self._test_one(
            msa=True,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(201), Response(202), [Response(501)],
                       store_and_forward=False) ],
                [b'hello, ', b'world!'],
                Response(250),  # injected
                [Response(202)],  # upstream
                [Response(501)],  # upstream
            ))

    def test_msa_single_rcpt_data_temp(self):
        self._test_one(
            msa=True,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(201), Response(202), [Response(401)],
                       store_and_forward=True) ],
                [b'hello, ', b'world!'],
                Response(250),  # injected
                [Response(202)],  # upstream
                [None, Response(250)],  # injected/upgraded
            ))

    def test_msa_single_rcpt_data_last_perm(self):
        self._test_one(
            msa=True,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(201), Response(202),
                       [None, Response(501)],
                       store_and_forward=False) ],
                [b'hello, ', b'world!'],
                Response(250),  # injected
                [Response(202)],  # upstream
                [None, Response(501)],  # upstream
            ))

    def test_msa_single_rcpt_data_last_temp(self):
        self._test_one(
            msa=True,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(201), Response(202),
                       [None, Response(401)],
                       store_and_forward=True) ],
                [b'hello, ', b'world!'],
                Response(250),  # injected
                [Response(202)],  # upstream
                [None, Response(250)],  # injected/upgraded
            ))

    def test_msa_single_rcpt_success(self):
        self._test_one(
            msa=True,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(201), Response(202),
                       [None, Response(203)],
                       store_and_forward=False) ],
                [b'hello, ', b'world!'],
                Response(250),  # injected
                [Response(202)],  # upstream
                [None, Response(203)],  # upstream
            ))

    # first recipient succeeds, second permfails after MAIL
    def test_msa_multi_rcpt_mail_perm(self):
        self._test_one(
            msa=True,
            test=Test(
                'alice',
                [ Rcpt('bob1', Response(201), Response(202),
                       [None, Response(203)],
                       store_and_forward=False),
                  Rcpt('bob2', Response(501), Response(502), [],
                       store_and_forward=False)],
                [b'hello, ', b'world!'],
                Response(250),  # injected
                [Response(202), Response(502)],
                [None, Response(203)],  # same data resp
            ))

    # first recipient succeeds, second permfails after RCPT
    def test_msa_multi_rcpt_rcpt_perm(self):
        self._test_one(
            msa=True,
            test=Test(
                'alice',
                [ Rcpt('bob1', Response(201), Response(202),
                       [None, Response(203)],
                       store_and_forward=False),
                  Rcpt('bob2', Response(201), Response(501), [],
                       store_and_forward=False)],
                [b'hello, ', b'world!'],
                Response(250),  # injected
                [Response(202), Response(501)],  # upstream
                [None, Response(203)],  # same data resp
            ))

    # first recipient succeeds, second permfails after !last data
    # early data resp
    def disabled_test_msa_multi_rcpt_data_perm(self):
        self._test_one(
            msa=True,
            test=Test(
                'alice',
                [ Rcpt('bob1', Response(201), Response(202),
                       [None, Response(203)],
                       store_and_forward=False),
                  Rcpt('bob2', Response(204), Response(205),
                       [Response(501)],
                       store_and_forward=True)],
                [b'hello, ', b'world!'],
                Response(250),  # injected
                [Response(202), Response(205)],  # upstream
                [None, Response(250)],  # 'async mixed upstream'
            ))

    # first recipient succeeds, second permfails after last data
    def test_msa_multi_rcpt_data_last_perm(self):
        self._test_one(
            msa=True,
            test=Test(
                'alice',
                [ Rcpt('bob1', Response(201), Response(202),
                       [None, Response(203)],
                       store_and_forward=False),
                  Rcpt('bob2', Response(204), Response(205),
                       [None, Response(501)],
                       store_and_forward=True)],
                [b'hello, ', b'world!'],
                Response(250),  # injected
                [Response(202), Response(205)],  # upstream
                [None, Response(250)],  # 'async mixed upstream'
            ))

    # all rcpts tempfail data -> s&f
    def test_msa_multi_rcpt_data_last_temp(self):
        self._test_one(
            msa=True,
            test=Test(
                'alice',
                [ Rcpt('bob1', Response(201), Response(202),
                       [None, Response(401)],
                       store_and_forward=True),
                  Rcpt('bob2', Response(204), Response(205),
                       [None, Response(402)],
                       store_and_forward=True)],
                [b'hello, ', b'world!'],
                Response(250),  # injected
                [Response(202), Response(205)],  # upstream
                [None, Response(250)],  # same response s&f
            ))

    def test_upstream_busy(self):
        exploder = Exploder('output-chain',
                            lambda: None,
                            rcpt_timeout=5,
                            default_notification={})
        tx = TransactionMetadata(mail_from=Mailbox('alice'),
                                 rcpt_to=[Mailbox('bob')])
        upstream_delta = exploder.on_update(tx, tx.copy())
        self.assertEqual(250, tx.mail_response.code)
        self.assertEqual(1, len(tx.rcpt_response))
        self.assertEqual(451, tx.rcpt_response[0].code)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s [%(thread)d] '
                        '%(filename)s:%(lineno)d %(message)s')

    unittest.main()
