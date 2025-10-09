# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import List, Optional

import unittest
import logging
import time
from functools import partial
from threading import Thread
import traceback

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
from koukan.fake_endpoints import MockAsyncFilter


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
    data_resp : Optional[Response]
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
        cursor.start_attempt()
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
            if self.data_resp and tx.body and not tx.data_response:
                assert tx.body.finalized()
                data_resp = self.data_resp
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

            rv, cloned = cursor.wait(0.5)
            # test can finish as soon as we write the last response
            try:
                if not rv or not cloned:
                    tx = cursor.load()
            except Exception:
                break
        logging.debug('output done')

    def set_endpoint(self, endpoint):
        self.endpoint = endpoint

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
    data : Optional[bytes]
    expected_mail_resp : Response
    expected_rcpt_resp : List[Response]
    expected_data_resp : Optional[Response]
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
                                  retry=True, notify=True)

    # xxx all tests validate response message

    def _test_one(self, msa, test : Test):
        logging.debug('_test_one()', stack_info=True)
        exploder = Exploder('output-chain',
                            partial(self.factory, msa),
                            rcpt_timeout=5)
        tx = TransactionMetadata()
        exploder.wire_downstream(tx)

        output_threads = []
        for r in test.rcpt:
            endpoint = self.add_endpoint()
            r.set_endpoint(endpoint)
            output_thread = Thread(
                target=partial(r.output, endpoint),
                daemon=True)
            output_thread.start()
            output_threads.append(output_thread)

        delta = TransactionMetadata(mail_from=Mailbox(test.mail_from))
        downstream_cursor = self.storage.get_transaction_cursor()
        downstream_cursor.create('downstream_rest_id', delta)
        assert exploder.downstream_tx is not None
        exploder.downstream_tx.merge_from(delta)
        exploder.on_update(delta)
        logging.debug(tx)
        assert tx.mail_response is not None
        self.assertEqual(tx.mail_response.code, test.expected_mail_resp.code)
        for i,r in enumerate(test.rcpt):
            prev = tx.copy()
            tx.rcpt_to.append(Mailbox(r.addr))
            tx_delta = prev.delta(tx)
            prev = tx.copy()
            exploder.on_update(tx_delta)
            upstream_delta = prev.delta(exploder.downstream_tx)
            def _code(r):
                assert r is not None
                return r.code
            self.assertEqual(
                [test.expected_rcpt_resp[i].code],
                [_code(rr) for rr in upstream_delta.rcpt_response])

        def code(r):
            assert r is not None
            return r.code
        self.assertEqual([rr.code for rr in test.expected_rcpt_resp],
                         [code(rr) for rr in tx.rcpt_response])

        if test.data is not None:
            delta =TransactionMetadata(body=BlobSpec(create_tx_body=True))
            downstream_cursor.write_envelope(delta)
            blob_uri = BlobUri('downstream_rest_id', tx_body=True)
            blob_writer = downstream_cursor.get_blob_for_append(blob_uri)
            blob_writer.append_data(0, test.data, last=True)
            tx_delta = TransactionMetadata(body=blob_writer)
            tx.merge_from(tx_delta)
            exploder.on_update(tx_delta)
        if test.expected_data_resp is not None:
            assert tx.data_response is not None
            self.assertEqual(test.expected_data_resp.code,
                             tx.data_response.code)
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
                [ Rcpt('bob', Response(501), Response(502), None,
                        store_and_forward=False) ],
                None,
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
                       None,
                       store_and_forward=False) ],
                None,
                Response(250),  # injected
                [Response(401)],  # upstream
                None))

    def test_mx_single_rcpt_rcpt_perm(self):
        self._test_one(
            msa=False,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(201), Response(501), None,
                       store_and_forward=False) ],
                None,
                Response(250),  # injected
                [Response(501)],  # upstream
                None,
            ))

    def test_mx_single_rcpt_rcpt_temp(self):
        self._test_one(
            msa=False,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(201), Response(401), None,
                       store_and_forward=False) ],
                None,
                Response(250),  # injected
                [Response(401)],  # upstreawm
                None))

    def test_mx_single_rcpt_data_last_perm(self):
        self._test_one(
            msa=False,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(201), Response(202),
                       Response(501),
                       store_and_forward=False) ],
                b'hello, world!',
                Response(250),  # injected
                [Response(202)],  # upstream
                Response(501),  # upstream
            ))

    def test_mx_single_rcpt_data_last_temp(self):
        self._test_one(
            msa=False,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(201), Response(202),
                       Response(401)) ],
                b'hello, world!',
                Response(250),  # injected
                [Response(202)],  # upstream
                Response(401),  # upstream
            ))

    def test_mx_single_rcpt_success(self):
        self._test_one(
            msa=False,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(201), Response(202),
                       Response(203),
                       store_and_forward=False) ],
                b'hello, world!',
                Response(250),  # injected
                [Response(202)],  # upstream
                Response(203),  # upstream
            ))

    def test_mx_multi_rcpt_success_cutthrough(self):
        self._test_one(
            msa=False,
            test=Test(
                'alice',
                [ Rcpt('bob1', Response(201), Response(202),
                       Response(203),
                       store_and_forward=False),
                  Rcpt('bob2', Response(204), Response(205),
                       Response(206),
                       store_and_forward=False) ],
                b'hello, world!',
                Response(250),  # injected
                [Response(202), Response(205)],  # upstream
                Response(203),  # upstream/cutthrough
            ))

    # first succeeds, second fails at rcpt
    def test_mx_multi_rcpt_rcpt_temp(self):
        self._test_one(
            msa=False,
            test=Test(
                'alice',
                [ Rcpt('bob1', Response(201), Response(202),
                       Response(203),
                       store_and_forward=False),
                  Rcpt('bob2', Response(204), Response(405), None,
                       store_and_forward=False) ],
                b'hello, world!',
                Response(250),  # injected
                [Response(202), Response(405)],  # upstream
                Response(203),  # injected
            ))


    def test_msa_single_rcpt_mail_perm(self):
        self._test_one(
            msa=True,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(501), None, None,
                       store_and_forward=False) ],
                None,
                Response(250),  # noop mail/injected
                [Response(501)],  # upstream
                None,
            ))

    def test_msa_single_rcpt_mail_temp(self):
        self._test_one(
            msa=True,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(401), None, None,
                       store_and_forward=True) ],
                b'hello, world!',
                Response(250),  # noop mail/injected
                [Response(250)],  # injected/upgraded
                Response(250),
            ))

    def test_msa_single_rcpt_rcpt_perm(self):
        self._test_one(
            msa=True,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(201), Response(501), None,
                       store_and_forward=False) ],
                None,
                Response(250),  # injected
                [Response(501)],  # upstream
                None,
            ))

    def test_msa_single_rcpt_rcpt_temp(self):
        self._test_one(
            msa=True,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(201), Response(401), None,
                       store_and_forward=True) ],
                b'hello, world!',
                Response(250),  # injected
                [Response(250)],  # injected/upgraded
                Response(250),
            ))

    def test_msa_single_rcpt_data_last_perm(self):
        self._test_one(
            msa=True,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(201), Response(202),
                       Response(501),
                       store_and_forward=False) ],
                b'hello, world!',
                Response(250),  # injected
                [Response(202)],  # upstream
                Response(501),  # upstream
            ))

    def test_msa_single_rcpt_data_last_temp(self):
        self._test_one(
            msa=True,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(201), Response(202),
                       Response(401),
                       store_and_forward=True) ],
                b'hello, world!',
                Response(250),  # injected
                [Response(202)],  # upstream
                Response(250),  # injected/upgraded
            ))

    def test_msa_single_rcpt_success(self):
        self._test_one(
            msa=True,
            test=Test(
                'alice',
                [ Rcpt('bob', Response(201), Response(202),
                       Response(203),
                       store_and_forward=False) ],
                b'hello, world!',
                Response(250),  # injected
                [Response(202)],  # upstream
                Response(203),  # upstream
            ))

    # first recipient succeeds, second permfails after MAIL
    def test_msa_multi_rcpt_mail_perm(self):
        self._test_one(
            msa=True,
            test=Test(
                'alice',
                [ Rcpt('bob1', Response(201), Response(202),
                       Response(203),
                       store_and_forward=False),
                  Rcpt('bob2', Response(501), Response(502), None,
                       store_and_forward=False)],
                b'hello, world!',
                Response(250),  # injected
                [Response(202), Response(502)],
                Response(203),  # same data resp
            ))

    # first recipient succeeds, second permfails after RCPT
    def test_msa_multi_rcpt_rcpt_perm(self):
        self._test_one(
            msa=True,
            test=Test(
                'alice',
                [ Rcpt('bob1', Response(201), Response(202),
                       Response(203),
                       store_and_forward=False),
                  Rcpt('bob2', Response(201), Response(501), None,
                       store_and_forward=False)],
                b'hello, world!',
                Response(250),  # injected
                [Response(202), Response(501)],  # upstream
                Response(203),  # same data resp
            ))

    # first recipient succeeds, second permfails after last data
    def test_msa_multi_rcpt_data_last_perm(self):
        self._test_one(
            msa=True,
            test=Test(
                'alice',
                [ Rcpt('bob1', Response(201), Response(202),
                       Response(203),
                       store_and_forward=False),
                  Rcpt('bob2', Response(204), Response(205),
                       Response(501),
                       store_and_forward=True)],
                b'hello, world!',
                Response(250),  # injected
                [Response(202), Response(205)],  # upstream
                Response(250),  # 'async mixed upstream'
            ))

    # all rcpts tempfail data -> s&f
    def test_msa_multi_rcpt_data_last_temp(self):
        self._test_one(
            msa=True,
            test=Test(
                'alice',
                [ Rcpt('bob1', Response(201), Response(202),
                       Response(401),
                       store_and_forward=True),
                  Rcpt('bob2', Response(204), Response(205),
                       Response(402),
                       store_and_forward=True)],
                b'hello, world!',
                Response(250),  # injected
                [Response(202), Response(205)],  # upstream
                Response(250),  # same response s&f
            ))

    def test_upstream_busy(self):
        exploder = Exploder('output-chain',
                            lambda: None,
                            rcpt_timeout=5)
        tx = TransactionMetadata()
        exploder.wire_downstream(tx)
        delta = TransactionMetadata(mail_from=Mailbox('alice'),
                                    rcpt_to=[Mailbox('bob')])
        tx.merge_from(delta)
        exploder.on_update(delta)
        self.assertEqual(250, tx.mail_response.code)
        self.assertEqual(1, len(tx.rcpt_response))
        self.assertEqual(451, tx.rcpt_response[0].code)

    def test_partial_body(self):
        upstream = MockAsyncFilter()
        exploder = Exploder('output-chain',
                            lambda: upstream,
                            rcpt_timeout=5)
        tx = TransactionMetadata()
        exploder.wire_downstream(tx)

        def exp_update(tx, delta):
            self.assertFalse(tx.body.finalized())
            prev = tx.copy()
            tx.mail_response=Response(201)
            tx.rcpt_response=[Response(202)]
            return prev.delta(tx), 2

        upstream.expect_update(exp_update)

        body = b'hello, world'

        prev = tx.copy()
        tx.mail_from = Mailbox('alice')
        tx.rcpt_to = [Mailbox('bob')]
        tx.body = InlineBlob(body)
        exploder.on_update(prev.delta(tx))
        self.assertEqual(250, tx.mail_response.code)
        self.assertEqual([202], [r.code for r in tx.rcpt_response])
        self.assertIsNone(tx.data_response)

        def exp_body(tx, delta):
            self.assertTrue(tx.body.finalized())
            prev = tx.copy()
            tx.data_response=Response(203)
            return prev.delta(tx), 3
        upstream.expect_update(exp_body)

        body += b'!'
        tx.body = InlineBlob(body, last=True)
        exploder.on_update(TransactionMetadata(body=tx.body))
        self.assertEqual(250, tx.mail_response.code)
        self.assertEqual([202], [r.code for r in tx.rcpt_response])
        self.assertEqual(203, tx.data_response.code)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s [%(thread)d] '
                        '%(filename)s:%(lineno)d %(message)s')

    unittest.main()
