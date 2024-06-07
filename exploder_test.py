from typing import List, Optional

import unittest
import logging
from threading import Condition, Lock, Thread
import time

from storage import Storage, TransactionCursor
from response import Response
from fake_endpoints import FakeAsyncEndpoint
from filter import Mailbox, TransactionMetadata

from blob import CompositeBlob, InlineBlob

from exploder import Exploder, Recipient

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
        if self.mail_resp:
            endpoint.merge(TransactionMetadata(mail_response=self.mail_resp))
        if self.rcpt_resp:
            endpoint.merge(TransactionMetadata(rcpt_response=[self.rcpt_resp]))

    def set_data_response(self, i):
        if i < len(self.data_resp):
            self.endpoint.merge(
                TransactionMetadata(data_response=self.data_resp[i]))


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
    def setUp(self):
        self.mu = Lock()
        self.cv = Condition(self.mu)

        self.storage = Storage.get_sqlite_inmemory_for_test()
        self.upstream_endpoints = []

    def dump_db(self):
        for l in self.storage.db.iterdump():
            print(l)

    def add_endpoint(self):
        endpoint = FakeAsyncEndpoint(rest_id='rest-id')
        with self.mu:
            self.upstream_endpoints.append(endpoint)
            self.cv.notify_all()
        return endpoint

    def factory(self):
        endpoint = FakeAsyncEndpoint(rest_id='rest-id')
        with self.mu:
            self.cv.wait_for(lambda: self.upstream_endpoints)
            return self.upstream_endpoints.pop(0)

    def start_update(self, filter, tx, tx_delta):
        # XXX capture upstream_delta
        t = Thread(target=lambda: filter.on_update(tx, tx_delta), daemon=True)
        t.start()
        time.sleep(0.1)
        return t

    def join(self, t):
        t.join(timeout=2)
        self.assertFalse(t.is_alive())

    # xxx all tests validate response message

    def _test_one(self, msa, t : Test):
        exploder = Exploder('output-chain', lambda: self.factory(),
                            rcpt_timeout=1,
                            msa=msa,
                            default_notification={})

        for r in t.rcpt:
            endpoint = self.add_endpoint()
            r.set_endpoint(endpoint)

        tx = TransactionMetadata(mail_from=Mailbox(t.mail_from))
        exploder.on_update(tx, tx)
        self.assertEqual(tx.mail_response.code, t.expected_mail_resp.code)
        for i,r in enumerate(t.rcpt):
            tx_delta = TransactionMetadata(rcpt_to=[Mailbox(r.addr)])
            assert tx.merge_from(tx_delta)
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
            # this is cheating a little in that we're setting the
            # upstream response before sending the downstream data...
            # for higher fidelity, we would have to do the whole
            # song&dance of starting the downstream update in a thread
            # and then set the upstream response, etc.
            for r in t.rcpt:
                r.set_data_response(i)
            exploder.on_update(tx, tx_delta)
            if t.expected_data_resp[i] is not None:
                self.assertEqual(tx.data_response.code,
                                 t.expected_data_resp[i].code)
                break
            else:
                self.assertIsNone(tx.data_response)

        for i,r in enumerate(t.rcpt):
            if r.store_and_forward:
                self.assertIsNotNone(r.endpoint.tx.retry)
                self.assertIsNotNone(r.endpoint.tx.notification)
            else:
                self.assertIsNone(r.endpoint.tx.retry)
                self.assertIsNone(r.endpoint.tx.notification)

    def test_mx(self):
        for i,t in enumerate(vec_mx):
            logging.info('test_mx %d', i)
            self._test_one(False, t)

    def test_msa(self):
        for i,t in enumerate(vec_msa):
            logging.info('test_msa %d', i)
            self._test_one(True, t)

    # Several of these one-off tests exercise the multi-rcpt fan-out
    # in Exploder._on_rcpts() that is otherwise dead code until the gateway
    # implements SMTP PIPELINING
    def testSuccess(self):
        exploder = Exploder('output-chain', lambda: self.factory())

        tx = TransactionMetadata()
        tx.mail_from = Mailbox('alice')
        exploder.on_update(tx, tx)
        self.assertEqual(tx.mail_response.code, 250)

        up0 = self.add_endpoint()
        up1 = self.add_endpoint()


        tx_delta = TransactionMetadata(
            rcpt_to = [ Mailbox('bob'), Mailbox('bob2') ])
        assert tx.merge_from(tx_delta) is not None
        t = self.start_update(exploder, tx, tx_delta)

        up0.merge(TransactionMetadata(mail_response=Response(250),
                                      rcpt_response=[Response(201)]))
        up1.merge(TransactionMetadata(mail_response=Response(250),
                                      rcpt_response=[Response(202)]))

        self.join(t)

        body_blob = CompositeBlob()
        b = InlineBlob(b'hello, ')
        body_blob.append(b, 0, b.len())

        tx_delta = TransactionMetadata()
        tx_delta.body_blob = body_blob
        assert tx.merge_from(tx_delta) is not None
        t = self.start_update(exploder, tx, tx_delta)

        self.join(t)

        b = InlineBlob(b'world!')
        body_blob.append(b, 0, b.len(), True)

        # same delta: body has grown
        t = self.start_update(exploder, tx, tx_delta)

        up0.merge(TransactionMetadata(data_response=Response(250)))
        up1.merge(TransactionMetadata(data_response=Response(250)))
        self.join(t)
        logging.debug(tx.data_response.message)
        self.assertEqual(tx.data_response.code, 250)
        for endpoint in self.upstream_endpoints:
            self.assertEqual(endpoint.body_blob.read(0), b'hello, world!')

        self.assertIsNone(up0.tx.retry)
        self.assertIsNone(up0.tx.notification)
        self.assertIsNone(up1.tx.retry)
        self.assertIsNone(up1.tx.notification)

    def testMxRcptTemp(self):
        exploder = Exploder('output-chain', lambda: self.factory(),
                            rcpt_timeout=1, msa=False,
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

        t = self.start_update(exploder, tx, tx)

        up0.merge(TransactionMetadata(mail_response=Response(250)))
        up0.merge(TransactionMetadata(rcpt_response=[Response(201)]))
        up1.merge(TransactionMetadata(mail_response=Response(250)))
        up1.merge(TransactionMetadata(rcpt_response=[Response(450)]))

        self.join(t)

        self.assertEqual(tx.mail_response.code, 250)
        self.assertEqual(tx.rcpt_response[0].code, 201)
        self.assertEqual(tx.rcpt_response[1].code, 450)

        up0.merge(TransactionMetadata(data_response=Response(202)))
        up1.merge(TransactionMetadata(data_response=Response(400)))

        tx_delta = TransactionMetadata(body_blob=InlineBlob(b'hello'))
        tx.merge_from(tx_delta)
        exploder.on_update(tx, tx_delta)
        self.assertEqual(tx.data_response.code, 202)
        # first rcpt succeeded -> no retry
        # second failed at rcpt -> wasn't accepted -> no retry
        self.assertIsNone(up0.tx.retry)
        self.assertIsNone(up0.tx.notification)
        self.assertIsNone(up1.tx.retry)
        self.assertIsNone(up1.tx.notification)


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
        endpoint = FakeAsyncEndpoint(rest_id='rest-id')

        downstream_tx = TransactionMetadata(
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob')])
        downstream_delta = downstream_tx.copy()

        rcpt = Recipient('smtp-out',
                         endpoint,
                         msa=msa,
                         rcpt=downstream_tx.rcpt_to[0],
                         rcpt_timeout=1,
                         data_timeout=1)

        upstream_delta = TransactionMetadata()
        if upstream_mail_resp:
            upstream_delta.mail_response = upstream_mail_resp
        if upstream_rcpt_resp:
            upstream_delta.rcpt_response = [upstream_rcpt_resp]
        endpoint.merge(upstream_delta)

        rcpt._on_rcpt(downstream_tx, downstream_delta)
        self.assertEqual(rcpt.mail_response.code, exp_mail_resp.code)
        self.assertEqual(rcpt.rcpt_response.code, exp_rcpt_resp.code)
        self.assertEqual(rcpt.store_and_forward, exp_sf_after_env)
        self.assertEqualStatus(rcpt.status, exp_status_after_env)

        if exp_mail_resp.err() or exp_rcpt_resp.err():
            return

        if upstream_data_resp:
            endpoint.merge(TransactionMetadata(
                data_response=upstream_data_resp))

        d = 'hello, world!'
        rcpt._append_upstream(InlineBlob(d[0:7], len(d)), False)
        self.assertEqual(rcpt.store_and_forward, exp_sf_after_data)
        self.assertEqualStatus(rcpt.status, exp_status_after_data)

        if exp_status_after_data is not None and exp_status_after_data.err():
            return

        if upstream_data_resp_last:
            endpoint.merge(TransactionMetadata(
                data_response=upstream_data_resp_last))
        rcpt._append_upstream(InlineBlob(d, len(d)), True)
        self.assertEqual(rcpt.store_and_forward, exp_sf_after_data_last)
        self.assertEqualStatus(rcpt.status, exp_status_after_data_last)

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
                        format='%(asctime)s %(message)s')

    unittest.main()
