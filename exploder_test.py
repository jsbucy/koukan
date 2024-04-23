from typing import List, Optional

import unittest
import logging
from threading import Condition, Lock, Thread
import time

from storage import Storage, TransactionCursor
from response import Response
from fake_endpoints import SyncEndpoint
from filter import Mailbox, TransactionMetadata

from blob import CompositeBlob, InlineBlob

from exploder import Exploder
#from storage_writer_filter import StorageWriterFilter

from fake_endpoints import SyncEndpoint

class Rcpt:
    addr : str
    mail_resp : Optional[Response]
    rcpt_resp : Optional[Response]
    data_resp : Optional[Response]

    def __init__(self, addr, m=None, r=None, d=None):
        self.addr = addr
        self.mail_resp = m
        self.rcpt_resp = r
        self.data_resp = d

    def set_endpoint(self, endpoint):
        if self.mail_resp:
            endpoint.set_mail_response(self.mail_resp)
        if self.rcpt_resp:
            endpoint.add_rcpt_response(self.rcpt_resp)
        for r in self.data_resp:
            endpoint.add_data_response(r)

class Test:
    mail_from : str
    rcpt : List[Rcpt]
    data : List[bytes]
    expected_mail_resp : Response
    expected_rcpt_resp : List[Response]
    expected_data_resp : List[Response]
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
        [ Rcpt('bob', Response(501), Response(502), []) ],
        [],
        Response(250),  # noop mail/injected
        [Response(501)],  # upstream
        None,
    ),

    # mail temp
    Test(
        'alice',
        [ Rcpt('bob', Response(401), Response(500), []) ],
        [],
        Response(250),  # injected
        [Response(401)],  # upstream
        None,
    ),

    # mail success, rcpt perm
    Test(
        'alice',
        [ Rcpt('bob', Response(201), Response(501), []) ],
        [],
        Response(250),  # injected
        [Response(501)],  # upstream
        None,
    ),

    # mail success, rcpt temp
    Test(
        'alice',
        [ Rcpt('bob', Response(201), Response(401), []) ],
        [],
        Response(250),  # injected
        [Response(401)],  # upstreawm
        None,
    ),

    # mail, rcpt success, data !last perm
    Test(
        'alice',
        [ Rcpt('bob', Response(201), Response(202), [Response(501)]) ],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202)],  # upstream
        [Response(501)],  # upstream
    ),

    # mail, rcpt success, data !last temp
    Test(
        'alice',
        [ Rcpt('bob', Response(201), Response(202), [Response(401)]) ],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202)],  # upstream
        [Response(401)],  # upstream
    ),

    # mail, rcpt success, data !last success, last perm
    Test(
        'alice',
        [ Rcpt('bob', Response(201), Response(202), [None, Response(501)]) ],
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
        [ Rcpt('bob', Response(201), Response(202), [None, Response(203)]) ],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202)],  # upstream
        [None, Response(203)],  # upstream
    ),
]

vec_msa = [
    # single recipient
    # mail perm
    Test(
        'alice',
        [ Rcpt('bob', Response(501), Response(502), []) ],
        [],
        Response(250),  # noop mail/injected
        [Response(501)],  # upstream
        None,
    ),

    # mail temp
    Test(
        'alice',
        [ Rcpt('bob', Response(401), Response(500), []) ],
        [],
        Response(250),  # noop mail/injected
        [Response(250)],  # injected/upgraded
        None,
    ),

    # mail success, rcpt perm
    Test(
        'alice',
        [ Rcpt('bob', Response(201), Response(501), []) ],
        [],
        Response(250),  # injected
        [Response(501)],  # upstream
        None,
    ),

    # mail success, rcpt temp
    Test(
        'alice',
        [ Rcpt('bob', Response(201), Response(401), []) ],
        [],
        Response(250),  # injected
        [Response(250)],  # injected/upgraded
        None,
    ),

    # mail, rcpt success, data !last perm
    Test(
        'alice',
        [ Rcpt('bob', Response(201), Response(202), [Response(501)]) ],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202)],  # upstream
        [Response(501)],  # upstream
    ),

    # mail, rcpt success, data !last temp
    Test(
        'alice',
        [ Rcpt('bob', Response(201), Response(202), [Response(401)]) ],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202)],  # upstream
        [None, Response(250)],  # injected/upgraded
    ),

    # mail, rcpt success, data !last ok, last perm
    Test(
        'alice',
        [ Rcpt('bob', Response(201), Response(202), [None, Response(501)]) ],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202)],  # upstream
        [None, Response(501)],  # upstream
    ),

    # mail, rcpt success, data !last ok, last temp
    Test(
        'alice',
        [ Rcpt('bob', Response(201), Response(202), [None, Response(401)]) ],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202)],  # upstream
        [None, Response(250)],  # injected/upgraded
    ),

    # mail, rcpt, data success
    Test(
        'alice',
        [ Rcpt('bob', Response(201), Response(202), [None, Response(203)]) ],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202)],  # upstream
        [None, Response(203)],  # upstream
    ),

    # multi-rcpt
    # first recipient succeeds, second permfails after MAIL
    Test(
        'alice',
        [ Rcpt('bob1', Response(201), Response(202), [None, Response(203)]),
          Rcpt('bob2', Response(501), Response(502), [])],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202), Response(501)],  # upstream mail err -> rcpt resp
        [None, Response(250)],  # 'async mixed upstream'
    ),

    # multi-rcpt
    # first recipient succeeds, second permfails after RCPT
    Test(
        'alice',
        [ Rcpt('bob1', Response(201), Response(202), [None, Response(203)]),
          Rcpt('bob2', Response(201), Response(501), [])],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202), Response(501)],  # upstream
        [None, Response(250)],  # 'async mixed upstream'
    ),

    # multi-rcpt
    # first recipient succeeds, second permfails after !last data
    Test(
        'alice',
        [ Rcpt('bob1', Response(201), Response(202), [None, Response(203)]),
          Rcpt('bob2', Response(204), Response(205), [Response(501)])],
        [b'hello, ', b'world!'],
        Response(250),  # injected
        [Response(202), Response(205)],  # upstream
        [None, Response(250)],  # 'async mixed upstream'
    ),

    # multi-rcpt
    # first recipient succeeds, second permfails after last data
    Test(
        'alice',
        [ Rcpt('bob1', Response(201), Response(202), [None, Response(203)]),
          Rcpt('bob2', Response(204), Response(205), [None, Response(501)])],
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

        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

        self.storage = Storage.get_sqlite_inmemory_for_test()
        self.upstream_endpoints = []

    def dump_db(self):
        for l in self.storage.db.iterdump():
            print(l)

    def add_endpoint(self):
        endpoint = SyncEndpoint()
        with self.mu:
            self.upstream_endpoints.append(endpoint)
            self.cv.notify_all()
        return endpoint

    def factory(self):
        endpoint = SyncEndpoint()
        with self.mu:
            self.cv.wait_for(lambda: self.upstream_endpoints)
            return self.upstream_endpoints.pop(0)

    def start_update(self, filter, tx):
        t = Thread(target=lambda: filter.on_update(tx), daemon=True)
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
                            msa=msa)

        for r in t.rcpt:
            endpoint = self.add_endpoint()
            r.set_endpoint(endpoint)

        tx = TransactionMetadata(mail_from=Mailbox(t.mail_from))
        exploder.on_update(tx)
        self.assertEqual(tx.mail_response.code, t.expected_mail_resp.code)
        for i,r in enumerate(t.rcpt):
            tx = TransactionMetadata(rcpt_to=[Mailbox(r.addr)])
            exploder.on_update(tx)
            self.assertEqual([rr.code for rr in tx.rcpt_response],
                             [t.expected_rcpt_resp[i].code])

        blob = None
        content_length = 0
        for d in t.data:
            content_length += len(d)

        for i,d in enumerate(t.data):
            if blob is None:
                blob = InlineBlob(d, content_length=content_length)
            else:
                blob.append(d)
            tx = TransactionMetadata(body_blob=blob)
            exploder.on_update(tx)
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
        for i,t in enumerate(vec_msa):
            logging.info('test_mx %d', i)
            self._test_one(True, t)

    # Several of these one-off tests exercise the multi-rcpt fan-out
    # in Exploder._on_rcpts() that is otherwise dead code until the gateway
    # implements SMTP PIPELINING
    def testSuccess(self):
        exploder = Exploder('output-chain', lambda: self.factory())

        tx = TransactionMetadata()
        tx.mail_from = Mailbox('alice')
        exploder.on_update(tx)
        self.assertEqual(tx.mail_response.code, 250)

        tx = TransactionMetadata()
        tx.rcpt_to = [ Mailbox('bob'), Mailbox('bob2') ]

        up0 = self.add_endpoint()
        up1 = self.add_endpoint()

        t = self.start_update(exploder, tx)

        up0.set_mail_response(Response(250))
        up0.add_rcpt_response(Response(201))
        up0.add_data_response(None)
        up1.set_mail_response(Response(250))
        up1.add_rcpt_response(Response(202))
        up1.add_data_response(None)

        self.join(t)

        body_blob = CompositeBlob()
        b = InlineBlob(b'hello, ')
        body_blob.append(b, 0, b.len())
        tx = TransactionMetadata()
        tx.body_blob = body_blob

        t = self.start_update(exploder, tx)

        self.join(t)

        b = InlineBlob(b'world!')
        body_blob.append(b, 0, b.len(), True)

        t = self.start_update(exploder, tx)

        up0.add_data_response(Response(250))
        up1.add_data_response(Response(250))
        self.join(t)
        logging.debug(tx.data_response.message)
        self.assertEqual(tx.data_response.code, 250)
        for endpoint in self.upstream_endpoints:
            self.assertEqual(endpoint.body_blob.read(0), b'hello, world!')

    def testMxRcptTemp(self):
        exploder = Exploder('output-chain', lambda: self.factory(),
                            rcpt_timeout=1, msa=False,
                            default_notifications={'host': 'smtp-out'})

        # xxx need to add rcpts in multiple updates to catch index bug
        tx = TransactionMetadata(
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob'),
                       Mailbox('bob2')])

        up0 = self.add_endpoint()
        up1 = self.add_endpoint()

        t = self.start_update(exploder, tx)

        up0.set_mail_response(Response(250))
        up0.add_rcpt_response(Response(201))
        up1.set_mail_response(Response(250))
        up1.add_rcpt_response(Response(450))

        self.join(t)

        self.assertEqual(tx.mail_response.code, 250)
        self.assertEqual(tx.rcpt_response[0].code, 201)
        self.assertEqual(tx.rcpt_response[1].code, 450)

        up0.add_data_response(Response(202))
        up1.add_data_response(Response(400))

        tx = TransactionMetadata(body_blob=InlineBlob(b'hello'))
        exploder.on_update(tx)
        self.assertEqual(tx.data_response.code, 250)
        # validate that we set notifications on the one that tempfailed
        # and didn't on the one that succeeded
        self.assertIsNone(up0.tx.notifications)
        self.assertEqual(up1.tx.notifications['host'], 'smtp-out')


if __name__ == '__main__':
    unittest.main()
