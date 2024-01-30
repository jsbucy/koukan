from typing import List, Optional

import unittest
import logging
from threading import Condition, Lock, Thread
import time

from storage import Storage, TransactionCursor
from response import Response
from fake_endpoints import SyncEndpoint
from filter import Mailbox, TransactionMetadata

from blob import InlineBlob

from exploder import Exploder
#from storage_writer_filter import StorageWriterFilter

from fake_endpoints import SyncEndpoint

class ExploderTest(unittest.TestCase):
    def setUp(self):
        self.mu = Lock()
        self.cv = Condition(self.mu)

        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

        self.storage = Storage()
        self.storage.connect(db=Storage.get_inmemory_for_test())
        self.upstream_endpoints = []

    def dump_db(self):
        for l in self.storage.db.iterdump():
            print(l)

    def factory(self):
        endpoint = SyncEndpoint()
        with self.mu:
            self.upstream_endpoints.append(endpoint)
            self.cv.notify_all()
        return endpoint

    def update(self, filter, tx):
        filter.on_update(tx)

    def append(self, rresp, filter, last, blob):
        rresp.append(filter.append_data(last, blob))

    def testSuccess(self):
        exploder = Exploder('output-chain', lambda: self.factory())

        tx = TransactionMetadata()
        tx.mail_from = Mailbox('alice')
        exploder.on_update(tx)
        self.assertEqual(tx.mail_response.code, 250)

        tx = TransactionMetadata()
        tx.rcpt_to = [ Mailbox('bob'), Mailbox('bob2') ]

        t = Thread(target=lambda: self.update(exploder, tx))
        t.start()

        with self.mu:
            self.cv.wait_for(lambda: len(self.upstream_endpoints) == 1)
        self.upstream_endpoints[0].set_mail_response(Response(250))
        self.upstream_endpoints[0].add_rcpt_response(Response(201))
        with self.mu:
            self.cv.wait_for(lambda: len(self.upstream_endpoints) == 2)
        self.upstream_endpoints[1].set_mail_response(Response(250))
        self.upstream_endpoints[1].add_rcpt_response(Response(202))

        t.join(timeout=1)

        rresp = []
        t = Thread(target=lambda: self.append(
            rresp, exploder, False, InlineBlob(b'hello, ')))
        t.start()

        self.upstream_endpoints[0].add_data_response(None)
        self.upstream_endpoints[1].add_data_response(None)
        t.join(timeout=1)
        self.assertIsNone(rresp[0])

        rresp = []
        t = Thread(target=lambda: self.append(
            rresp, exploder, True, InlineBlob(b'world!')))
        t.start()

        self.upstream_endpoints[0].add_data_response(Response(250))
        self.upstream_endpoints[1].add_data_response(Response(250))
        t.join(timeout=1)
        self.assertEqual(rresp[0].code, 250)
        for endpoint in self.upstream_endpoints:
            self.assertEqual(endpoint.get_data(), b'hello, world!')

    def testMsaRcptTimeout(self):
        exploder = Exploder('output-chain', lambda: self.factory(),
                            rcpt_timeout=1, msa=True)

        tx = TransactionMetadata(
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob')])

        t = Thread(target=lambda: self.update(exploder, tx))
        t.start()
        t.join(timeout=2)

        self.assertEqual(tx.mail_response.code, 250)
        self.assertEqual(tx.rcpt_response[0].code, 250)

        self.upstream_endpoints[0].add_data_response(None)

        resp = exploder.append_data(last=True, blob=InlineBlob(b'hello'))
        self.assertEqual(resp.code, 250)

    def testMxRcptTemp(self):
        exploder = Exploder('output-chain', lambda: self.factory(),
                            rcpt_timeout=1, msa=False)

        tx = TransactionMetadata(
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob'),
                       Mailbox('bob2')])

        t = Thread(target=lambda: self.update(exploder, tx))
        t.start()

        with self.mu:
            self.cv.wait_for(lambda: len(self.upstream_endpoints) == 1)
        self.upstream_endpoints[0].set_mail_response(Response(250))
        self.upstream_endpoints[0].add_rcpt_response(Response(201))
        with self.mu:
            self.cv.wait_for(lambda: len(self.upstream_endpoints) == 2)
        self.upstream_endpoints[1].set_mail_response(Response(250))
        self.upstream_endpoints[1].add_rcpt_response(Response(450))

        t.join(timeout=2)

        self.assertEqual(tx.mail_response.code, 250)
        self.assertEqual(tx.rcpt_response[0].code, 201)
        self.assertEqual(tx.rcpt_response[1].code, 450)

        self.upstream_endpoints[0].add_data_response(None)

        resp = exploder.append_data(last=True, blob=InlineBlob(b'hello'))
        self.assertEqual(resp.code, 250)

    def testDataTimeout(self):
        exploder = Exploder('output-chain', lambda: self.factory(),
                            rcpt_timeout=1, data_timeout=1, msa=True)

        tx = TransactionMetadata(
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob'), Mailbox('bob2')])

        t = Thread(target=lambda: self.update(exploder, tx))
        t.start()
        time.sleep(0.1)

        with self.mu:
            self.cv.wait_for(lambda: len(self.upstream_endpoints) == 1)
        self.upstream_endpoints[0].set_mail_response(Response(250))
        self.upstream_endpoints[0].add_rcpt_response(Response(201))
        with self.mu:
            self.cv.wait_for(lambda: len(self.upstream_endpoints) == 2)
        self.upstream_endpoints[1].set_mail_response(Response(250))
        self.upstream_endpoints[1].add_rcpt_response(Response(202))

        t.join(timeout=1)

        self.upstream_endpoints[0].add_data_response(Response(250))
        #self.upstream_endpoints[1].add_data_response(Response(250))
        resp = exploder.append_data(last=True, blob=InlineBlob(b'hello'))
        self.assertEqual(resp.code, 250)

    def testDataEarlyResp(self):
        exploder = Exploder('output-chain', lambda: self.factory(),
                            rcpt_timeout=1, data_timeout=1, msa=True)

        tx = TransactionMetadata(
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob'), Mailbox('bob2')])

        t = Thread(target=lambda: self.update(exploder, tx))
        t.start()
        time.sleep(0.1)

        with self.mu:
            self.cv.wait_for(lambda: len(self.upstream_endpoints) == 1)
        self.upstream_endpoints[0].set_mail_response(Response(250))
        self.upstream_endpoints[0].add_rcpt_response(Response(201))
        with self.mu:
            self.cv.wait_for(lambda: len(self.upstream_endpoints) == 2)
        self.upstream_endpoints[1].set_mail_response(Response(250))
        self.upstream_endpoints[1].add_rcpt_response(Response(202))

        t.join(timeout=1)

        self.upstream_endpoints[0].add_data_response(None)
        self.upstream_endpoints[1].add_data_response(Response(450))
        resp = exploder.append_data(last=False, blob=InlineBlob(b'hello, '))

        self.upstream_endpoints[0].add_data_response(Response(250))
        resp = exploder.append_data(last=True, blob=InlineBlob(b'world!'))

        self.assertEqual(resp.code, 250)
        for endpoint in self.upstream_endpoints:
            self.assertEqual(endpoint.get_data(), b'hello, world!')


if __name__ == '__main__':
    unittest.main()
