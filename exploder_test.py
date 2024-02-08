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
        t.join(timeout=1)
        self.assertFalse(t.is_alive())

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
        up1.set_mail_response(Response(250))
        up1.add_rcpt_response(Response(202))

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
        self.assertEqual(tx.data_response.code, 250)
        for endpoint in self.upstream_endpoints:
            self.assertEqual(endpoint.body_blob.read(0), b'hello, world!')


    def testMsaRcptTimeout(self):
        exploder = Exploder('output-chain', lambda: self.factory(),
                            rcpt_timeout=1, msa=True)

        tx = TransactionMetadata(
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob')])

        up0 = self.add_endpoint()
        t = self.start_update(exploder, tx)
        self.join(t)

        self.assertEqual(tx.mail_response.code, 250)
        self.assertEqual(tx.rcpt_response[0].code, 250)

        up0.add_data_response(None)

        tx = TransactionMetadata(body_blob=InlineBlob(b'hello'))
        exploder.on_update(tx)
        self.assertEqual(tx.data_response.code, 250)

    def testMxRcptTemp(self):
        exploder = Exploder('output-chain', lambda: self.factory(),
                            rcpt_timeout=1, msa=False)

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

        up0.add_data_response(None)

        tx = TransactionMetadata(body_blob=InlineBlob(b'hello'))
        exploder.on_update(tx)
        self.assertEqual(tx.data_response.code, 250)

    def testDataTimeout(self):
        exploder = Exploder('output-chain', lambda: self.factory(),
                            rcpt_timeout=1, data_timeout=1, msa=True)

        tx = TransactionMetadata(
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob'), Mailbox('bob2')])

        up0 = self.add_endpoint()
        up1 = self.add_endpoint()


        t = self.start_update(exploder, tx)

        up0.set_mail_response(Response(250))
        up0.add_rcpt_response(Response(201))
        up1.set_mail_response(Response(250))
        up1.add_rcpt_response(Response(202))

        self.join(t)

        up0.add_data_response(Response(250))
        #up1.add_data_response(Response(250))
        tx = TransactionMetadata(body_blob=InlineBlob(b'hello'))
        exploder.on_update(tx)
        self.assertEqual(tx.data_response.code, 250)


if __name__ == '__main__':
    unittest.main()
