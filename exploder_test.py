from typing import List, Optional

import unittest
import logging
from threading import Condition, Lock, Thread
import time

from storage import Action, Storage, TransactionCursor
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

    def testBasic(self):
        exploder = Exploder('output-chain', lambda: self.factory())

        tx = TransactionMetadata()
        tx.mail_from = Mailbox('alice')
        exploder.on_update(tx)
        self.assertEqual(tx.mail_response.code, 250)

        tx = TransactionMetadata()
        tx.rcpt_to = [Mailbox('bob')]

        t = Thread(target=lambda: self.update(exploder, tx))
        t.start()
        time.sleep(0.1)

        with self.mu:
            self.cv.wait_for(lambda: len(self.upstream_endpoints) > 0)
        self.upstream_endpoints[0].add_rcpt_response(Response(201))

        t.join(timeout=1)

        rresp = []
        t = Thread(target=lambda: self.append(
            rresp, exploder, True, InlineBlob('hello')))
        t.start()
        time.sleep(0.1)

        self.upstream_endpoints[0].add_data_response(Response(250))
        t.join(timeout=1)

        self.assertEqual(rresp[0].code, 250)

if __name__ == '__main__':
    unittest.main()
