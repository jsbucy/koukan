
from router_transaction import RouterTransaction, BlobIdMap
from blobs import BlobStorage
from blob import InlineBlob

from storage import Storage, Status as StorageStatus

from executor import Executor

from threading import Lock, Condition

from response import Response

import unittest
import logging
import time

from typing import Optional

class FakeEndpoint:
    def __init__(self):
        self.lock = Lock()
        self.cv = Condition(self.lock)
        self.start_resp = None
        self.final_resp = None

    def start(self, local_host, remote_host,
              mail_from, transaction_esmtp,
              rcpt_to, rcpt_esmtp) -> Response:
        with self.lock:
            self.cv.wait_for(lambda: self.start_resp is not None)
            return self.start_resp

    def set_start_resp(self, resp):
        with self.lock:
            self.start_resp = resp
            self.cv.notify_all()

    def append_data(self, last, blob) -> Response:
        with self.lock:
            self.cv.wait_for(lambda: self.final_resp is not None)
            return self.final_resp

    def set_final_resp(self, resp):
        with self.lock:
            self.final_resp = resp
            self.cv.notify_all()



class RouterTransactionTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)

        self.storage = Storage()
        self.storage.connect(db=Storage.get_inmemory_for_test())
        self.blobs = BlobStorage()
        self.blob_id_map = BlobIdMap()

        self.executor = Executor(100, {0: 100})


    # RouterTransaction api surface to rest is
    # start()
    # get_start_result()
    # append(last?)
    # get_final_status()
    # set_durable()
    # set_mx_multi_rcpt()

    def fast(self,
             msa : bool,
             multi_mx : bool,
             append_response : Response,
             expected_response_code : Optional[int],
             expected_storage_status,
             status_after_durable):
        endpoint = FakeEndpoint()
        tx = RouterTransaction(
            self.executor, self.storage, self.blob_id_map, self.blobs,
            next=endpoint,
            host='host', msa=msa, tag=0)
        tx.generate_rest_id()
        logging.info('%d %s', tx.storage_id, tx.rest_id)
        tx.start('local_host', 'remote_host',
                 'alice@example.com', None,
                 'bob@example.com', None)
        self.assertIsNone(tx.get_start_result(timeout=0))
        endpoint.set_start_resp(Response())
        resp = tx.get_start_result(timeout=1)
        self.assertIsNotNone(resp)
        self.assertTrue(resp.ok())

        if multi_mx:
            tx.set_mx_multi_rcpt()

        tx.append_data(last=True, blob=InlineBlob(b'hello'))
        self.assertIsNone(tx.get_final_status())
        endpoint.set_final_resp(append_response)
        resp = tx.get_final_status(timeout=1)
        if expected_response_code is not None:
            self.assertIsNotNone(resp)
            self.assertEqual(resp.code, expected_response_code)
        else:
            self.assertIsNone(resp)

        storage_tx = self.storage.get_transaction_cursor()
        self.assertTrue(storage_tx.load(tx.storage_id))
        self.assertEqual(storage_tx.status, expected_storage_status)

        if status_after_durable is None: return

        tx.set_durable()

        #for l in self.storage.db.iterdump():
        #    print(l)

        storage_tx = self.storage.get_transaction_cursor()
        self.assertTrue(storage_tx.load(tx.storage_id))
        self.assertEqual(storage_tx.status, status_after_durable)


    def test_msa_fast_success(self):
        self.fast(True, False, Response(), 200,
                  StorageStatus.ONESHOT_DONE,
                  StorageStatus.ONESHOT_DONE)
    def test_msa_fast_temp(self):
        self.fast(True, False, Response(code=400), None,
                  StorageStatus.INSERT,
                  StorageStatus.WAITING)
    def test_msa_fast_perm(self):
        self.fast(True, False, Response(code=500), 500,
                  StorageStatus.ONESHOT_DONE,
                  StorageStatus.ONESHOT_DONE)

    # single-mx is always oneshot, never durable
    def test_mx_single_fast_success(self):
        self.fast(False, False, Response(), 200,
                  StorageStatus.ONESHOT_DONE,
                  None)
    def test_mx_single_fast_temp(self):
        self.fast(False, False, Response(code=400), 400,
                  StorageStatus.ONESHOT_DONE,
                  None)
    def test_mx_single_fast_perm(self):
        self.fast(False, False, Response(code=500), 500,
                  StorageStatus.ONESHOT_DONE,
                  None)

    # multi-mx (from smtp gw only) is durable if the results were mixed
    def test_mx_multi_fast_success(self):
        self.fast(False, True, Response(), 200,
                  StorageStatus.ONESHOT_DONE,
                  StorageStatus.ONESHOT_DONE)
    def test_mx_multi_fast_temp(self):
        self.fast(False, True, Response(code=400), 400,
                  StorageStatus.ONESHOT_DONE,
                  StorageStatus.WAITING)
    def test_mx_multi_fast_perm(self):
        self.fast(False, True, Response(code=500), 500,
                  StorageStatus.ONESHOT_DONE,
                  StorageStatus.ONESHOT_DONE)

    # more scenarios:
    # append concurrent with slow upstream start
    #   msa only (always a precondition for mx)
    # set_durable concurrent with slow upstream start/append
    #   msa or multi mx (single mx always oneshot)

if __name__ == '__main__':
    unittest.main()



