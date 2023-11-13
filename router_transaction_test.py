
from router_transaction import RouterTransaction, BlobIdMap
from blobs import BlobStorage
from blob import InlineBlob

from storage import Storage, Status as StorageStatus, Action

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
        logging.info('FakeEndpoint.append_data')
        with self.lock:
            self.cv.wait_for(lambda: self.final_resp is not None)
            return self.final_resp

    def set_final_resp(self, resp):
        with self.lock:
            self.final_resp = resp
            self.cv.notify_all()



class RouterTransactionTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

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

    def check_storage(self, id, exp_status, exp_action):
        storage_tx = self.storage.get_transaction_cursor()
        storage_tx.load(id)
        self.assertEqual(exp_status, storage_tx.status)
        actions = storage_tx.load_last_action(1)
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0][1], exp_action)

    def fast(self,
             msa : bool,
             multi_mx : bool,
             start_response : Response,
             append_response : Optional[Response],
             expected_storage_status, action,
             status_after_durable, action_after_durable):
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
        endpoint.set_start_resp(start_response)
        resp = tx.get_start_result(timeout=1)
        self.assertIsNotNone(resp)
        self.assertEqual(resp.code, start_response.code)

        if multi_mx:
            tx.set_mx_multi_rcpt()

        if start_response.ok() or msa:
            resp = tx.append_data(last=True, blob=InlineBlob(b'hello'))
        async_resp = tx.get_final_status(timeout=0)
        logging.info('s=%s as=%s', resp, async_resp)
        if msa:
            if start_response.ok():
                self.assertIsNone(resp)
            elif start_response.temp():
                self.assertIsNone(resp)
                self.assertIsNotNone(async_resp)
                self.assertEqual(async_resp.code, start_response.code)
            elif start_response.perm():
                self.assertIsNotNone(resp)
                self.assertEqual(resp.code, start_response.code)
        else:
            self.assertEqual(resp is None, async_resp is None)
            # mx start is always sync
            if start_response.err():
                self.assertIsNotNone(resp)
                self.assertEqual(resp.code, start_response.code)
            else:
                self.assertIsNone(resp)

        # we only do the upstream append if the transaction
        # hasn't already failed
        if resp is None and append_response is not None:
            endpoint.set_final_resp(append_response)

            # this may need to wait to give the append running in another
            # thread time to finish
            resp = tx.get_final_status(timeout=1)
            self.assertIsNotNone(resp)
            self.assertEqual(resp.code, append_response.code)

        self.check_storage(tx.storage_id, expected_storage_status, action)

        if status_after_durable is None:
            tx.finalize()
            return

        tx.set_durable()

        #for l in self.storage.db.iterdump():
        #    print(l)

        self.check_storage(tx.storage_id, status_after_durable,
                           action_after_durable)
        tx.finalize()


    def test_msa_fast_success(self):
        self.fast(True, False,
                  Response(),
                  Response(),
                  StorageStatus.ONESHOT_DONE, Action.DELIVERED,
                  StorageStatus.ONESHOT_DONE, Action.DELIVERED)

    def test_msa_fast_start_temp(self):
        self.fast(True, False,
                  Response(code=400),
                  None,
                  StorageStatus.ONESHOT_DONE, Action.TEMP_FAIL,
                  StorageStatus.WAITING, Action.TEMP_FAIL)

    def test_msa_fast_append_temp(self):
        self.fast(True, False,
                  Response(),
                  Response(code=400),
                  StorageStatus.ONESHOT_DONE, Action.TEMP_FAIL,
                  StorageStatus.WAITING, Action.TEMP_FAIL)

    def test_msa_fast_perm(self):
        self.fast(True, False,
                  Response(),
                  Response(code=500),
                  StorageStatus.ONESHOT_DONE, Action.PERM_FAIL,
                  StorageStatus.ONESHOT_DONE, Action.PERM_FAIL)


    # single-mx is always oneshot, never durable
    def test_mx_single_fast_success(self):
        self.fast(False, False, Response(), Response(),
                  StorageStatus.ONESHOT_DONE, Action.DELIVERED,
                  None, None)
    def test_mx_single_fast_start_temp(self):
        self.fast(False, False, Response(code=400), Response(),
                  StorageStatus.ONESHOT_DONE, Action.TEMP_FAIL,
                  None, None)
    def test_mx_single_fast_append_temp(self):
        self.fast(False, False, Response(), Response(code=400),
                  StorageStatus.ONESHOT_DONE, Action.TEMP_FAIL,
                  None, None)

    def test_mx_single_fast_perm(self):
        self.fast(False, False, Response(), Response(code=500),
                  StorageStatus.ONESHOT_DONE, Action.PERM_FAIL,
                  None, None)

    # multi-mx (from smtp gw only) is durable if the results were mixed
    def test_mx_multi_fast_success(self):
        self.fast(False, True, Response(), Response(),
                  StorageStatus.ONESHOT_DONE, Action.DELIVERED,
                  StorageStatus.ONESHOT_DONE, Action.DELIVERED)
    # mx (single and multi) never continues after upstream start err
    def test_mx_multi_fast_start_temp(self):
        self.fast(False, True, Response(code=400), None,
                  StorageStatus.ONESHOT_DONE, Action.TEMP_FAIL,
                  None, None)
    def test_mx_multi_fast_append_temp(self):
        self.fast(False, True, Response(), Response(code=400),
                  StorageStatus.ONESHOT_DONE, Action.TEMP_FAIL,
                  StorageStatus.WAITING, Action.TEMP_FAIL)

    def test_mx_multi_fast_perm(self):
        self.fast(False, True, Response(), Response(code=500),
                  StorageStatus.ONESHOT_DONE, Action.PERM_FAIL,
                  StorageStatus.ONESHOT_DONE, Action.PERM_FAIL)

    # more scenarios:
    # append concurrent with inflight upstream start
    #   msa only (always a precondition for mx)
    # set_durable concurrent with slow upstream start/append
    #   msa or multi mx (single mx always oneshot)


    def recover(self, id, start_resp, append_resp, exp_status, exp_action):
        storage_tx = self.storage.get_transaction_cursor()
        self.assertTrue(storage_tx.load(id))
        self.assertEqual(storage_tx.status, StorageStatus.WAITING)

        recovery_endpoint = FakeEndpoint()
        recovery_endpoint.set_start_resp(start_resp)
        if append_resp is not None:
            recovery_endpoint.set_final_resp(append_resp)
        recovery_tx = RouterTransaction(
            self.executor, self.storage, self.blob_id_map, self.blobs,
            next=recovery_endpoint,
            host='host', msa=True, tag=0, storage_tx=storage_tx)

        recovery_tx.load()

        self.check_storage(id, exp_status, exp_action)
        recovery_tx.finalize()

    def recovery(self, expectations):
        endpoint = FakeEndpoint()
        tx = RouterTransaction(
            self.executor, self.storage, self.blob_id_map, self.blobs,
            next=endpoint,
            host='host', msa=True, tag=0)
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

        tx.append_data(last=True, blob=InlineBlob(b'hello'))
        # we expect the result to be none so don't wait for it to be non-none
        self.assertIsNone(tx.get_final_status(timeout=0))

        endpoint.set_final_resp(Response(code=400))
        # this may need to wait to give the append running in another
        # thread time to finish
        resp = tx.get_final_status(timeout=1)
        self.assertEqual(400, resp.code)

        tx.set_durable()
        tx.finalize()

        for (start_resp, append_resp, status, action) in expectations:
            self.recover(tx.storage_id, start_resp, append_resp, status, action)


    # start_resp, data_resp, exp_status, exp_action

    def test_recovery_temp_success(self):
        expectations = [
        (Response(code=400), None, StorageStatus.WAITING, Action.TEMP_FAIL),
        (Response(), Response(code=400), StorageStatus.WAITING,
         Action.TEMP_FAIL),
        (Response(), Response(), StorageStatus.DONE, Action.DELIVERED)]
        self.recovery(expectations)

    def test_recovery_start_perm(self):
        expectations = [
        (Response(code=500), None, StorageStatus.DONE, Action.PERM_FAIL),
        ]
        self.recovery(expectations)

    def test_recovery_append_perm(self):
        expectations = [
        (Response(), Response(code=500), StorageStatus.DONE, Action.PERM_FAIL),
        ]
        self.recovery(expectations)



if __name__ == '__main__':
    unittest.main()



