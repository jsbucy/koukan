from typing import Callable, List, Optional, Tuple
from threading import Lock, Condition
import logging
import time

from blob import Blob, WritableBlob
from response import Response, Esmtp
from filter import (
    AsyncFilter,
    Mailbox,
    SyncFilter,
    TransactionMetadata )
from rest_schema import BlobUri

class FakeAsyncEndpoint(AsyncFilter):
    tx : TransactionMetadata
    mu : Lock
    cv : Condition
    _version : int = 0
    rest_id : str
    body_blob : Optional[WritableBlob] = None

    def __init__(self, rest_id):
        self.tx = TransactionMetadata()
        self.mu = Lock()
        self.cv = Condition(self.mu)
        self.rest_id = rest_id

    def merge(self, tx):
        with self.mu:
            assert self.tx.merge_from(tx) is not None
            self._version += 1
            self.cv.notify_all()

    # AsyncFilter
    def update(self,
               tx : TransactionMetadata,
               delta : TransactionMetadata,
               timeout : Optional[float] = None
               ) -> Optional[TransactionMetadata]:
        with self.mu:
            assert self.tx.merge_from(delta)
            self._version += 1
            self.cv.notify_all()
            self.cv.wait_for(lambda: not self.tx.req_inflight(), timeout)
            upstream_delta = tx.delta(self.tx)
            assert upstream_delta is not None
            tx.replace_from(self.tx)
            return upstream_delta

    # AsyncFilter
    def get(self, timeout : Optional[float] = None
            ) -> Optional[TransactionMetadata]:
        with self.mu:
            self.cv.wait_for(lambda: not self.tx.req_inflight(), timeout)
            return self.tx.copy()


    def get_blob_writer(self,
                        create : bool,
                        blob_rest_id : Optional[str] = None,
                        tx_body : Optional[bool] = None,
                        copy_from_tx_body : Optional[str] = None
                        ) -> Optional[WritableBlob]:
        #assert not(tx_body and blob_rest_id)
        return self.body_blob

    def version(self):
        return self._version


Expectation = Callable[[TransactionMetadata,TransactionMetadata],
                       Optional[TransactionMetadata]]
class FakeSyncFilter(SyncFilter):
    expectation : List[Expectation]

    def __init__(self):
        self.expectation = []

    def add_expectation(self, exp : Expectation):
        self.expectation.append(exp)

    # SyncFilter
    def on_update(self,
                  tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:
        exp = self.expectation[0]
        self.expectation.pop(0)
        upstream_delta = exp(tx, tx_delta)
        assert upstream_delta is not None
        return upstream_delta
