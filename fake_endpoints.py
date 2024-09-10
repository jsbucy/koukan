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
            upstream_delta = tx.delta(self.tx)
            assert upstream_delta is not None
            tx.replace_from(self.tx)
            upstream_delta.version = tx.version = self._version
            return upstream_delta

    # AsyncFilter
    def get(self, timeout : Optional[float] = None
            ) -> Optional[TransactionMetadata]:
        with self.mu:
            tx = self.tx.copy()
            tx.version = self._version
            return tx

    def get_blob_writer(self,
                        create : bool,
                        blob_rest_id : Optional[str] = None,
                        tx_body : Optional[bool] = None
                        ) -> Optional[WritableBlob]:
        #assert not(tx_body and blob_rest_id)
        return self.body_blob

    def version(self):
        return self._version

    def wait(self, version, timeout) -> bool:
        with self.mu:
            return self.cv.wait_for(lambda: self._version != version, timeout)

    async def wait_async(self, timeout) -> bool:
        raise NotImplementedError()


UpdateExpectation = Callable[[TransactionMetadata,TransactionMetadata],
                             Optional[TransactionMetadata]]
class MockAsyncFilter(AsyncFilter):
    update_expectation : List[UpdateExpectation]
    get_expectation : List[TransactionMetadata]

    def __init__(self):
        self.update_expectation = []
        self.get_expectation = []

    def expect_update(self, exp : UpdateExpectation):
        self.update_expectation.append(exp)
    def expect_get(self, tx : TransactionMetadata):
        self.get_expectation.append(tx)

    def update(self,
               tx : TransactionMetadata,
               tx_delta : TransactionMetadata
               ) -> TransactionMetadata:
        exp = self.update_expectation[0]
        self.update_expectation.pop(0)
        upstream_delta = exp(tx, tx_delta)
        assert upstream_delta is not None
        return upstream_delta

    def get(self) -> TransactionMetadata:
        tx = self.get_expectation[0]
        self.get_expectation.pop(0)
        return tx

    def get_blob_writer(
            self,
            create : bool,
            blob_rest_id : Optional[str] = None,
            tx_body : Optional[bool] = None
    ) -> Optional[WritableBlob]:
        raise NotImplementedError()

    def version(self) -> Optional[int]:
        pass

    def wait(self, version, timeout) -> bool:
        return bool(self.get_expectation)

    async def wait_async(self, version, timeout) -> bool:
        return bool(self.get_expectation)


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
