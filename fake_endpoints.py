from typing import Callable, Dict, List, Optional, Tuple
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

from storage_schema import VersionConflictException

UpdateExpectation = Callable[[TransactionMetadata,TransactionMetadata],
                             Optional[TransactionMetadata]]
class MockAsyncFilter(AsyncFilter):
    update_expectation : List[UpdateExpectation]
    get_expectation : List[TransactionMetadata]
    body_blob : Optional[WritableBlob] = None
    blob : Dict[str, WritableBlob]

    def __init__(self):
        self.update_expectation = []
        self.get_expectation = []
        self.blob = {}

    def expect_update(self, exp : UpdateExpectation):
        self.update_expectation.append(exp)
    def expect_get(self, tx : TransactionMetadata):
        self.get_expectation.append(tx)

    def update(self,
               tx : TransactionMetadata,
               tx_delta : TransactionMetadata
               ) -> TransactionMetadata:
        logging.debug('MockAsyncFilter.update %s', tx)
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
        if tx_body:
            return self.body_blob
        #assert blob_rest_id and blob_rest_id in self.blob
        return self.blob[blob_rest_id]

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
