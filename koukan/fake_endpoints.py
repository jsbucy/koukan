# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Callable, Dict, List, Optional, Tuple
from threading import Lock, Condition
import logging
import time

from koukan.blob import Blob, WritableBlob
from koukan.response import Response, Esmtp
from koukan.filter import (
    AsyncFilter,
    Mailbox,
    SyncFilter,
    TransactionMetadata )
from koukan.filter_chain import Filter

from koukan.storage_schema import VersionConflictException

UpdateExpectation = Callable[[TransactionMetadata,TransactionMetadata],
                             Optional[TransactionMetadata]]
GetExpectation = Callable[[],Optional[TransactionMetadata]]
class MockAsyncFilter(AsyncFilter):
    update_expectation : List[UpdateExpectation]
    get_expectation : List[GetExpectation]
    body : Optional[WritableBlob] = None
    blob : Dict[str, WritableBlob]
    _version : Optional[int] = None
    _incremental : Optional[bool] = None

    def __init__(self, incremental=None):
        self.update_expectation = []
        self.get_expectation = []
        self.blob = {}
        self._incremental = incremental

    def expect_update(self, exp : UpdateExpectation):
        self.update_expectation.append(exp)
    def expect_get(self, tx : TransactionMetadata):
        self.get_expectation.append(lambda: tx)
    def expect_get_cb(self, exp : GetExpectation):
        self.get_expectation.append(exp)

    def update(self,
               tx : TransactionMetadata,
               tx_delta : TransactionMetadata
               ) -> TransactionMetadata:
        logging.debug('MockAsyncFilter.update %s', tx)
        exp = self.update_expectation[0]
        self.update_expectation.pop(0)
        upstream_delta = exp(tx, tx_delta)
        assert upstream_delta is not None
        self._version = tx.version
        return upstream_delta

    def get(self) -> TransactionMetadata:
        cb = self.get_expectation[0]
        self.get_expectation.pop(0)
        tx = cb()
        assert tx is not None
        self._version = tx.version
        return tx

    def get_blob_writer(
            self,
            create : bool,
            blob_rest_id : Optional[str] = None,
            tx_body : Optional[bool] = None
    ) -> Optional[WritableBlob]:
        if tx_body:
            return self.body
        #assert blob_rest_id and blob_rest_id in self.blob
        return self.blob[blob_rest_id]

    def version(self) -> Optional[int]:
        return self._version

    def wait(self, version, timeout) -> bool:
        return bool(self.get_expectation)

    async def wait_async(self, version, timeout) -> bool:
        return bool(self.get_expectation)

    def incremental(self):
        if self._incremental is None:
            raise NotImplementedError()
        return self._incremental

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
        if not self.expectation:
            raise IndexError()
        exp = self.expectation[0]
        self.expectation.pop(0)
        upstream_delta = exp(tx, tx_delta)
        assert upstream_delta is not None
        return upstream_delta

class FakeFilter(Filter):
    expectation : List[Expectation]

    def __init__(self):
        self.expectation = []

    def add_expectation(self, exp : Expectation):
        self.expectation.append(exp)

    async def on_update(self, tx_delta : TransactionMetadata,
                        upstream):  # unused
        if not self.expectation:
            raise IndexError()
        exp = self.expectation[0]
        self.expectation.pop(0)
        exp(self.downstream, tx_delta)
