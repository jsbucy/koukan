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
    TransactionMetadata )
from koukan.filter_chain import FilterResult, Filter

from koukan.storage_schema import VersionConflictException

# tx, version
State = Tuple[Optional[TransactionMetadata], int]

UpdateExpectation = Callable[[TransactionMetadata,TransactionMetadata],
                             State]
GetExpectation = Callable[[], State]
CheckExpectation = Callable[[], Optional[AsyncFilter.CheckTxResult]]

class MockAsyncFilter(AsyncFilter):
    update_expectation : List[UpdateExpectation]
    get_expectation : List[GetExpectation]
    check_expectation : List[CheckExpectation]
    check_cache_expectation : List[CheckExpectation]
    body : Optional[WritableBlob] = None
    blob : Dict[str, WritableBlob]
    _version : Optional[int] = None
    _incremental : Optional[bool] = None

    def __init__(self, incremental=None):
        self.update_expectation = []
        self.get_expectation = []
        self.check_expectation = []
        self.check_cache_expectation = []
        self.blob = {}
        self._incremental = incremental

    def expect_update(self, exp : UpdateExpectation):
        self.update_expectation.append(exp)
    def expect_get(self, state : State):
        self.get_expectation.append(lambda: state)
    def expect_get_cb(self, exp : GetExpectation):
        self.get_expectation.append(exp)

    def update(self,
               tx : TransactionMetadata,
               tx_delta : TransactionMetadata
               ) -> TransactionMetadata:
        logging.debug('MockAsyncFilter.update %s', tx)
        exp = self.update_expectation[0]
        self.update_expectation.pop(0)
        upstream_delta, version = exp(tx, tx_delta)
        assert upstream_delta is not None
        self._version = version
        logging.debug(upstream_delta)
        return upstream_delta

    def get(self) -> TransactionMetadata:
        cb = self.get_expectation[0]
        self.get_expectation.pop(0)
        tx, version = cb()
        assert tx is not None
        self._version = version
        logging.debug(tx)
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
        assert blob_rest_id is not None
        return self.blob[blob_rest_id]

    @property
    def version(self) -> Optional[int]:
        return self._version

    def wait(self, version, timeout
             ) -> Tuple[bool, Optional[TransactionMetadata]]:
        if not self.get_expectation:
            return False, None
        return True, self.get()
        #return bool(self.get_expectation)

    async def wait_async(self, version, timeout)  -> Tuple[bool, Optional[TransactionMetadata]]:
        #return bool(self.get_expectation)
        return self.wait(version, timeout)

    def incremental(self):
        if self._incremental is None:
            raise NotImplementedError()
        return self._incremental

    def _check(self, exp):
        if not exp:
            return None
        cb = exp.pop(0)
        return cb()

    def check_cache(self):
        return self._check(self.check_cache_expectation)

    def check(self):
        return self._check(self.check_expectation)

Expectation = Callable[[TransactionMetadata,TransactionMetadata],
                       Optional[TransactionMetadata]]
class FakeFilter(Filter):
    expectation : List[Expectation]

    def __init__(self):
        self.expectation = []

    def add_expectation(self, exp : Expectation):
        self.expectation.append(exp)

    def on_update(self, tx_delta : TransactionMetadata) -> FilterResult:
        if not self.expectation:
            raise IndexError()
        exp = self.expectation[0]
        self.expectation.pop(0)
        assert self.downstream_tx is not None
        exp(self.downstream_tx, tx_delta)
        return FilterResult()
