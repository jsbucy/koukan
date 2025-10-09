# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union )

from abc import ABC, abstractmethod
import logging
import time
from threading import Condition, Lock
import json
import asyncio
from functools import partial

from urllib.parse import urljoin

from werkzeug.datastructures import ContentRange
import werkzeug.http

from fastapi import (
    Request as FastApiRequest,
    Response as FastApiResponse )
from fastapi.responses import (
    JSONResponse as FastApiJsonResponse,
    PlainTextResponse )

HttpRequest = FastApiRequest
HttpResponse = FastApiResponse

from httpx import Client, Response as HttpxResponse

from koukan.deadline import Deadline
from koukan.response import Response as MailResponse
from koukan.blob import Blob, InlineBlob, WritableBlob

from koukan.rest_service_handler import Handler, HandlerFactory
from koukan.filter import (
    AsyncFilter,
    TransactionMetadata,
    WhichJson )
from koukan.filter_chain import FilterChain
from koukan.executor import Executor

from koukan.rest_schema import BlobUri, make_blob_uri, make_tx_uri, parse_blob_uri
from koukan.version_cache import IdVersion
from koukan.storage_schema import VersionConflictException


# runs SyncFilter on Executor with AsyncFilter interface for
# RestHandler -> SyncFilterAdapter -> SmtpEndpoint

# TODO I'm unhappy with how complicated this has become. I have
# considered writing a memory-backed TransactionCursor and running
# SmtpEndpoint on OutputHandler instead. In the past I didn't think
# that would actually be simpler than this but now I'm not so sure...
class SyncFilterAdapter(AsyncFilter):
    class BlobWriter(WritableBlob):
        # all accessed with parent.mu
        parent : "SyncFilterAdapter"
        offset : int = 0
        # queue of staged appends, these are propagated to
        # parent.body in _update_once()
        q : List[bytes]
        content_length : Optional[int] = None
        def __init__(self, parent):
            self.parent = parent
            self.q = []

        def len(self):
            return self.offset

        def append_data(self, offset : int, d : bytes,
                        content_length : Optional[int] = None
                        ) -> Tuple[bool, int, Optional[int]]:
            with self.parent.mu:
                # flow control: don't buffer multiple chunks from downstream
                # TODO this can probably be further simplified
                self.parent.cv.wait_for(lambda: not(self.q))
                assert self.content_length is None or (
                    content_length == self.content_length)
                if self.content_length is not None and (
                        self.offset + len(d) > self.content_length):
                    return False, self.offset, self.content_length
                if offset != self.offset:
                    return False, self.offset, None
                self.q.append(d)
                self.offset += len(d)
                self.content_length = content_length
            self.parent._blob_wakeup()
            return True, self.offset, content_length

    executor : Executor
    chain : FilterChain
    prev_tx : TransactionMetadata
    tx : TransactionMetadata
    mu : Lock
    cv : Condition
    inflight : bool = False
    rest_id : str
    _last_update : float
    blob_writer : Optional[BlobWriter] = None
    body : Optional[InlineBlob] = None
    # transaction has reached a final status: data response or cancelled
    # (used for gc)
    done : bool = False
    id_version : IdVersion

    def __init__(self, executor : Executor, chain : FilterChain, rest_id : str):
        self.executor = executor
        self.mu = Lock()
        self.cv = Condition(self.mu)
        self.chain = chain
        self.rest_id = rest_id
        self._last_update = time.monotonic()
        self.prev_tx = TransactionMetadata()
        self.tx = TransactionMetadata()
        self.tx.rest_id = rest_id
        self.id_version = IdVersion(db_id=1, rest_id='1', version=1)
        self.chain.init(TransactionMetadata())

    def incremental(self):
        return True

    def idle(self, now : float, ttl : float, done_ttl : float):
        with self.mu:
            t = done_ttl if self.done else ttl
            idle = now - self._last_update
            timedout = idle > t
            if self.inflight:
                if timedout:
                    logging.warning('maybe stuck %s idle %d',
                                    self.rest_id, idle)
                return False
            return timedout

    @property
    def version(self):
        return self.id_version.get()

    def wait(self, version, timeout):
        return self.id_version.wait(
            version=version, timeout=timeout), None

    async def wait_async(self, version, timeout):
        return await self.id_version.wait_async(
            version=version, timeout=timeout), None

    def _update(self):
        with self.mu:
            try:
                while self._update_once():
                    pass
            except Exception:
                logging.exception('SyncFilterAdapter._update_once() %s',
                                  self.rest_id)
                # The upstream SyncFilter is supposed to return tx
                # error responses and not throw
                # exceptions. i.e. SmtpEndpoint is supposed to convert
                # all smtplib/socket exceptions to error responses but there
                # may be bugs.
                # TODO this should probably self.filter = None
                # and downstream fastfail
                self.tx.fill_inflight_responses(MailResponse(
                    450, 'internal error: unexpected exception in '
                    'SyncFilterAdapter'))
                raise
            finally:
                self.inflight = False
                self.cv.notify_all()

    def _update_once(self):
        assert self.inflight
        delta = self.prev_tx.delta(self.tx)  # new reqs
        logging.debug('SyncFilterAdapter._update_once() '
                      'downstream_delta %s staged %s', delta,
                      len(self.blob_writer.q) if self.blob_writer else None)

        # propagate staged appends from blob_writer to body
        dequeued = 0
        if self.blob_writer is not None and self.blob_writer.q:
            # body goes in delta if it changed
            delta.body = self.body
            for b in self.blob_writer.q:
                last = (self.blob_writer.content_length is not None and
                        (self.body.len() + len(b) ==
                         self.blob_writer.content_length))
                self.body.append(b, last)
                dequeued += len(b)
                logging.debug('append %d %s', len(b), self.body)
            self.blob_writer.q = []

        if not delta and not dequeued:
            return False
        self.chain.tx.merge_from(delta)
        self.mu.release()
        try:
            upstream_delta = self.chain.update()
        finally:
            self.mu.acquire()
        if self.body is not None:
            self.body.trim_front(self.body.len())

        logging.debug('SyncFilterAdapter._update_once() '
                      'tx after upstream %s', self.chain.tx)
        self.tx.merge_from(upstream_delta)
        self.prev_tx = self.tx.copy()

        # TODO closer to req_inflight() logic i.e. tx has reached
        # a final state due to an error
        self.done = self.tx.cancelled or self.tx.data_response is not None
        version = self.id_version.get()
        version += 1
        self.cv.notify_all()
        self.id_version.update(version=version, leased=True)
        self._last_update = time.monotonic()
        return True

    def update(self,
               tx : TransactionMetadata,
               tx_delta : TransactionMetadata
               ) -> Optional[TransactionMetadata]:
        if tx.retry is not None or tx.notification is not None:
            err = TransactionMetadata()
            tx.fill_inflight_responses(
                MailResponse(500, 'internal err/invalid transaction fields'),
                err)
            tx.merge_from(err)
            return err

        with self.mu:
            version = self.id_version.get()

            # try this non-destructively to see if the delta is valid...
            if self.tx.merge(tx_delta) is None:
                # bad delta, xxx this should throw an exception distinct
                # from VersionConflictException, cannot make forward progress
                return None
            # ... before committing to self.tx
            self.tx.merge_from(tx_delta)
            logging.debug('SyncFilterAdapter.updated merged %s', self.tx)

            version = self.id_version.get()
            version += 1
            self.id_version.update(version, leased=True)

            if not self.inflight:
                fut = self.executor.submit(lambda: self._update(), 0)
                # TODO we need a better way to report this error but
                # throwing here will -> http 500
                assert fut is not None
                self.inflight = True
            # no longer waits for inflight
            prev = tx.copy()
            tx.rest_id = self.rest_id
            return prev.delta(tx)

    def get(self) -> Optional[TransactionMetadata]:
        with self.mu:
            return self.tx.copy()

    def get_blob_writer(self,
                        create : bool,  # vestigal
                        blob_rest_id : Optional[str] = None,
                        tx_body : Optional[bool] = None,
                        ) -> Optional[WritableBlob]:
        if not tx_body:
            raise NotImplementedError()
        if create and self.blob_writer is None:
            with self.mu:
                assert self.body is None
                assert self.blob_writer is None
                self.body = InlineBlob(b'')
                self.body.blob_uri = BlobUri(self.rest_id, tx_body=True)
                self.tx.body = self.body
                self.blob_writer = SyncFilterAdapter.BlobWriter(self)
        return self.blob_writer


    def _blob_wakeup(self):
        logging.debug('SyncFilterAdapter.blob_wakeup %s',
                      [len(b) for b in self.blob_writer.q])
        tx = self.get()
        assert tx is not None
        # shenanigans: empty update, _update_once() will dequeue from
        # BlobWriter.q to self.body
        tx_delta = TransactionMetadata()
        self.update(tx, tx_delta)

    def check_cache(self) -> Optional[AsyncFilter.CheckTxResult]:
        tx = self.get()
        if tx is None:
            return None
        return self.version, tx, True, None


    def check(self) -> Optional[AsyncFilter.CheckTxResult]:
        raise NotImplementedError()
