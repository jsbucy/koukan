from typing import Optional
import logging

from koukan.filter import (
    AsyncFilter,
    SyncFilter,
    TransactionMetadata )
from koukan.deadline import Deadline
from koukan.storage_schema import VersionConflictException
from koukan.response import Response
from koukan.backoff import backoff

# AsyncFilterWrapper provdes store&forward business logic on top of
# StorageWriterFilter for Exploder and related workflows
# (AddRouteFilter). This consists of ~4 things:
# 1: version conflict retries
# 2: populate a temp error response for upstream timeout
# 3: populate "smtp precondition" failure responses e.g. rcpt fails if
# mail failed, etc.
# 4: if store-and-forward is enabled, upgrades upstream temp errs to
# success and enables retries/notifications on the upstream transaction.
class AsyncFilterWrapper(AsyncFilter, SyncFilter):
    filter : AsyncFilter
    timeout : float
    store_and_forward : bool
    tx : Optional[TransactionMetadata] = None
    default_notification : Optional[dict] = None
    do_store_and_forward : bool = False
    retry_params : Optional[dict] = None

    def __init__(self, filter : AsyncFilter,
                 timeout : float,
                 store_and_forward : bool = False,
                 default_notification : Optional[dict] = None,
                 retry_params : Optional[dict] = None):
        self.filter = filter
        self.timeout = timeout
        self.store_and_forward = store_and_forward
        self.default_notification = default_notification
        self.retry_params = retry_params if retry_params else {}

    def get_blob_writer(
            self,
            create : bool,
            blob_rest_id : Optional[str] = None,
            tx_body : Optional[bool] = None):
        raise NotImplementedError()

    def wait(self, version : int, timeout : float) -> bool:
        if rv := self.filter.wait(version, timeout):
            self.tx = None
        else:
            timeout_resp = Response(
                450, 'upstream timeout (AsyncFilterWrapper)')
            assert self.tx is not None
            self.tx.fill_inflight_responses(timeout_resp)

        return rv

    async def wait_async(self, version : int, timeout : float) -> bool:
        return await self.filter.wait_async(version, timeout)

    def version(self) -> Optional[int]:
        return self.filter.version()

    def update(self, tx : TransactionMetadata,
               tx_delta : TransactionMetadata
               ) -> Optional[TransactionMetadata]:
        tx_orig = tx.copy()
        upstream_tx : TransactionMetadata = tx.copy()
        for i in range(0,5):
            try:
                # StorageWriterFilter write body_blob -> body (placeholder)
                if upstream_tx.body:
                    del upstream_tx.body

                upstream_delta = self.filter.update(upstream_tx, tx_delta)
                break
            except VersionConflictException:
                if i == 4:
                    raise
                backoff(i)
                t = self.filter.get()
                assert t is not None
                upstream_tx = t
                assert upstream_tx.merge_from(tx_delta) is not None

        self._check_preconditions(upstream_tx)
        logging.debug(upstream_tx)
        del tx_orig.version
        upstream_delta = tx_orig.delta(upstream_tx)
        assert tx.merge_from(upstream_delta) is not None
        self.tx = upstream_tx
        return upstream_delta


    def get(self) -> Optional[TransactionMetadata]:
        if self.tx is not None:
            return self.tx

        t = self.filter.get()
        assert t is not None
        self.tx = t

        self._check_preconditions(self.tx)
        return self.tx

    def _check_preconditions(self, tx):
        # smtp preconditions: rcpt and no rcpt resp after mail err, etc.
        if tx.mail_response and tx.mail_response.err():
            # xxx code vs data_response (below)
            rcpt_err = Response(
                tx.mail_response.code,
                'RCPT failed precondition MAIL (AsyncFilterWrapper)')
            for i in range(len(tx.rcpt_response), len(tx.rcpt_to)):
                tx.rcpt_response.append(rcpt_err)
        if (tx.body_blob and tx.body_blob.finalized() and
            (not tx.data_response) and
            (len([r for r in tx.rcpt_response if r is not None]) ==
             len(tx.rcpt_to)) and
            (not any([r.ok() for r in tx.rcpt_response]))):
            temp = any([r.temp() for r in tx.rcpt_response])
            tx.data_response = Response(
                450 if temp else 550,
                'DATA failed precondition RCPT (AsyncFilterWrapper)')

        # if store&forward: upgrade temp errors to success
        if self.store_and_forward:
            data_last = False
            if tx.mail_response and tx.mail_response.temp():
                self.do_store_and_forward = True
                tx.mail_response = Response(
                    250, 'MAIL ok (AsyncFilterWrapper store and forward)')
            rcpt_response = Response(
                250, 'RCPT ok (AsyncFilterWrapper store and forward)')
            for i, resp in enumerate(tx.rcpt_response):
                if resp.temp():
                    tx.rcpt_response[i] = rcpt_response
                    self.do_store_and_forward = True

            if tx.body_blob is not None:
                if (not tx.body_blob.finalized() and
                    tx.data_response and tx.data_response.temp()):
                    tx.data_response = None
                    self.do_store_and_forward = True

                if (tx.body_blob.finalized() and
                    (self.do_store_and_forward or
                        (tx.data_response is not None and
                         tx.data_response.temp()))):
                    data_last = True
                    self.do_store_and_forward = True
                    tx.data_response = Response(
                        250, 'DATA ok (AsyncFilterWrapper store and forward)')

            if data_last and self.do_store_and_forward and tx.retry is None:
                retry_delta = TransactionMetadata(
                    retry = self.retry_params,
                    # XXX this will blackhole if unset!
                    notification=self.default_notification)
                assert tx.merge_from(retry_delta) is not None
                self.filter.update(tx, retry_delta)


    def on_update(self, tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:
        tx_orig = tx.copy()
        upstream_delta = self.update(tx, tx_delta)
        deadline = Deadline(self.timeout)
        upstream_tx = tx.copy()
        while deadline.remaining() and upstream_tx.req_inflight():
            self.wait(self.version(), deadline.deadline_left())
            upstream_tx = self.get()
        del tx_orig.version
        upstream_delta = tx_orig.delta(upstream_tx)
        assert tx.merge_from(upstream_delta) is not None
        return upstream_delta
