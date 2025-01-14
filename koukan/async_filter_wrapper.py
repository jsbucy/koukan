from typing import Optional
import logging

from koukan.filter import (
    AsyncFilter,
    SyncFilter,
    TransactionMetadata )
from koukan.deadline import Deadline
from koukan.storage_schema import VersionConflictException
from koukan.response import Response

class AsyncFilterWrapper(AsyncFilter):
    filter : AsyncFilter
    timeout : float
    store_and_forward : bool
    tx : Optional[TransactionMetadata] = None

    def __init__(self, filter : AsyncFilter,
                 timeout : float,
                 store_and_forward : bool = False):
        self.filter = filter
        self.timeout = timeout
        self.store_and_forward = store_and_forward

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
            self.tx.fill_inflight_responses(timeout_resp)

        return rv

    async def wait_async(self, version : int, timeout : float) -> bool:
        return await self.filter.wait_async(version, timeout)

    def version(self) -> Optional[int]:
        return self.filter.version()

    def update(self, tx : TransactionMetadata,
               tx_delta : TransactionMetadata
               ) -> Optional[TransactionMetadata]:
        deadline = Deadline(self.timeout)
        logging.debug('%s %s', tx, tx_delta)

        # xxx noop if prev temp err and not store&forward?

        tx_orig = tx.copy()
        upstream_tx : TransactionMetadata = tx.copy()
        while deadline.remaining():
            logging.debug('%s', upstream_tx)
            try:
                if upstream_tx.body:  # XXX yikes!
                    del upstream_tx.body

                upstream_delta = self.filter.update(upstream_tx, tx_delta)
                break
            except VersionConflictException:
                t = self.filter.get()
                assert t is not None
                upstream_tx = t
                assert upstream_tx.merge_from(tx_delta) is not None

        self._check_preconditions(upstream_tx)
        upstream_delta = tx_orig.delta(upstream_tx)
        assert tx.merge_from(upstream_delta) is not None
        self.tx = upstream_tx
        return upstream_delta


    def get(self) -> Optional[TransactionMetadata]:

        # TODO don't wait if store&forward and prev err

        if self.tx is not None:
            return self.tx

        t = self.filter.get()
        assert t is not None
        self.tx = t
        logging.debug('%s', self.tx)

        # TODO: we have a few of these hacks due to the way body/body_blob
        # get swapped around in and out of storage
#        if tx_orig.body_blob:
#            del tx_orig.body_blob

        # e.g. with rest, the client may
        # PUT /tx/123/body
        # GET /tx/123
        # and expect to see {...'body': {}}

        # however in internal call sites (i.e. Exploder), it's updating with
        # body_blob and not expecting to get body back
        # so only do this if tx_orig.body_blob?
        if self.tx.body:
            del self.tx.body
        if self.tx.version:
            del self.tx.version
        logging.debug(self.tx)

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
                #upstream_delta.rcpt_response.append(rcpt_err)
        logging.debug(tx)
        if (tx.body_blob and tx.body_blob.finalized() and
            (not tx.data_response) and
            (not any([r.ok() for r in tx.rcpt_response]))):
            temp = any([r.temp() for r in tx.rcpt_response])
            # upstream_delta.data_response =
            tx.data_response = Response(
                450 if temp else 550,
                'DATA failed precondition RCPT (AsyncFilterWrapper)')

        # if store&forward: upgrade temp errors to success
        if self.store_and_forward:
            if tx.mail_response and tx.mail_response.temp():
                #upstream_delta.mail_response =
                tx.mail_response = Response(
                    250, 'MAIL ok (AsyncFilterWrapper store and forward)')
            rcpt_response = Response(
                250, 'RCPT ok (AsyncFilterWrapper store and forward)')
            logging.debug(tx)
            rcpt_response = [rcpt_response if r.temp() else r
                             for r in tx.rcpt_response]
            #upstream_delta.rcpt_response =
            tx.rcpt_response = rcpt_response

            if tx.body_blob is not None:
                if (not tx.body_blob.finalized() and
                    tx.data_response and tx.data_response.temp()):
                    tx.data_response = None

                if (tx.body_blob.finalized() and
                    (tx.data_response is None or tx.data_response.temp())):
                    tx.data_response = Response(
                        250, 'DATA ok (AsyncFilterWrapper store and forward)')

            logging.debug(tx)
            # XXX enable retry/notification


    # update(tx)
    #   self.filter.update()  handling version conflict
    #   self.tx = tx

    # wait()
    #   if rv := self.filter.wait()
    #     self.tx = None
    #   else:
    #     self.tx.fill_inflight_response(Response(450, 'upstream timeout'))
    #   return rv

    # get()
    #   if self.tx is None:
    #     self.tx = self.filter.get()
    #   preconditions: rcpt but mail failed, etc.
    #   store and forward: prev timeout or temp err
    #   return self.tx


    # sync on_update
    #   self.filter.update()  handling version conflict
    #   while not timed out and req_inflight:
    #     self.filter.wait()
    #   tx = self.filter.get()
    #   timeout: req and resp is None
    #   s&f: temp err -> ok
    #   preconditions
