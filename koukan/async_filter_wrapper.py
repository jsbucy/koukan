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
        return self.filter.wait(version, timeout)
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
        return upstream_delta


    def get(self) -> Optional[TransactionMetadata]:

        # TODO don't wait if store&forward and prev err
        # while deadline.remaining() and upstream_tx.req_inflight():
        #     logging.debug('%s', upstream_tx)
        #     self.filter.wait(upstream_tx.version, deadline.deadline_left())
        #     # TODO need nospin logic here when we want to refactor RestEndpoint
        t = self.filter.get()
        assert t is not None
        upstream_tx = t
        logging.debug('%s', upstream_tx)

        # TODO: we have a few of these hacks due to the way body/body_blob
        # get swapped around in and out of storage
        if tx_orig.body_blob:
            del tx_orig.body_blob

        # e.g. with rest, the client may
        # PUT /tx/123/body
        # GET /tx/123
        # and expect to see {...'body': {}}

        # however in internal call sites (i.e. Exploder), it's updating with
        # body_blob and not expecting to get body back
        # so only do this if tx_orig.body_blob?
        if upstream_tx.body:
            del upstream_tx.body
        if tx_orig.version:
            del tx_orig.version
        logging.debug(tx_orig)
        logging.debug(upstream_tx)
        upstream_delta = tx_orig.delta(upstream_tx)
        assert tx.merge_from(upstream_delta) is not None

        # check timeouts XXX only after timeout
        timeout_resp = Response(450, 'upstream timeout (AsyncFilterWrapper)')
        if tx.mail_from and tx.mail_response is None:
            #upstream_delta.mail_response =
            tx.mail_response = timeout_resp
        elif len(tx.rcpt_response) < len(tx.rcpt_to):
            rcpt_timeout = [timeout_resp] * (len(tx.rcpt_to) - len(tx.rcpt_response))
            tx.rcpt_response.extend(rcpt_timeout)
            #upstream_delta.rcpt_response.extend(rcpt_timeout)
        elif tx.body_blob and tx.body_blob.finalized() and not tx.data_response:
            tx.data_response = timeout_resp

        self._check_preconditions(upstream_tx)
        return upstream_tx

    def _check_preconditions(self, tx):
        # smtp preconditions: rcpt and no rcpt resp after mail err, etc.
        if tx.mail_response and tx.mail_response.err():
            # xxx code vs data_response (below)
            rcpt_err = Response(tx.mail_response.code, 'RCPT failed precondition MAIL (AsyncFilterWrapper)')
            for i in range(len(tx.rcpt_response), len(tx.rcpt_to)):
                tx.rcpt_response.append(rcpt_err)
                #upstream_delta.rcpt_response.append(rcpt_err)
        logging.debug(tx)
        if tx.body_blob and tx.body_blob.finalized() and not tx.data_response and not any([r.ok() for r in tx.rcpt_response]):
            temp = any([r.temp() for r in tx.rcpt_response])
            # upstream_delta.data_response =
            tx.data_response = Response(
                450 if temp else 550, 'DATA failed precondition RCPT (AsyncFilterWrapper)')

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
                if not tx.body_blob.finalized() and tx.data_response and tx.data_response.temp():
                    tx.data_response = None

                if tx.body_blob.finalized() and (tx.data_response is None or tx.data_response.temp()):
                    tx.data_response = Response(
                        250, 'DATA ok (AsyncFilterWrapper store and forward)')

            logging.debug(tx)
            # XXX enable retry/notification


    # update()
    #   self.filter.update()  handling version conflict
    #

    # wait()
    #   if self.timed_out():
    #     return
    #   if not self.filter.wait():
    #     self.timed_out = True
    #     if self.store_and_forward_enabled:
    #       self.do_store_and_forward = True

    # get()
    #   tx = self.filter.get()
    #   if self.timed_out:
    #     # req and response is None
    #   preconditions: rcpt but mail failed, etc.
    #   store and forward: prev timeout or temp err


    # sync on_update
    #   self.filter.update()  handling version conflict
    #   while not timed out and req_inflight:
    #     self.filter.wait()
    #   tx = self.filter.get()
    #   timeout: req and resp is None
    #   s&f: temp err -> ok
    #   preconditions
