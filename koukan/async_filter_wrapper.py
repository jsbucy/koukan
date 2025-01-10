from typing import Optional
import logging

from koukan.filter import (
    AsyncFilter,
    SyncFilter,
    TransactionMetadata )
from koukan.deadline import Deadline
from koukan.storage_schema import VersionConflictException

class AsyncFilterWrapper(SyncFilter):
    filter : AsyncFilter
    timeout : float
    def __init__(self, filter : AsyncFilter,
                 timeout : float):
        self.filter = filter
        self.timeout = timeout

    def on_update(self, tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:
        deadline = Deadline(self.timeout)
        logging.debug('%s %s', tx, tx_delta)
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

        while deadline.remaining() and upstream_tx.req_inflight():
            logging.debug('%s', upstream_tx)
            self.filter.wait(upstream_tx.version, deadline.deadline_left())
            # TODO need nospin logic here when we want to refactor RestEndpoint
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
        upstream_delta = tx_orig.delta(upstream_tx)
        assert tx.merge_from(upstream_delta) is not None

        # the original update_wait_inflight() didn't
        # tx.fill_inflight_responses() on a timeout here since it
        # wasn't trying to implement SyncFilter semantics; Exploder
        # took care of that. This probably needs to.
        # OTOH StoreAndForward just converts temp errors to success?

        return upstream_delta
