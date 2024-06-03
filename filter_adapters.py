from typing import Optional
import logging

from filter import (
    AsyncFilter,
    Filter,
    SyncFilter,
    TransactionMetadata )

class DeltaToFullAdapter(Filter):
    upstream : SyncFilter
    prev_tx : Optional[TransactionMetadata] = None

    def __init__(self, upstream : SyncFilter):
        self.upstream = upstream

    def on_update(self, tx_delta : TransactionMetadata,
                  timeout : Optional[float] = None):
        if self.prev_tx is None:
            self.prev_tx = tx_delta.copy()
        else:
            self.prev_tx.merge_from(tx_delta)
        logging.debug('DeltaToFullAdapter start')
        upstream_delta = self.upstream.on_update(self.prev_tx, tx_delta)
        logging.debug('DeltaToFullAdapter done')
        assert len(self.prev_tx.rcpt_response) <= len(self.prev_tx.rcpt_to)
        assert len(upstream_delta.rcpt_response) <= len(self.prev_tx.rcpt_to)
        assert tx_delta.merge_from(upstream_delta) is not None


class FullToDeltaAdapter(SyncFilter):
    upstream : Filter

    def __init__(self, upstream : Filter):
        self.upstream = upstream

    def on_update(self, tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:
        req = tx_delta.copy()
        logging.debug('FullToDeltaAdapter start')
        self.upstream.on_update(tx_delta)
        logging.debug('FullToDeltaAdapter done')
        assert len(tx_delta.rcpt_response) <= len(tx.rcpt_to)
        upstream_delta = req.delta(tx_delta)
        tx.merge_from(upstream_delta)
        return upstream_delta


class SyncToAsyncAdapter(SyncFilter):
    upstream : AsyncFilter

    def __init__(self, upstream : AsyncFilter):
        self.upstream = upstream

    def on_update(self, tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:
        upstream_tx = tx_delta.copy()
        upstream_delta = self.upstream.update(upstream_tx, tx_delta)

        while tx.req_inflight():
            # if timed out:
            #   tx.fill_inflight_responses(Response(450, "upstream timeout"))
            #   break
            # sleep/backoff
            upstream_tx = self.upstream.get()


        upstream_delta = tx.delta(upstream_tx)
        tx.replace_from(upstream_tx)
        return upstream_delta
