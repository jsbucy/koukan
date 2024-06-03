from typing import Optional
import logging

from filter import Filter, SyncFilter, TransactionMetadata


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
        upstream_delta = self.upstream.on_update(self.prev_tx, tx_delta)
        assert tx_delta.merge_from(upstream_delta) is not None
        assert len(tx_delta.rcpt_response) <= len(tx_delta.rcpt_to)


class FullToDeltaAdapter(SyncFilter):
    upstream : Filter

    def __init__(self, upstream : Filter):
        self.upstream = upstream

    def on_update(self, tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:
        downstream_delta = tx_delta.copy()
        req = tx_delta.copy()
        self.upstream.on_update(downstream_delta)
        upstream_delta = req.delta(downstream_delta)
        tx.merge_from(upstream_delta)
        return upstream_delta
