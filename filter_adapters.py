from typing import Optional
import logging

from filter import (
    AsyncFilter,
    SyncFilter,
    TransactionMetadata )

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
