
from koukan.filter import AsyncFilter
from koukan.filter_chain import Filter, FilterResult
from koukan.filter import TransactionMetadata
from koukan.deadline import Deadline
from koukan.storage_schema import (
    VersionConflictException )
from koukan.backoff import backoff

# AsyncFilter -> filter_chain.Filter shim to use SWF in FilterChain
# for add_route
class AsyncFilterAdapter(Filter):
    async_filter : AsyncFilter
    timeout : float

    def __init__(self, async_filter : AsyncFilter, timeout):
        self.async_filter = async_filter
        self.timeout = timeout

    def on_update(self, tx_delta : TransactionMetadata) -> FilterResult:
        assert self.downstream_tx is not None
        tx = self.downstream_tx
        if tx_delta.body is not None and not tx_delta._body_last():
            tx_delta.body = None
            if not tx_delta:
                return FilterResult()
            tx = self.downstream_tx.copy()
            tx.body = None

        prev = tx.copy()
        for i in range(0,5):
            try:
                upstream_delta = self.async_filter.update(tx, tx_delta)
            except VersionConflictException:
                if i == 4:
                    raise
                backoff(i)
                tx = self.async_filter.get()
                assert tx.merge_from(tx_delta) is not None
                # assert tx.merge_from(prev.delta(upstream_tx)) is not None
        self.downstream_tx.merge_from(prev.delta(tx))

        deadline = Deadline(self.timeout)
        if tx.req_inflight():
            while deadline.remaining() and tx.req_inflight():
                version = self.async_filter.version
                assert version is not None
                dl = deadline.deadline_left()
                assert dl is not None
                prev = tx.copy()
                rv, u = self.async_filter.wait(version, dl)
                if u is None:
                    u = self.async_filter.get()
                assert u is not None
                tx = u
            upstream_delta = prev.delta(tx)
            self.downstream_tx.merge_from(upstream_delta)
        return FilterResult()
