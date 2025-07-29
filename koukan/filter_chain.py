from typing import Callable, List, Optional
from functools import partial
import logging
import asyncio

from koukan.filter import TransactionMetadata

class Filter:
    downstream : Optional[TransactionMetadata] = None
    upstream : Optional[TransactionMetadata] = None

    def __init__(self):
        pass

    def wire_downstream(self, tx : TransactionMetadata):
        self.downstream = tx

    # upstream() yields to scheduler, returns delta
    async def update(self, delta : TransactionMetadata,
                     upstream : Callable[[], TransactionMetadata]):
        pass

class ProxyFilter(Filter):
    def wire_upstream(self, tx):
        self.upstream = tx

class FilterChain:
    filters : List[Filter]
    prev_tx : TransactionMetadata
    tx : TransactionMetadata
    loop : asyncio.BaseEventLoop

    def __init__(self, filters : List[Filter]):
        self.filters = filters

        # placeholder to avoid deprecation warning creating Future
        # without a loop. AFAICT Future only uses it for scheduling
        # callbacks which we never register.
        self.loop = asyncio.new_event_loop()


    def init(self, tx : TransactionMetadata):
        self.prev_tx = TransactionMetadata()
        self.tx = tx

        for f in self.filters:
            f.wire_downstream(tx)
            if isinstance(f, ProxyFilter):
                tx = TransactionMetadata()
                f.wire_upstream(tx)

    def update(self):
        logging.debug(self.tx)
        delta = self.prev_tx.delta(self.tx)
        logging.debug(delta)
        completion = []  # Tuple(filter, coroutine, future, tx)

        async def upstream(futures):
            logging.debug('upstream')
            futures[0] = self.loop.create_future()
            return await futures[0]

        tx = self.tx
        for f in self.filters:
            prev = tx.copy()

            futures = [None]
            co = f.update(delta, partial(upstream, futures))
            try:
                co.send(None)
            except StopIteration:
                pass  # i.e. never called upstream()
            delta = prev.delta(tx)
            assert delta is not None
            fut = futures[0]
            futures = None
            completion.append((f, co, fut, prev))
            if f == self.filters[-1]:
                assert fut is None  # i.e. RestEndpoint
            if fut is None:
                logging.debug('no fut')
                break
            if f.upstream is not None and f.downstream is not f.upstream:
                tx = f.upstream

        for f, co, fut, prev in reversed(completion):
            if fut is not None:
                fut.set_result(delta)
                # TODO The filter impl could do multiple roundtrips
                # with the upstream? That would manifest here as
                # calling upstream again and not raising
                # StopIteration? For the time being, we clear futures
                # after the first call (above) so a subsequent call
                # will throw.
                # To implement this, we would loop around the whole
                # thing here and restart the first downstream loop at
                # the filter after f here.
                try:
                    co.send(None)
                except StopIteration:
                    pass

            delta = prev.delta(f.downstream)

        self.prev_tx = self.tx.copy()

        return delta
