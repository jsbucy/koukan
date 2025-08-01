from typing import Awaitable, Callable, List, Optional
from functools import partial
import logging
import asyncio

from koukan.filter import TransactionMetadata

class Filter:
    prev_downstream : Optional[TransactionMetadata] = None
    downstream : Optional[TransactionMetadata] = None
    prev_upstream : Optional[TransactionMetadata] = None
    upstream : Optional[TransactionMetadata] = None

    def __init__(self):
        pass

    def wire_downstream(self, tx : TransactionMetadata):
        self.downstream = self.upstream = tx
        self.prev_downstream = TransactionMetadata()
        self.prev_upstream = TransactionMetadata()

    # upstream() yields to scheduler, returns delta
    async def update(self, delta : TransactionMetadata,
                     upstream : Callable[[], Awaitable[TransactionMetadata]]):
        pass


class ProxyFilter(Filter):
    def wire_upstream(self, tx):
        self.upstream = tx

class FilterChain:
    filters : List[Filter]
    loop : asyncio.AbstractEventLoop

    def __init__(self, filters : List[Filter]):
        self.filters = filters

        # placeholder to avoid deprecation warning creating Future
        # without a loop. AFAICT Future only uses it for scheduling
        # callbacks which we never register.
        self.loop = asyncio.new_event_loop()

    def init(self, tx : TransactionMetadata):
        for f in self.filters:
            f.wire_downstream(tx)
            if isinstance(f, ProxyFilter):
                tx = TransactionMetadata()
                f.wire_upstream(tx)

    def update(self):
        completion = []  # Tuple(filter, coroutine, future)

        async def upstream(futures):
            # logging.debug('upstream')
            futures[0] = self.loop.create_future()
            return await futures[0]

        prev = self.filters[0].downstream.copy()

        for f in self.filters:
            delta = f.prev_downstream.delta(f.downstream)
            f.prev_downstream = f.downstream.copy()

            futures = [None]
            co = f.update(delta, partial(upstream, futures))
            try:
                co.send(None)
            except StopIteration:
                pass  # i.e. never called upstream()
            f.prev_upstream = f.upstream.copy()
            fut = futures[0]
            futures = None
            completion.append((f, co, fut))
            if f == self.filters[-1]:
                assert fut is None  # i.e. RestEndpoint
            if fut is None:
                logging.debug('no fut')
                break

        for f, co, fut in reversed(completion):
            if fut is not None:
                delta = f.prev_upstream.delta(f.upstream)
                f.prev_upstream = f.upstream.copy()
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

        return prev.delta(self.filters[0].downstream)
