from typing import Awaitable, Callable, List, Optional
from functools import partial
import logging
import asyncio

from koukan.filter import TransactionMetadata

class FilterResult:
    # delta to be merged after upstream returns
    downstream_delta : Optional[TransactionMetadata] = None
    def __init__(self, delta : Optional[TransactionMetadata] = None):
        self.downstream_delta = delta

class BaseFilter:
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

# Whereas a "regular" filter only conservatively extends the tx, a
# "proxy" filter can implement an arbitrary transformation. The common
# case is modifying the body. Another example is rejecting individual
# rcpts in a multi-rcpt tx, say rate limit downstream of exploder.
class ProxyBaseFilter(BaseFilter):
    def wire_upstream(self, tx):
        self.upstream = tx

class FilterMixin:
    # upstream() yields to scheduler, returns delta
    async def on_update(
            self, delta : TransactionMetadata,
            upstream : Callable[[], Awaitable[TransactionMetadata]]):
        raise NotImplementedError()

class Filter(BaseFilter, FilterMixin):
    pass

class ProxyFilter(ProxyBaseFilter, FilterMixin):
    pass

class OneshotFilterMixin:
    def on_update(self, delta : TransactionMetadata) -> FilterResult:
        raise NotImplementedError()

class OneshotFilter(BaseFilter, OneshotFilterMixin):
    pass
class OneshotProxyFilter(ProxyBaseFilter, OneshotFilterMixin):
    pass


class FilterChain:
    filters : List[BaseFilter]
    loop : asyncio.AbstractEventLoop
    # convenience alias for filters[0].downstream
    tx : Optional[TransactionMetadata] = None

    def __init__(self, filters : List[BaseFilter],
                 loop : Optional[asyncio.AbstractEventLoop] = None):
        self.filters = filters

        # placeholder to avoid deprecation warning creating Future
        # without a loop. AFAICT Future only uses it for scheduling
        # callbacks which we never register.
        self.loop = loop

    def __del__(self):
        if self.loop:
            self.loop.close()
            self.loop = None

    def init(self, tx : TransactionMetadata):
        self.tx = tx
        for f in self.filters:
            f.wire_downstream(tx)
            if isinstance(f, ProxyBaseFilter):
                tx = TransactionMetadata()
                f.wire_upstream(tx)

    def update(self):
        completion = []  # Tuple(filter, coroutine, future, FilterResult)

        # TODO maybe move noop/heartbeat/keepalive to a separate entry
        # point which most impls don't need to implement
        noop = not self.filters[0].prev_downstream.delta(self.tx)

        async def upstream(futures):
            # logging.debug('upstream')
            futures[0] = self.loop.create_future()
            return await futures[0]

        prev = self.filters[0].downstream.copy()

        for f in self.filters:
            logging.debug(f)
            logging.debug(f.prev_downstream)
            logging.debug(f.downstream)
            delta = f.prev_downstream.delta(f.downstream)
            if not noop and not delta:
                break
            assert delta.mail_response is None
            assert not delta.rcpt_response
            assert delta.data_response is None

            f.prev_downstream = f.downstream.copy()

            co = None
            fut = None
            filter_result = None
            if isinstance(f, FilterMixin):
                futures = [None]
                co = f.on_update(delta, partial(upstream, futures))
                try:
                    co.send(None)
                except StopIteration as e:
                    # i.e. returned without calling upstream()
                    co = None
                fut = futures[0]
                futures = None
            elif isinstance(f, OneshotFilterMixin):
                filter_result = f.on_update(delta)
            else:
                raise NotImplementedError()

            f.prev_upstream = f.upstream.copy()

            completion.append((f, co, fut, filter_result))
            if f == self.filters[-1]:
                assert fut is None  # i.e. RestEndpoint
            if not f.downstream.check_preconditions():
                break

        for f, co, fut, prev_result in reversed(completion):
            logging.debug('%s %s %s %s', f, co, fut, prev_result)
            delta = f.prev_upstream.delta(f.upstream)
            f.prev_upstream = f.upstream.copy()
            if co is not None and fut is not None:
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
                filter_result = None
                try:
                    co.send(None)
                except StopIteration as e:
                    if isinstance(e.value, FilterResult):
                        filter_result = e.value
                # unexpected for it *not* to raise?
            elif prev_result is not None:
                logging.debug(prev_result.downstream_delta)
                if f.upstream is not f.downstream:
                    f.downstream.merge_from(delta)
                if prev_result.downstream_delta is not None:
                    f.downstream.merge_from(prev_result.downstream_delta)
                logging.debug(f.downstream)

            f.prev_downstream = f.downstream.copy()

        return prev.delta(self.filters[0].downstream)

