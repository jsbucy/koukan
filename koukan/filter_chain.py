# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Awaitable, Callable, Coroutine, List, Optional, Tuple
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
    _prev_downstream_tx : Optional[TransactionMetadata] = None
    downstream_tx : Optional[TransactionMetadata] = None
    _prev_upstream_tx : Optional[TransactionMetadata] = None
    upstream_tx : Optional[TransactionMetadata] = None

    # upstream_tx.rcpt_to[i] == downstream_tx[rcpt_offset[i]]
    _rcpt_offset : Optional[List[int]] = None


    def __init__(self):
        pass

    def wire_downstream(self, tx : TransactionMetadata):
        self.downstream_tx = self.upstream_tx = tx
        self._prev_downstream_tx = TransactionMetadata()
        self._prev_upstream_tx = TransactionMetadata()

# Whereas a "regular" filter only conservatively extends the tx, a
# "proxy" filter can implement an arbitrary transformation. The common
# case is modifying the body. Another example is rejecting individual
# rcpts in a multi-rcpt tx, say rate limit downstream of exploder.
class ProxyBaseFilter(BaseFilter):
    def wire_upstream(self, tx):
        self.upstream_tx = tx

# A simple filter doesn't do anything in the downstream direction
# except possibly merging an error response.
class FilterMixin:
    def on_update(self, delta : TransactionMetadata) -> FilterResult:
        raise NotImplementedError()

class Filter(BaseFilter, FilterMixin):
    pass
class ProxyFilter(ProxyBaseFilter, FilterMixin):
    pass


# Coroutine filters take a callable that "yields to the scheduler"
# (FilterChain) and returns the upstream delta.
class CoroutineFilterMixin:
    # upstream() yields to scheduler, returns delta
    async def on_update(
            self, delta : TransactionMetadata,
            upstream : Callable[[], Awaitable[TransactionMetadata]]):
        raise NotImplementedError()

class CoroutineFilter(BaseFilter, CoroutineFilterMixin):
    pass

class CoroutineProxyFilter(ProxyBaseFilter, CoroutineFilterMixin):
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
        if loop is None:
            loop = asyncio.new_event_loop()
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

    def _filter_rcpts(self, f : Filter):
        # A ProxyFilter may fail individual rcpts by setting the
        # corresponding rcpt_response. This copies the remaining rcpts
        # from downstream_tx to upstream_tx and populates _rcpt_offset
        # for _copy_rcpt_responses() on the way back down.
        assert f.downstream_tx is not None
        assert f.upstream_tx is not None

        down_rcpt = len(f.downstream_tx.rcpt_to)
        up_rcpt = len(f.upstream_tx.rcpt_to)

        if f.downstream_tx is f.upstream_tx or down_rcpt == up_rcpt:
            return
        if f._rcpt_offset is None:
            f._rcpt_offset = []
        for i,rcpt in enumerate(f.downstream_tx.rcpt_to):
            no_resp = (i >= len(f.downstream_tx.rcpt_response)) or (
                f.downstream_tx.rcpt_response[i] is None)
            if (i >= len(f.upstream_tx.rcpt_to)) and no_resp:
                f.upstream_tx.rcpt_to.append(f.downstream_tx.rcpt_to[i])
            if no_resp:
                f._rcpt_offset.append(i)

        assert len(f._rcpt_offset) == len(f.upstream_tx.rcpt_to)

    def _copy_rcpt_responses(self, f : Filter, delta : TransactionMetadata):
        assert f.downstream_tx is not None
        assert f.upstream_tx is not None
        # If _filter_rcpts() previously copied rcpts from downstream
        # to upstream and populated _rcpt_offset, propagate
        # rcpt_response from upstream_tx to downstream_tx per
        # _rcpt_offset mapping.
        if f._rcpt_offset is None:
            return

        delta.rcpt_response = []
        delta.rcpt_response_list_offset = None
        for i, resp in enumerate(f.upstream_tx.rcpt_response):
            off = f._rcpt_offset[i]
            if (off < len(f.downstream_tx.rcpt_response) and
                f.downstream_tx.rcpt_response[off] is not None):
                continue
            if off >= len(f.downstream_tx.rcpt_response):
                f.downstream_tx.rcpt_response.append(None)
            assert f.downstream_tx.rcpt_response[off] is None
            f.downstream_tx.rcpt_response[off] = resp

    def update(self):
        completion : List[
            Tuple[BaseFilter, Coroutine, asyncio.Future, FilterResult]] = []

        # TODO maybe move noop/heartbeat/keepalive to a separate entry
        # point which most impls don't need to implement
        noop = not self.filters[0]._prev_downstream_tx.delta(self.tx)

        async def upstream(futures):
            futures[0] = self.loop.create_future()
            return await futures[0]

        prev = self.filters[0].downstream_tx.copy()

        for f in self.filters:
            assert f.downstream_tx is not None
            assert f._prev_downstream_tx is not None
            delta = f._prev_downstream_tx.delta(f.downstream_tx)
            if not noop and not delta:
                break
            assert delta.mail_response is None
            assert not delta.rcpt_response
            assert delta.data_response is None

            f._prev_downstream_tx = f.downstream_tx.copy()

            co = None
            fut = None
            filter_result = None
            if isinstance(f, CoroutineFilterMixin):
                futures = [None]
                co = f.on_update(delta, partial(upstream, futures))
                try:
                    co.send(None)
                except StopIteration as e:
                    # i.e. returned without calling upstream()
                    co = None
                fut = futures[0]
                futures = None
            elif isinstance(f, FilterMixin):
                filter_result = f.on_update(delta)
            else:
                raise NotImplementedError()

            if filter_result is not None:
                self._filter_rcpts(f)

            f._prev_upstream_tx = f.upstream_tx.copy()

            completion.append((f, co, fut, filter_result))
            if f == self.filters[-1]:
                assert fut is None  # i.e. RestEndpoint
            if not f.downstream_tx.check_preconditions():
                break

        for f, co, fut, prev_result in reversed(completion):
            delta = f._prev_upstream_tx.delta(f.upstream_tx)
            f._prev_upstream_tx = f.upstream_tx.copy()
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
                    pass
                # unexpected for it *not* to raise?
            elif prev_result is not None:
                if f.upstream_tx is not f.downstream_tx:
                    self._copy_rcpt_responses(f, delta)
                    f.downstream_tx.merge_from(delta)
                if prev_result.downstream_delta is not None:
                    f.downstream_tx.merge_from(prev_result.downstream_delta)

            f._prev_downstream_tx = f.downstream_tx.copy()

        entries = []
        for f, co, fut, prev_result in completion:
            entries.append(f.__class__.__name__)
        logging.debug(', '.join(entries))

        return prev.delta(self.filters[0].downstream_tx)
