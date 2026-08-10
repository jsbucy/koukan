# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
from enum import IntEnum
import json
import logging
import secrets
from functools import partial, reduce
from threading import Lock, Condition
import time

from koukan.backoff import backoff
from koukan.storage import (
    BlobCursor,
    GroupCursor,
    Storage,
    TransactionCursor )
from koukan.storage_schema import BlobSpec
from koukan.response import Response
from koukan.filter import (
    AsyncFilter,
    HostPort,
    Mailbox,
    TransactionGroup,
    TransactionMetadata )
from koukan.blob import Blob, InlineBlob, WritableBlob
from koukan.deadline import Deadline

from koukan.rest_schema import BlobUri, parse_blob_uri
from koukan.message_builder import MessageBuilderSpec
from koukan.sender import Sender

EndpointYamlProvider = Callable[[Sender], Optional[dict]]
TxHandler = Callable[[Sender, Callable[[], Optional[TransactionCursor]]], bool]

class DownstreamResponse(IntEnum):
    NONE = 0
    SF = 1
    UPSTREAM_TIMEOUT = 2

downstream_responses = {
    DownstreamResponse.SF: Response(250, 'StorageWriterFilter store&forward'),
    DownstreamResponse.UPSTREAM_TIMEOUT: Response(
        450, 'StorageWriterFilter upstream timeout')
}

class StorageWriterFilter(AsyncFilter):
    storage : Storage
    # tx_cursor : Optional[TransactionCursor] = None
    group_cursor : Optional[GroupCursor] = None
    rest_id_factory : Optional[Callable[[], str]] = None
    rest_id : Optional[str] = None
    create_leased : bool = False
    # leased cursor for cutthrough
    upstream_cursor : List[Optional[TransactionCursor]]
    create_err : bool = False
    mu : Lock
    cv : Condition
    endpoint_yaml_provider : Optional[EndpointYamlProvider] = None
    endpoint_yaml : Optional[Dict[str, Any]] = None
    sf_mode : Optional[str] = None
    exploder : Optional[bool] = None
    sender : Optional[Sender] = None
    tx_handler : Optional[TxHandler]
    tx_group : Optional[TransactionGroup] = None
    # per _timeout()
    next_upstream_timeout : Optional[int] = None

    def __init__(self, storage,
                 rest_id_factory : Optional[Callable[[], str]] = None,
                 rest_id : Optional[str] = None,
                 create_leased : bool = False,
                 sender : Optional[Sender] = None,
                 endpoint_yaml_provider : Optional[EndpointYamlProvider] = None,
                 tx_handler : Optional[TxHandler] = None):
        self.storage = storage
        self.rest_id_factory = rest_id_factory
        self.rest_id = rest_id
        self.create_leased = create_leased
        self.mu = Lock()
        self.cv = Condition(self.mu)
        self.endpoint_yaml_provider = endpoint_yaml_provider
        self.tx_handler = tx_handler
        self.upstream_cursor = []
        self._maybe_load_endpoint_yaml(sender)

    def _maybe_load_endpoint_yaml(
            self, sender : Optional[Sender] = None) -> None:
        if self.sender is not None or sender is None:
            return
        self.sender = sender
        assert self.endpoint_yaml_provider is not None
        if self.endpoint_yaml is not None:
            return
        self.endpoint_yaml = self.endpoint_yaml_provider(self.sender)
        assert self.endpoint_yaml is not None
        self.sf_mode = self.endpoint_yaml.get('sf_mode', None)
        if self.sf_mode is not None:
            assert self.sf_mode in [
                'upstream_unavailability',  # ~submission
                'mixed_data_response']      # ~interchange
        chain = self.endpoint_yaml.get('chain', [])
        self.exploder = chain[-1]['filter'] == 'exploder' if chain else False

    def incremental(self):
        assert self.endpoint_yaml is not None
        return (self.sf_mode is not None or
                self.exploder or
                self.endpoint_yaml.get('allow_incremental', False))

    # AsyncFilter (for Exploder/AsyncFilterWrapper)
    def wait(self, version, timeout
             ) -> Tuple[bool, Optional[TransactionMetadata]]:
        # cursor can be None after first update() to create with the
        # cutthrough/handoff workflow

        assert self.group_cursor is not None
        if self.version != version:
            return True, None
        assert len(self.group_cursor.tx_cursors) == 1
        res = self.group_cursor.tx_cursors[0].wait(timeout)
        logging.debug(res)
        if res is None:
            return False, None
        success, cloned = res
        logging.debug([c.version for c in self.group_cursor.tx_cursors])
        if not success:
            return False, None
        return True, self._get() if cloned else None


    # AsyncFilter (for RestHandler)
    async def wait_async(self, version, timeout
                         ) -> Tuple[bool, Optional[TransactionMetadata]]:
        assert self.group_cursor is not None
        if self.version != version:
            return True, None
        logging.debug(version)
        logging.debug([c.version for c in self.group_cursor.tx_cursors])

        # if there's a s&f downstream timeout, cap timeout by that,
        # _get() after will set s&f response and bump version.
        upstream = False
        if self.next_upstream_timeout is not None:
            now_ms = self._millis()
            logging.debug('%d %d', self.next_upstream_timeout, now_ms)
            upstream_timeout = max(
                self.next_upstream_timeout - now_ms, 0) / 1000.0
            logging.debug('%f %f', timeout, upstream_timeout)
            timeout = min(timeout, upstream_timeout)
            upstream = timeout == upstream_timeout

        res = await self.group_cursor.wait_async(timeout)
        logging.debug(res)
        if res is None and not upstream:
            return False, None
        if res is not None:
            success, cloned = res
            assert success
            return True, self._get() if cloned else None
        elif res is None and upstream:
            return True, self._get()
        assert False, 'bug'

    def release_transaction_cursor(
            self, i : int) -> Optional[TransactionCursor]:
        with self.mu:
            if not self.cv.wait_for(
                    lambda: len(self.upstream_cursor) > i and
                    self.upstream_cursor[i] is not None or
                    self.create_err, 3):
                logging.debug(self.upstream_cursor)
                logging.warning(
                    'StorageWriterFilter.get_transaction_cursor timeout %s',
                    self.create_err)
                return None
            elif (len(self.upstream_cursor) <= i or
                  self.upstream_cursor[i] is None):
                return None
            logging.debug('StorageWriterFilter.release_transaction_cursor')
            cursor = self.upstream_cursor[i]
            self.upstream_cursor[i] = None
            return cursor

    @property
    def version(self) -> Optional[int]:
        if self.group_cursor is None:
            return None
        version = 0
        for i in self.group_cursor.tx_cursors:
            assert i.tx is not None
            assert i.version is not None
            version += i.version
        return version

    def _millis(self):
        return time.time_ns() / 1e6

    def _timeout(self, delta : TransactionMetadata) -> int:
        assert self.sf_mode is not None
        assert self.endpoint_yaml is not None
        yaml = self.endpoint_yaml.get('downstream', {})
        secs = 0
        if delta.mail_from:
            secs += yaml.get('mail_timeout', 30)
        if delta.rcpt_to:
            secs += yaml.get('rcpt_timeout', 30)
        if delta._body_last():
            secs += yaml.get('data_timeout', 60)

        return int(self._millis() + secs * 1e3)

    def _create(self, tx : TransactionMetadata) -> None:
        # TODO handle
        # assert len(tx.rcpt_to) <= 1
        tx_cursor = self.storage.get_transaction_cursor()
        assert self.rest_id_factory is not None
        rest_id = self.rest_id_factory()
        storage_tx = tx.copy()
        rcpt_to = None
        if len(storage_tx.rcpt_to) > 1:
            rcpt_to = storage_tx.rcpt_to
            storage_tx.rcpt_to = rcpt_to[0:1]
            rcpt_to = rcpt_to[1:]
        self._maybe_load_endpoint_yaml()
        assert self.endpoint_yaml is not None
        output_yaml = self.endpoint_yaml.get('output_handler', {})
        assert self.sender is not None
        assert self.sender.yaml is not None
        if self.sender.yaml.get('retry', None) == 'output_chain':
            storage_tx.retry = {}
        if self.sender.yaml.get('notification', None) == 'output_chain':
            storage_tx.notification = {}
        if self.sf_mode is not None:
            timeout = self._timeout(tx)
            storage_tx.sf_mail_timeout = timeout
            if storage_tx.rcpt_to:
                storage_tx.sf_rcpt_timeout = [timeout] * len(storage_tx.rcpt_to)
        assert self.tx_group is None
        self.tx_group = TransactionGroup([])
        storage_tx.group = self.tx_group
        tx_cursor.create(rest_id, storage_tx,
                         create_leased=self.create_leased)
        self.group_cursor = GroupCursor(self.storage, tx_cursor)
        self.tx_group.tx_cursors.extend(
            [c.clone() for c in self.group_cursor.tx_cursors])

        self.rest_id = rest_id  # XXX locking below?

        if rcpt_to:
            prev = storage_tx.copy()
            storage_tx.rcpt_to.extend(rcpt_to)
            self._update(storage_tx, prev.delta(storage_tx))

        with self.mu:
            # self.rest_id = rest_id
            self.cv.notify_all()

    def _update_timeout(self, downstream_resp : Optional[int], timeout):
        if downstream_resp is not None and downstream_resp:
            return
        if timeout is None:
            return
        elif self.next_upstream_timeout is None:
            self.next_upstream_timeout = timeout
        else:
            self.next_upstream_timeout = min(
                self.next_upstream_timeout, timeout)

    # xxx _maybe_load()?
    def _load(self, cache_only=False) -> bool:
        logging.debug(self.rest_id)
        tx = None
        if self.group_cursor is None:
            self.group_cursor = GroupCursor(self.storage, rest_id=self.rest_id)

        if not self.group_cursor.try_cache():
            if cache_only:
                return False
            if not self.group_cursor.load(tx_rest_id=self.rest_id):
                return False

        logging.debug('%s %s %s', self.rest_id, self.group_cursor.db_id_versions(), tx)

        if not self.group_cursor.tx_cursors:  # 404 e.g. after GC
            return False
        tx0 = self.group_cursor.tx_cursors[0].tx
        assert tx0 is not None
        if self.sender is None:
            if tx0.sender is None:
                return False
            self._maybe_load_endpoint_yaml(tx0.sender)

        if self.tx_group is None:
            if tx0.group is None:
                tx0.group = self.tx_group = TransactionGroup(
                    [c.clone() for c in self.group_cursor.tx_cursors])
            else:
                self.tx_group = tx0.group
            for c in self.group_cursor.tx_cursors[1:]:
                assert c.tx is not None
                assert c.tx.group is None or c.tx.group is self.tx_group
                c.tx.group = self.tx_group

        # Set next_upstream_timeout to the min of all the
        # sf...timeouts that don't have a corresponding
        # downstream...response. This is used to cap the timeout in
        # wait_async().

        self.next_upstream_timeout = None
        if self.group_cursor.tx_cursors:
            tx0 = self.group_cursor.tx_cursors[0].tx
            assert tx0 is not None
            logging.debug(tx0)
            self._update_timeout(
                tx0.downstream_mail_response, tx0.sf_mail_timeout)
        for txc in self.group_cursor.tx_cursors:
            assert txc.tx is not None
            logging.debug(txc.tx)
            for i in range(0, len(txc.tx.rcpt_to)):
                downstream_resp = None
                if i >= len(txc.tx.sf_rcpt_timeout):
                    continue
                if (i < len(txc.tx.downstream_rcpt_response) and
                    not txc.tx.downstream_rcpt_response[i]):
                    continue
                # placeholder non-None value
                self._update_timeout(1, txc.tx.sf_rcpt_timeout[i])
            self._update_timeout(
                txc.tx.downstream_data_response, txc.tx.sf_data_timeout)

        return True

    @staticmethod
    def reduce_data_response(
            lhs : Optional[TransactionMetadata],
            rhs : Optional[TransactionMetadata]
    ) -> Optional[TransactionMetadata]:
        if lhs is None:
            return None
        assert lhs is not None
        if not lhs.rcpt_response or lhs.rcpt_response[0] is None:
            return None
        assert rhs is not None
        if not rhs.rcpt_response or rhs.rcpt_response[0] is None:
            return None
        if lhs.rcpt_response[0].err():
            return rhs
        if rhs.rcpt_response[0].err():
            return lhs
        if (lhs.data_response is None) or (rhs.data_response is None):
            return None
        if (lhs.data_response.major_code() !=
            rhs.data_response.major_code()):
            return None
        return lhs

    # AsyncFilter
    def get(self) -> Optional[TransactionMetadata]:
        if not self._load():
            return None
        return self._get()

    # if an upstream rcpt/tx has returned a temp error or timed out,
    # sets the corresponding downstream...response field to "250 store&forward"
    def _do_sf_unavail(self, group_tx) -> List[TransactionCursor]:
        assert self.group_cursor is not None
        now = self._millis()

        def any_rcpt_ok(rcpts : List[Optional[Response]]) -> bool:
            return any([r is not None and r.ok() for r in rcpts])
        def unavail(tx, resp : Optional[Response], timeout : int) -> bool:
            logging.debug('%d %s %s %s', now, timeout, now > timeout, resp)
            res = (resp is None and now > timeout) or (
                resp is not None and resp.temp())
            logging.debug(res)
            return res
        sf_cursor = []
        for i,tx in enumerate(group_tx):
            prev = tx.copy()
            logging.debug(tx)
            sf_mail = False

            if (not tx.downstream_mail_response and
                tx.mail_from is not None and
                unavail(tx, tx.mail_response, tx.sf_mail_timeout)):
                sf_mail = True
                tx.downstream_mail_response = DownstreamResponse.SF
            if tx.downstream_mail_response:
                tx.mail_response = downstream_responses[
                    tx.downstream_mail_response]
                tx.rcpt_response.extend(
                    [None] * (len(tx.rcpt_to) - len(tx.rcpt_response)))
            upstream_rcpt_ok = any_rcpt_ok(tx.rcpt_response)
            sf_rcpt = False
            for j,rcpt in enumerate(tx.rcpt_to):
                if j >= len(tx.rcpt_response):
                    rcpt_resp = None
                else:
                    rcpt_resp = tx.rcpt_response[j]
                logging.debug(rcpt_resp)
                if ((j >= len(tx.downstream_rcpt_response) or
                     not tx.downstream_rcpt_response[j]) and
                    (sf_mail or unavail(tx, rcpt_resp, tx.sf_rcpt_timeout[j]))):
                    tx.downstream_rcpt_response.extend(
                        [DownstreamResponse.NONE] *
                        (len(tx.rcpt_to) -
                         len(tx.downstream_rcpt_response)))
                    tx.downstream_rcpt_response[j] = DownstreamResponse.SF
                # convert DownstreamResponse enum to Response
                if (j < len(tx.downstream_rcpt_response) and
                    tx.downstream_rcpt_response[j] is not None):
                    tx.rcpt_response.extend(
                        [None] * (j - len(tx.rcpt_response) + 1))
                    drr = tx.downstream_rcpt_response[j]
                    tx.rcpt_response[j] = downstream_responses[drr]
                    if drr == DownstreamResponse.SF:
                        sf_rcpt = True

            assert (DownstreamResponse.NONE not in
                    tx.downstream_rcpt_response), tx

            # if any rcpt_resp is SF then data_resp is SF
            sf_data = False
            if tx.downstream_data_response is None:
                if (sf_rcpt and tx._body_last()):
                    tx.downstream_data_response = DownstreamResponse.SF
                elif (not tx.downstream_data_response and
                      tx._body_last() and
                      unavail(tx, tx.data_response, tx.sf_data_timeout)):
                    sf_data = True
                    tx.downstream_data_response = DownstreamResponse.SF
            logging.debug(tx.downstream_data_response)
            if tx.downstream_data_response:
                tx.data_response = downstream_responses[
                    tx.downstream_data_response]

            prev_downstream_resp = TransactionMetadata()
            downstream_resp = TransactionMetadata()

            # compute the delta of just the downstream resp fields to write
            # prev -> prev_downstream_resp
            # tx -> downstream_resp
            for t,dr in (prev,prev_downstream_resp),(tx,downstream_resp):
                dr.downstream_mail_response = t.downstream_mail_response
                dr.downstream_rcpt_response = t.downstream_rcpt_response
                dr.downstream_data_response = t.downstream_data_response
            downstream_resp_delta = prev_downstream_resp.delta(
                downstream_resp)

            if downstream_resp_delta:
                if (downstream_resp_delta.downstream_data_response ==
                    DownstreamResponse.SF):
                    downstream_resp_delta.retry = {}
                    downstream_resp_delta.notification = {}
                self.group_cursor.tx_cursors[i].write_envelope(
                    downstream_resp_delta)

            if sf_data:
                sf_cursor.append(self.group_cursor.tx_cursors[i])
            logging.debug(tx)
        return sf_cursor

    # Fans in upstream tx from group_cursor into a single downstream
    # view. Handling of upstream temp errors is per sf_mode:
    # upstream_unavailability ~ smtp submission/msa:
    #   covert upstream temp errors to 250 store&forward
    # mixed_data_response ~ smtp ingress/relay:
    #   return upstream temp errors verbatim
    # Then if all upstream data_responses have the same major code, we
    # return that directly. Otherwise we enable notification/retry on
    # the upstream transactions that didn't already succeed and return
    # a 250 store&forward data_response downstream.
    def _get(self) -> Optional[TransactionMetadata]:
        assert self.group_cursor is not None
        assert self.group_cursor.tx_cursors[0].tx is not None

        assert self.endpoint_yaml is not None
        if self.sf_mode is None:
            assert len(self.group_cursor.tx_cursors) <= 1, [
                c.db_id for c in self.group_cursor.tx_cursors]
        sf_unavail = self.sf_mode == 'upstream_unavailability'

        def copy_tx(c):
            assert c.tx is not None
            return c.tx.copy()
        group_tx = [copy_tx(c) for c in self.group_cursor.tx_cursors]
        assert self.group_cursor.tx_cursors[0].db_id is not None

        # convert upstream timeout/temp err to 250 s&f resp
        sf_cursor = []
        if sf_unavail:
            sf_cursor = self._do_sf_unavail(group_tx)
        tx = group_tx[0].copy()
        # TODO upstream mail response is most likely 250 noop from
        # rcpt router. If the config can return a real upstream
        # response here (unlikely: only with pipelining or no rcpt
        # routing/static outbound gw), any mail_response err will end
        # the tx.
        for cursor in self.group_cursor.tx_cursors[1:]:
            assert cursor.tx is not None
            tx.rcpt_to.extend(cursor.tx.rcpt_to)
            tx.rcpt_response.extend(cursor.tx.rcpt_response)
        tx.data_response = None

        # NOTE Normally, output flow returns a timeout response if the
        # upstream timed out. If it was terminated by an exception, it
        # will probably set a 450 internal error response in the
        # finally: at the end of OutputHandler. If it just hangs, this
        # will eventually cause an Executor watchdog timeout. The
        # downstream rest client must implement its own timeouts.

        # rcpt_response must be returned in order
        for i,rr in enumerate(tx.rcpt_response):
            if rr is None:
                tx.rcpt_response = tx.rcpt_response[0:i]
                break
        assert None not in tx.rcpt_response

        data_resp = None
        for t in group_tx:
            # could do a consistency check vs the other data_responses here
            if t.data_response is not None and t.data_response.group_reject:
                tx.data_response = Response(
                    t.data_response.code,
                    t.data_response.message + ' (SWF group reject)')
                return tx

        if not tx._body_last() or any(
                [t.data_response is None for t in group_tx]):
            logging.debug('still waiting data_response')
            return tx

        if len(group_tx) == 1 and len(group_tx[0].rcpt_to) == 1:
            tx.data_response = group_tx[0].data_response
            logging.debug('single rcpt data resp')
            return tx

        data_resp_tx = reduce(
            StorageWriterFilter.reduce_data_response, group_tx)
        logging.debug(data_resp_tx)
        data_resp = (data_resp_tx.data_response if data_resp_tx is not None
                     else None)
        if data_resp is not None:
            if len(self.group_cursor.tx_cursors) > 1:
                data_resp = Response(
                    data_resp.code,
                    data_resp.message + ' (SWF Exploder same response)')
        tx.data_response = data_resp

        if tx.data_response is None:  # mixed
            for i,txi in enumerate(group_tx):
                cursor = self.group_cursor.tx_cursors[i]
                if cursor in sf_cursor:
                    continue
                def ok(r):
                    assert r is not None
                    return r.ok()
                txdr = txi.data_response
                if (ok(tx.mail_response) and
                    any([not ok(r) for r in tx.rcpt_response])
                    or not ok(txi.data_response)):
                    sf_cursor.append(cursor)

        # NOTE there is a bit of a "if a tree falls in a
        # forest..." flavor to this: we don't actually enable
        # retries until the client does a GET that returns the 250
        # s&f response. Formally, the update to input_done would
        # start a timer that triggers this.
        logging.debug([c.tx for c in sf_cursor])
        def needs_retry(c):
            assert c.tx is not None
            # would have early-returned if still waiting for timeout (above)
            assert c.tx.data_response is not None
            return not c.tx.data_response.ok() and (
                not c.tx.notification or not c.tx.retrty)
        sf_cursor = [c for c in sf_cursor if needs_retry(c) ]
        if sf_cursor:
            logging.debug(sf_cursor)
            self.group_cursor.update_all(
                TransactionMetadata(
                    retry = {},
                    notification={}),
                cursors=sf_cursor)

            tx.data_response = Response(
                250, 'message accepted (SWF store&forward mixed upstream)')
        logging.debug(tx)
        return tx

    # AsyncFilter
    def update(self,
               tx : TransactionMetadata,
               tx_delta : TransactionMetadata
               ) -> Optional[TransactionMetadata]:
        needs_create = self.rest_id is None and self.create_leased
        try:
            upstream_delta = self._update(tx, tx_delta)
            assert upstream_delta is not None
            # xxx update to group_cursor
            # if self.tx_cursor is not None:
            #     assert self.tx_cursor.version is not None
            return upstream_delta
        finally:
            if needs_create and self.upstream_cursor is None:
                # i.e. uncaught exception
                with self.mu:
                    self.create_err = True
                    self.cv.notify_all()

    # caller handles VersionConflictException -> http 412
    def _update(self,
               tx : TransactionMetadata,
               tx_delta : TransactionMetadata
               ) -> Optional[TransactionMetadata]:
        # TODO this currently always returns an empty delta, probably
        # it should snapshot the tx before write_envelope() and return
        # the delta from the final cursor.tx, it's possible the
        # version conflict retry paths could pick up upstream deltas?
        logging.debug('StorageWriterFilter._update tx %s %s',
                      self.rest_id, tx)
        logging.debug('StorageWriterFilter._update tx_delta %s %s',
                      self.rest_id, tx_delta)

        self._maybe_load_endpoint_yaml(tx.sender)

        if tx_delta.cancelled:
            assert self.group_cursor is not None
            self.group_cursor.update_all(
                tx_delta, final_attempt_reason='downstream cancelled')
            return TransactionMetadata()

        if not tx_delta:  # heartbeat
            assert self.group_cursor is not None
            self.group_cursor.update_all(
                TransactionMetadata(), ping_tx=True, max_conflict_retries=1)
            return TransactionMetadata()

        downstream_tx = tx.copy()
        downstream_delta = tx_delta.copy()
        if getattr(downstream_tx, 'rest_id', None) is not None:
            del downstream_tx.rest_id

        created = False
        if self.rest_id is None:
            created = True
            self._create(downstream_tx)
            assert self.group_cursor is not None
            tx.rest_id = self.rest_id
        else:
            assert self.group_cursor is not None
            # TODO do this in a batch if we get multiple (moot
            # without pipelining)
            assert self.group_cursor.tx_cursors[0].tx is not None
            logging.debug(self.group_cursor.tx_cursors[0].tx)
            # xxx there was a bug in AsyncFilterAdapter where it was
            # retrying after it succeeded the first time... possibly
            # this should keep a copy of the downstream multi-rcpt tx
            # to make sure the deltas make sense
            if (self.sf_mode is None or
                (downstream_delta.rcpt_to and
                 not self.group_cursor.tx_cursors[0].tx.rcpt_to)):
                delta = tx_delta.copy()
                if self.sf_mode is None:
                    downstream_delta = TransactionMetadata()
                else:
                    delta.sf_rcpt_timeout = [self._timeout(delta)] * len(delta.rcpt_to)
                if delta.rcpt_to:
                    delta.rcpt_to = [delta.rcpt_to[0]]
                self.group_cursor.tx_cursors[0].write_envelope(
                    delta, max_conflict_retries=1)
                downstream_delta.rcpt_to = downstream_delta.rcpt_to[1:]
                # xxx rcpt_to_list_offset?

            for rcpt in downstream_delta.rcpt_to:
                assert rcpt is not None
                # This creates the db tx before we know if the
                # executor is going to overflow (below). We could try
                # to schedule the upstream and if that fails, record
                # the fact in tx0 but I think all that saves is the
                # row and not the write and creates some additional
                # complexity in that the rcpt vector in tx0 may become
                # sparse as a result of this.
                delta = TransactionMetadata(rcpt_to=[rcpt])
                # xxx refactor
                timeout = self._timeout(delta)
                delta.sf_mail_timeout = timeout
                delta.sf_rcpt_timeout = [timeout]
                self.group_cursor.clone_tx(
                    delta, create_leased=self.create_leased)
                assert self.tx_group is not None
                self.tx_group.tx_cursors.append(
                    self.group_cursor.tx_cursors[-1].clone())

                # callable to top-level router to start handler
                # if this fails executor overflow, write rcpt_resp 450
                # server busy, etc.
                assert self.tx_handler is not None
                assert self.sender is not None
                if not (sched := self.tx_handler(
                        self.sender,
                        partial(self.release_transaction_cursor,
                                len(self.group_cursor.tx_cursors) - 1))):
                    cursor = self.group_cursor.tx_cursors[-1]
                    cursor.start_attempt()
                    # TODO option to s&f on this err?
                    cursor.write_envelope(
                        TransactionMetadata(),
                        attempt_delta=TransactionMetadata(
                            mail_response=Response(250, 'swf fastfail'),
                            rcpt_response=[
                                Response(451, '4.5.3 too many recipients '
                                         '(SWF could not schedule upstream)')]),
                        finalize_attempt=True,
                        max_conflict_retries=1,
                        final_attempt_reason='SWF fastfail')
                else:
                    created = True
                logging.debug(sched)
            downstream_delta.rcpt_to = []
            if downstream_delta:
                if downstream_delta.body is not None and downstream_delta._body_last():
                    downstream_delta.sf_data_timeout = self._timeout(downstream_delta)

                self.group_cursor.update_all(downstream_delta,
                                             max_conflict_retries=1)

        logging.debug('StorageWriterFilter.update %s result %s',
                      self.rest_id, [c.tx for c in self.group_cursor.tx_cursors])

        logging.debug('input tx %s', tx)

        # TODO how often is the cursor in this SWF reused after return
        # from this call?
        if created and self.create_leased:
            logging.debug(self.upstream_cursor)
            logging.debug(self.group_cursor.tx_cursors)
            with self.mu:
                for i in range(len(self.upstream_cursor),
                               len(self.group_cursor.tx_cursors)):
                    cursor = self.group_cursor.tx_cursors[i]
                    self.upstream_cursor.append(cursor)
                    self.group_cursor.tx_cursors[i] = cursor.clone()
                self.cv.notify_all()

        return TransactionMetadata()

    class BlobWriter(WritableBlob):
        blob : WritableBlob
        parent : 'StorageWriterFilter'
        def __init__(self, parent, blob):
            assert blob is not None
            self.parent = parent
            self.blob = blob
        def len(self) -> int:
            return self.blob.len()
        def rest_id(self) -> Optional[str]:
            return self.blob.rest_id()
        def session_url(self) -> Optional[str]:
            return self.blob.session_uri()

        def append_data(self, offset : int, d : bytes,
                        content_length : Optional[int] = None
                        ) -> Tuple[bool, int, Optional[int]]:
            res = self.blob.append_data(
                offset, d, content_length=content_length)
            appended, length, content_length = res
            # reset timeout
            # xxx: need this condition?
            if appended and length == content_length:
                self.parent.blob_done(self)
            return appended, length, content_length


    def blob_done(self, writer) -> None:
        delta = TransactionMetadata()
        if self.sf_mode is not None:
            delta.sf_data_timeout = self._timeout(
                TransactionMetadata(body=writer.blob))
        assert self.group_cursor is not None
        self.group_cursor.blob_done(writer.blob, delta)

    def get_blob_writer(self,
                        create : bool,
                        blob_rest_id : Optional[str] = None,
                        tx_body : Optional[bool] = None
                        ) -> Optional[WritableBlob]:
        if not self._load():
            return None
        assert self.group_cursor is not None
        assert self.group_cursor.tx_cursors[0].tx is not None
        assert self.rest_id is not None
        assert tx_body or blob_rest_id

        if create:
            assert tx_body
            body = self.group_cursor.tx_cursors[0].tx.body
            if isinstance(body, WritableBlob):
                return StorageWriterFilter.BlobWriter(self, body)
            else:
                assert body is None
            self.group_cursor.update_all(
                TransactionMetadata(body=BlobSpec(create_tx_body=True)))

        blob_writer = self.group_cursor.tx_cursors[0].get_blob_for_append(
            BlobUri(tx_id=self.rest_id, blob=blob_rest_id,
                    tx_body=tx_body if tx_body else False))
        if blob_writer is None:
            return None
        return StorageWriterFilter.BlobWriter(self, blob_writer)


    def check_cache(self) -> Optional[AsyncFilter.CheckTxResult]:
        if not self._load(cache_only=True):
            return None
        tx = self._get()
        v = self.version
        assert v is not None
        return (v, tx, True, None)

    def check(self) -> Optional[AsyncFilter.CheckTxResult]:
        if not self._load():
            return None

        assert self.group_cursor is not None
        res = self.group_cursor.tx_cursors[0].check()
        logging.debug('%s %s', self.rest_id, res)
        if res is None:
            return None
        leased, other_session = res
        assert self.version is not None
        return (self.version, None, leased, other_session)
