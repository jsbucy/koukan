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
    Storage,
    TransactionCursor,
    TransactionGroup )
from koukan.storage_schema import BlobSpec, VersionConflictException
from koukan.response import Response
from koukan.filter import (
    AsyncFilter,
    HostPort,
    Mailbox,
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

class Timeouts:
    # xxx this should get saved in the cached tx? will need this for
    # cross-tx coordination (e.g. spam reject)
    # so load the rest id from the cache and then the rest hang off of that?
    groups : Dict[str, List[int]]
    def __init__(self):
        # self.tx_status = {}
        self.groups = {}

class StorageWriterFilter(AsyncFilter):
    storage : Storage
    tx_cursor : Optional[TransactionCursor] = None
    tx_group : Optional[TransactionGroup] = None
    rest_id_factory : Optional[Callable[[], str]] = None
    rest_id : Optional[str] = None
    create_leased : bool = False
    # leased cursor for cutthrough
    upstream_cursor : List[Optional[TransactionCursor]]
    create_err : bool = False
    mu : Lock
    cv : Condition
    endpoint_yaml : Optional[EndpointYamlProvider] = None
    sender : Optional[Sender] = None
    tx_handler : Optional[TxHandler]
    timeouts : Optional[Timeouts] = None

    def __init__(self, storage,
                 rest_id_factory : Optional[Callable[[], str]] = None,
                 rest_id : Optional[str] = None,
                 create_leased : bool = False,
                 sender : Optional[Sender] = None,
                 endpoint_yaml : Optional[EndpointYamlProvider] = None,
                 tx_handler : Optional[TxHandler] = None,
                 timeouts : Optional[Timeouts] = None):
        self.storage = storage
        self.rest_id_factory = rest_id_factory
        self.rest_id = rest_id
        self.create_leased = create_leased
        self.mu = Lock()
        self.cv = Condition(self.mu)
        self.sender = sender
        self.endpoint_yaml = endpoint_yaml
        self.tx_handler = tx_handler
        self.upstream_cursor = []
        self.timeouts = timeouts

    def incremental(self):
        assert self.endpoint_yaml is not None
        yaml = self.endpoint_yaml(self.sender)
        assert yaml is not None
        return 'sf_mode' in yaml

    # AsyncFilter
    def wait(self, version, timeout
             ) -> Tuple[bool, Optional[TransactionMetadata]]:
        # cursor can be None after first update() to create with the
        # cutthrough/handoff workflow
        if self.tx_cursor is None:
            self._load()
            assert self.tx_cursor is not None
        clone = False
        if self.tx_cursor.version == version:
            rv, clone = self.tx_cursor.wait(timeout, clone=True)
        else:
            rv = True
        tx_out = None
        if rv and clone:
            assert self.tx_cursor.tx is not None
            tx_out = self.tx_cursor.tx.copy()
        return rv, tx_out

    # AsyncFilter
    async def wait_async(self, version, timeout
                         ) -> Tuple[bool, Optional[TransactionMetadata]]:
        assert self.tx_group is not None
        if self.version != version:
            return True, None
        logging.debug(version)
        logging.debug([c.version for c in self.tx_group.tx_cursors])
        res = await self.tx_group.wait_async(timeout)
        logging.debug(res)
        if res is None:
            return False, None
        success, cloned = res
        logging.debug([c.version for c in self.tx_group.tx_cursors])
        assert success
        return True, self._get() if cloned else None

    def release_transaction_cursor(
            self, i : int) -> Optional[TransactionCursor]:
        with self.mu:
            if not self.cv.wait_for(
                    lambda: len(self.upstream_cursor) > i and self.upstream_cursor[i] is not None or
                    self.create_err, 3):
                logging.debug(self.upstream_cursor)
                logging.warning(
                    'StorageWriterFilter.get_transaction_cursor timeout %s',
                    self.create_err)
                return None
            elif len(self.upstream_cursor) <= i or self.upstream_cursor[i] is None:
                return None
            logging.debug('StorageWriterFilter.release_transaction_cursor')
            cursor = self.upstream_cursor[i]
            self.upstream_cursor[i] = None
            return cursor

    @property
    def version(self) -> Optional[int]:
        if self.tx_group is None:
            return None
        version = 0
        for i in self.tx_group.tx_cursors:
            assert i.tx is not None
            assert i.version is not None
            version += i.version
        return version


    def _timeout(self, secs):
        return int(time.time_ns()/1e6 + 1000)  # xxx secs * 1e3)

    def _create(self, tx : TransactionMetadata):
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
        for i in range(0,1):
            if self.endpoint_yaml is None:
                break
            assert self.sender is not None
            if (endpoint_yaml := self.endpoint_yaml(self.sender)) is None:
                break
            if (output_yaml := endpoint_yaml.get('output_handler', None)) is None:
                break
            if self.sender.yaml:
                if self.sender.yaml.get('retry', None) == 'output_chain':
                    storage_tx.retry = {}
                if self.sender.yaml.get('notification', None) == 'output_chain':
                    storage_tx.notification = {}
        to = self._timeout(30)
        storage_tx.sf_mail_timeout = to
        if storage_tx.rcpt_to:
            storage_tx.sf_rcpt_timeout = [to] * len(storage_tx.rcpt_to)
        tx_cursor.create(rest_id, storage_tx,
                         create_leased=self.create_leased)
        self.tx_group = TransactionGroup(self.storage, tx_cursor)

        self.rest_id = rest_id  # XXX locking below?

        if rcpt_to:
            prev = storage_tx.copy()
            storage_tx.rcpt_to.extend(rcpt_to)
            self._update(storage_tx, prev.delta(storage_tx))

        with self.mu:
            # self.rest_id = rest_id
            self.cv.notify_all()

    # xxx _maybe_load()?
    def _load(self) -> bool:
        logging.debug(self.rest_id)
        tx = None
        if self.tx_group is None:
            self.tx_group = TransactionGroup(self.storage)
        assert self.timeouts is not None
        assert self.rest_id is not None
        if (group_db_ids := self.timeouts.groups.get(self.rest_id, None)) is not None:
            if self.tx_group.try_cache(group_db_ids):
                tx = self._get()

        if tx is None:  #not self.tx_group.tx_cursors:
            if not self.tx_group.load(tx_rest_id=self.rest_id):
                return False
            tx = self._get()

        logging.debug('%s %s %s', self.rest_id, self.tx_group.db_id_versions(), tx)

        if not self.tx_group.tx_cursors:  # 404 e.g. after GC
            return False
        if self.sender is None:
            assert self.tx_group.tx_cursors[0].tx is not None
            self.sender = self.tx_group.tx_cursors[0].tx.sender
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

    def _do_sf_unavail(self, group_tx) -> List[TransactionCursor]:
        assert self.tx_group is not None
        now = time.time_ns()/1e6

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
                self.tx_group.tx_cursors[i].write_envelope(
                    downstream_resp_delta)

            if sf_data:
                sf_cursor.append(self.tx_group.tx_cursors[i])
            logging.debug(tx)
        return sf_cursor

    def _get(self) -> Optional[TransactionMetadata]:
        assert self.tx_group is not None
        assert self.tx_group.tx_cursors[0].tx is not None

        if self.timeouts is not None:
            assert self.rest_id is not None
            self.timeouts.groups[self.rest_id] = self.tx_group.db_ids()

        assert self.endpoint_yaml is not None
        if self.sender is None:
            assert self.tx_group.tx_cursors[0].tx is not None
            self.sender = self.tx_group.tx_cursors[0].tx.sender

        assert self.sender is not None
        endpoint_yaml = self.endpoint_yaml(self.sender)
        sf_mode = None
        if endpoint_yaml is not None:
            sf_mode = endpoint_yaml.get('sf_mode', None)
            if sf_mode:
                assert sf_mode in ['upstream_unavailability',  # ~submission
                                   'mixed_data_response']      # ~interchange
            else:
                assert len(self.tx_group.tx_cursors) <= 1, [
                    c.db_id for c in self.tx_group.tx_cursors]
        sf_unavail = sf_mode == 'upstream_unavailability'

        def copy_tx(c):
            assert c.tx is not None
            return c.tx.copy()
        group_tx = [copy_tx(c) for c in self.tx_group.tx_cursors]

        assert self.tx_group.tx_cursors[0].db_id is not None

        # convert upstream timeout/temp err to 250 s&f resp
        sf_cursor = []
        if sf_unavail:
            sf_cursor = self._do_sf_unavail(group_tx)

        tx = group_tx[0].copy()
        # TODO upstream mail response is most likely 250 noop from rcpt
        # router; if the config can return a real upstream response
        # here (unlikely: only with no rcpt routing/static outbound
        # gw), any mail_response err will end the tx
        for cursor in self.tx_group.tx_cursors[1:]:
            assert cursor.tx is not None
            tx.rcpt_to.extend(cursor.tx.rcpt_to)
            tx.rcpt_response.extend(cursor.tx.rcpt_response)
        tx.data_response = None
        # xxx final_attempt_reason
        logging.debug(tx)
        logging.debug(group_tx)

        # TODO Normally, output flow returns a timeout response if the
        # upstream timed out however it could be e.g. terminated by an
        # exception in which case it's nicer behavior to return a
        # response downstream rather than hanging.
        # upstream_bug_timeout = False

        # rcpt_response must be returned in order
        for i,rr in enumerate(tx.rcpt_response):
            if rr is None:
                tx.rcpt_response = tx.rcpt_response[0:i]
                break
        assert None not in tx.rcpt_response

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
            if len(self.tx_group.tx_cursors) > 1:
                data_resp = Response(
                    data_resp.code,
                    data_resp.message + ' (SWF Exploder same response)')
        tx.data_response = data_resp

        if tx.data_response is None:  # mixed
            for i,tx in enumerate(group_tx):
                cursor = self.tx_group.tx_cursors[i]
                if cursor in sf_cursor:
                    continue
                def ok(r):
                    assert r is not None
                    return r.ok()
                txdr = tx.data_response
                if (ok(tx.mail_response) and
                    any([not ok(r) for r in tx.rcpt_response])
                    or not ok(tx.data_response)):
                    sf_cursor.append(cursor)

        # NOTE there is a bit of a "if a tree falls in a
        # forest..." flavor to this: we don't actually enable
        # retries until the client does a GET that returns the 250
        # s&f response. Formally, the update to input_done would
        # start a timer that triggers this.
        logging.debug([c.tx for c in sf_cursor])
        def needs_retry(c):
            assert c.tx is not None
            return c.tx.data_response is None and (
                not c.tx.notification or not c.tx.retrty)
        sf_cursor = [c for c in sf_cursor if needs_retry(c) ]
        if sf_cursor:
            logging.debug(sf_cursor)
            self.tx_group.update_all(
                TransactionMetadata(
                    retry = {},
                    notification={}),
                cursors=sf_cursor)

            tx.data_response = Response(
                250, 'message accepted (SWF store&forward mixed upstream)')
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
            if self.tx_cursor is not None:
                assert self.tx_cursor.version is not None
            return upstream_delta
        finally:
            if needs_create and self.upstream_cursor is None:
                # i.e. uncaught exception
                with self.mu:
                    self.create_err = True
                    self.cv.notify_all()

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

        if tx_delta.cancelled:
            assert self.tx_group is not None
            for i in range(0,5):
                try:
                    # XXX TransactionGroup doesn't yet replicate
                    # TxCursor special-case logic to noop if tx has
                    # final_attempt_reason
                    self.tx_group.update_all(
                        tx_delta, final_attempt_reason='downstream cancelled')
                    break
                except VersionConflictException:
                    logging.debug('VersionConflictException')
                    if i == 4:
                        raise
                    backoff(i)
                    self.tx_group.load()
            assert self.tx_group is not None
            return TransactionMetadata()

        if not tx_delta:  # heartbeat
            assert self.tx_cursor is not None
            self.tx_cursor.write_envelope(TransactionMetadata(), ping_tx=True)
            return TransactionMetadata()

        downstream_tx = tx.copy()
        downstream_delta = tx_delta.copy()
        if getattr(downstream_tx, 'rest_id', None) is not None:
            del downstream_tx.rest_id

        created = False
        if self.rest_id is None:
            created = True
            self._create(downstream_tx)
            assert self.tx_group is not None
            tx.rest_id = self.rest_id
        else:
            if self.tx_group is None:
                self._load()
                assert self.tx_group is not None
            # TODO do this in a batch if we get multiple (moot
            # without pipelining)
            assert self.tx_group.tx_cursors[0].tx is not None
            logging.debug(self.tx_group.tx_cursors[0].tx)
            if (downstream_delta.rcpt_to and
                not self.tx_group.tx_cursors[0].tx.rcpt_to):
                rcpt = downstream_delta.rcpt_to[0]
                assert rcpt is not None
                delta = TransactionMetadata(rcpt_to=[rcpt])
                delta.sf_rcpt_timeout = [self._timeout(30)] * len(delta.rcpt_to)
                self.tx_group.tx_cursors[0].write_envelope(delta)
                downstream_delta.rcpt_to = downstream_delta.rcpt_to[1:]
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
                to = self._timeout(30)
                delta.sf_mail_timeout = to
                delta.sf_rcpt_timeout = [to]
                self.tx_group.clone_tx(delta,
                                       create_leased=self.create_leased)
                # callable to top-level router to start handler
                # if this fails executor overflow, write rcpt_resp 450
                # server busy, etc.
                assert self.tx_handler is not None
                assert self.sender is not None
                if not (sched := self.tx_handler(
                        self.sender,
                        partial(self.release_transaction_cursor,
                                len(self.tx_group.tx_cursors) - 1))):
                    cursor = self.tx_group.tx_cursors[-1]
                    # TODO combine these writes?
                    cursor.start_attempt()
                    # TODO option to s&f on this err?
                    cursor.write_envelope(TransactionMetadata(
                        rcpt_response=[
                            Response(451, '4.5.3 too many recipients '
                                     '(SWF could not schedule upstream)')]),
                        finalize_attempt=True)
                    # xxx final_attempt_reason=?
                else:
                    created = True
                logging.debug(sched)
            assert self.timeouts is not None
            self.timeouts.groups[self.rest_id] = self.tx_group.db_ids()
            downstream_delta.rcpt_to = []
            if downstream_delta:
                if downstream_delta.body is not None and downstream_delta._body_last():
                    downstream_delta.sf_data_timeout = self._timeout(30)

                # caller handles VersionConflictException
                self.tx_group.update_all(downstream_delta)

        logging.debug('StorageWriterFilter.update %s result %s',
                      self.rest_id, [c.tx for c in self.tx_group.tx_cursors])

        logging.debug('input tx %s', tx)

        assert self.endpoint_yaml is not None
        assert self.sender is not None
        if ((self.timeouts is not None) and
            ((endpoint_yaml := self.endpoint_yaml(self.sender)) is not None) and
            (endpoint_yaml.get('sf_mode', None) == 'upstream_unavailability')):
            timeout = endpoint_yaml.get('sf_timeout', 10)
            assert self.tx_group.tx_cursors[0].db_id is not None
            # self.timeouts.tx_status[self.tx_group.tx_cursors[0].db_id] = (
            #     Status(time.monotonic_ns() + timeout * int(1e9)))

        # TODO how often is the cursor in this SWF reused after return
        # from this call?
        if created and self.create_leased:
            logging.debug(self.upstream_cursor)
            logging.debug(self.tx_group.tx_cursors)
            with self.mu:
                for i in range(len(self.upstream_cursor),
                               len(self.tx_group.tx_cursors)):
                    cursor = self.tx_group.tx_cursors[i]
                    self.upstream_cursor.append(cursor)
                    self.tx_group.tx_cursors[i] = cursor.clone()
                self.cv.notify_all()

        return TransactionMetadata()

    class BlobWriter(WritableBlob):
        def __init__(self, parent, blob):
            self.parent = parent
            self.blob = blob
        def len(self):
            return self.blob.len()
        def rest_id(self):
            return self.blob.rest_id()
        def session_url(self):
            return self.blob.session_uri()

        def append_data(self, offset : int, d : bytes,
                        content_length : Optional[int] = None
                        ) -> Tuple[bool, int, Optional[int]]:
            res = self.blob.append_data(
                offset, d, content_length=content_length, update_tx=False)
            appended, length, content_length = res
            # reset timeout
            # xxx: need this condition?
            if appended and length == content_length:
                self.parent.blob_done(self)
            return appended, length, content_length


    def blob_done(self, writer):
        delta = TransactionMetadata()
        # xxx refactor
        delta.sf_data_timeout = self._timeout(30)
        self.tx_group.blob_done(writer.blob, delta)

    def get_blob_writer(self,
                        create : bool,
                        blob_rest_id : Optional[str] = None,
                        tx_body : Optional[bool] = None
                        ) -> Optional[WritableBlob]:
        if not self._load():
            return None
        assert self.tx_group is not None
        assert self.tx_group.tx_cursors[0].tx is not None
        assert self.rest_id is not None
        assert tx_body or blob_rest_id

        if create:
            assert tx_body
            for i in range(0,5):
                try:
                    body = self.tx_group.tx_cursors[0].tx.body
                    if isinstance(body, WritableBlob):
                        return StorageWriterFilter.BlobWriter(self, body)
                    else:
                        assert body is None
                    self.tx_group.update_all(
                        TransactionMetadata(body=BlobSpec(create_tx_body=True)))
                    break
                except VersionConflictException:
                    logging.debug('VersionConflictException')
                    if i == 4:
                        raise
                    backoff(i)
                    self.tx_group.load()

        return StorageWriterFilter.BlobWriter(
            self,
            self.tx_group.tx_cursors[0].get_blob_for_append(
                BlobUri(tx_id=self.rest_id, blob=blob_rest_id,
                        tx_body=tx_body if tx_body else False)))


    def check_cache(self) -> Optional[AsyncFilter.CheckTxResult]:
        if self.tx_group is None:
            assert self.timeouts is not None
            assert self.rest_id is not None
            if (group_db_ids := self.timeouts.groups.get(self.rest_id, None)) is None:
                return None
            self.tx_group = TransactionGroup(self.storage)
            if not self.tx_group.try_cache(group_db_ids):
                return None

        tx = self._get()
        v = self.version
        assert v is not None
        return (v, tx, True, None)

    def check(self) -> Optional[AsyncFilter.CheckTxResult]:
        if not self._load():
            return None

        assert self.tx_group is not None
        res = self.tx_group.tx_cursors[0].check()
        logging.debug('%s %s', self.rest_id, res)
        if res is None:
            return None
        leased, other_session = res
        assert self.version is not None
        return (self.version, None, leased, other_session)
