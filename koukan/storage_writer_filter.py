# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
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

class Status:
    # corresponds to downstream tx.rcpt_response: have we returned an
    # s&f response for each rcpt?
    rcpts : List[bool]
    timeout : int  # time.monotonic_ns()
    def __init__(self, timeout):
        self.rcpts = []
        self.timeout = timeout

class Timeouts:
    # TODO BEFORE MERGE gc/retirement
    tx_status : Dict[int, Status]
    def __init__(self):
        self.tx_status = {}

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
        tx = self.get()
        if version != self.version:
            return True, tx
        return False, None

        assert self.tx_cursor is not None
        assert self.tx_cursor.version is not None
        logging.debug('%s %s', version, self.tx_cursor.version)
        if self.tx_cursor.version == version:
            rv, clone = await self.tx_cursor.wait_async(timeout, clone=True)
        else:
            rv = True

        tx_out = None
        if rv and clone:
            assert self.tx_cursor.tx is not None
            tx_out = self.tx_cursor.tx.copy()

        return rv, tx_out

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
        # assert self.tx_cursor is not None
        # if self.tx_cursor is None:
        #     return None
        # return self.tx_cursor.version

        if self.tx_group is None:
            return None
        version = 0
        for i in self.tx_group.tx_cursors:
            assert i.tx is not None
            assert i.version is not None
            version += i.version
        return version

    def _create(self, tx : TransactionMetadata):
        # TODO handle
        assert len(tx.rcpt_to) <= 1
        tx_cursor = self.storage.get_transaction_cursor()
        assert self.rest_id_factory is not None
        rest_id = self.rest_id_factory()
        storage_tx = tx.copy()
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

        tx_cursor.create(rest_id, storage_tx,
                         create_leased=self.create_leased)
        self.tx_group = TransactionGroup(self.storage, tx_cursor)
        with self.mu:
            self.rest_id = rest_id
            self.cv.notify_all()

    # xxx _maybe_load()?
    def _load(self) -> bool:
        tx = None
        if self.tx_group is None:
            self.tx_group = TransactionGroup(self.storage)
        # if self.tx_cursor.try_cache() and self.tx_cursor.tx is not None:
        #     tx = self.tx_cursor.tx
        #     logging.debug(tx)
        # else:
        if not self.tx_group.load(tx_rest_id=self.rest_id):
            return False
        # if not self.tx_group.tx_cursors:  # 404 e.g. after GC
        #     return
        if self.sender is None:
            assert self.tx_group.tx_cursors[0].tx is not None
            self.sender = self.tx_group.tx_cursors[0].tx.sender
        return True

    @staticmethod
    def reduce_data_response(
            lhs : Optional[TransactionCursor],
            rhs : Optional[TransactionCursor]
    ) -> Optional[TransactionCursor]:
        if lhs is None:
            return None
        assert lhs is not None
        assert lhs.tx is not None
        if not lhs.tx.rcpt_response or lhs.tx.rcpt_response[0] is None:
            return None
        assert rhs is not None
        assert rhs.tx is not None
        if not rhs.tx.rcpt_response or rhs.tx.rcpt_response[0] is None:
            return None
        if lhs.tx.rcpt_response[0].err():
            return rhs
        if rhs.tx.rcpt_response[0].err():
            return lhs
        if (lhs.tx.data_response is None) or (rhs.tx.data_response is None):
            return None
        if (lhs.tx.data_response.major_code() !=
            rhs.tx.data_response.major_code()):
            return None
        return lhs

    # AsyncFilter
    def get(self) -> Optional[TransactionMetadata]:
        self._load()
        assert self.tx_group is not None
        assert self.tx_group.tx_cursors[0].tx is not None
        tx = self.tx_group.tx_cursors[0].tx.copy()
        for c in self.tx_group.tx_cursors:
            logging.debug(c.tx)
        for cursor in self.tx_group.tx_cursors[1:]:
            assert cursor.tx is not None
            tx.rcpt_to.extend(cursor.tx.rcpt_to)
            tx.rcpt_response.extend(cursor.tx.rcpt_response)
        logging.debug(tx)

        assert self.endpoint_yaml is not None
        assert self.sender is not None
        endpoint_yaml = self.endpoint_yaml(self.sender)
        sf_mode = None
        if endpoint_yaml is not None:
            sf_mode = endpoint_yaml.get('sf_mode', None)
            if sf_mode:
                assert sf_mode in ['upstream_unavailability',  # ~submission
                                   'mixed_data_response']      # ~interchange
            else:
                assert len(self.tx_group.tx_cursors) <= 1
        sf_unavail = sf_mode == 'upstream_unavailability'

        timeout = False
        # TODO Normally, output flow returns a timeout response if the
        # upstream timed out however it could be e.g. terminated by an
        # exception in which case it's nicer behavior to return a
        # response downstream rather than hanging.
        upstream_bug_timeout = False

        if sf_unavail:
            now = time.monotonic_ns()
            assert self.tx_group.tx_cursors[0].db_id is not None
            tx_status = None
            if (self.timeouts is not None and
                ((tx_status := self.timeouts.tx_status.get(
                    self.tx_group.tx_cursors[0].db_id, None)) is not None)):
                timeout = now > tx_status.timeout

            if tx.mail_from is not None:
                mail_temp = (tx.mail_response is not None and
                             tx.mail_response.temp())
                if (timeout and tx.mail_response is None) or mail_temp:
                    tx.mail_response = Response(
                        250, 'mail ok (SWF store and forward)')

            tx.rcpt_response.extend(
                [None] * (len(tx.rcpt_to) - len(tx.rcpt_response)))
            if tx_status is not None:
                tx_status.rcpts.extend(
                    [False] * (len(tx.rcpt_to) - len(tx_status.rcpts)))
            for i,rcpt in enumerate(tx.rcpt_to):
               rcpt_resp = None
               rcpt_resp = tx.rcpt_response[i]

               rcpt_temp = rcpt_resp is not None and rcpt_resp.temp()
               if (tx_status is not None and
                   (tx_status.rcpts[i] or
                    (timeout and rcpt_resp is None) or
                    rcpt_temp)):
                   tx_status.rcpts[i] = True
                   tx.rcpt_response[i] = Response(
                       250, 'rcpt ok (SWF store and forward)')

                # elif rcpt_resp is None and upstream_bug_timeout:
                #   tx.rcpt_response[i] = Response(450, 'SWF upstream timeout')

        r : Sequence[Optional[TransactionCursor]] = self.tx_group.tx_cursors

        data_resp_cursor : Optional[TransactionCursor] = reduce(
            StorageWriterFilter.reduce_data_response, r)
        logging.debug(data_resp_cursor.tx if data_resp_cursor else None)
        data_resp = None
        if data_resp_cursor is not None:
            assert data_resp_cursor.tx is not None
            data_resp = data_resp_cursor.tx.data_response
        if data_resp and not (sf_unavail and data_resp.temp()):
            if len(self.tx_group.tx_cursors) > 1:
                tx.data_response = Response(
                    data_resp.code,
                    data_resp.message + ' (SWF Exploder same response)')
            else:
                tx.data_response = data_resp
            return tx

        if (tx.body is None) or not tx._body_last():  # XXX
            return tx

        # if upstream_bug_timeout and data_resp is None:
        #     tx.data_response = Response(450, 'SWF upstream timeout')

        elif (sf_unavail and
              (timeout or (data_resp is not None and data_resp.temp()))):
            sf_cursors = []
            for cursor in self.tx_group.tx_cursors:
                rr = None
                assert cursor.tx is not None
                if cursor.tx.rcpt_response:
                    rr = cursor.tx.rcpt_response[0]
                dr = cursor.tx.data_response
                if rr is None or (not rr.perm()) or dr is None or not dr.ok():
                    if (cursor.tx.retry is None or
                        cursor.tx.notification is None):
                        sf_cursors.append(cursor)

            # NOTE there is a bit of a "if a tree falls in a
            # forest..." flavor to this: we don't actually enable
            # retries until the client does a GET that returns the 250
            # s&f response. Formally, the update to input_done would
            # start a timer that triggers this.
            if sf_cursors:
                self.tx_group.update_all(
                    TransactionMetadata(
                        retry = {},
                        notification={}),
                    cursors=sf_cursors)

            tx.data_response = Response(
                250, 'message accepted (SWF store and forward)')

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
                    # storage has special-case logic to noop if
                    # tx has final_attempt_reason
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
            if (downstream_delta.rcpt_to and
                not self.tx_group.tx_cursors[0].tx.rcpt_to):
                rcpt = downstream_delta.rcpt_to[0]
                assert rcpt is not None
                self.tx_group.tx_cursors[0].write_envelope(TransactionMetadata(
                    rcpt_to=[rcpt]))
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
                self.tx_group.clone_tx(TransactionMetadata(rcpt_to=[rcpt]),
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
                    cursor.write_envelope(TransactionMetadata(
                        rcpt_response=[Response(450, 'busy SWF')]),
                        finalize_attempt=True)
                else:
                    created = True
                logging.debug(sched)

            downstream_delta.rcpt_to = []
            if downstream_delta:
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
            self.timeouts.tx_status[self.tx_group.tx_cursors[0].db_id] = (
                Status(time.monotonic_ns() + timeout * int(1e9)))

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
            appended, length, content_length = self.blob.append_data(
                offset, d, content_length=content_length, update_tx=False)
            # reset timeout
            if appended and length == content_length:
                self.parent.blob_done(self)
            return appended, length, content_length


    def blob_done(self, writer):
        bd = False
        for i,c in enumerate(self.tx_group.tx_cursors):
            bdi = c._blob_done(writer.blob)
            if i == 0:
                bd = bdi
            else:
                assert bd == bdi

        logging.debug(bd)
        if bd:
            self.tx_group.update_all(TransactionMetadata(), input_done=True)

    def get_blob_writer(self,
                        create : bool,
                        blob_rest_id : Optional[str] = None,
                        tx_body : Optional[bool] = None
                        ) -> Optional[WritableBlob]:
        self._load()
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
                        return body
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
        return None

        if self.tx_cursor is None:
            self.tx_cursor = self.storage.get_transaction_cursor(
                rest_id=self.rest_id)
        if not self.tx_cursor.try_cache():
            return None
        assert self.tx_cursor.tx is not None
        assert self.tx_cursor.version is not None
        tx = self.tx_cursor.tx.copy()
        return (self.tx_cursor.version, tx, True, None)

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
