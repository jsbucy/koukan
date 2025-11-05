# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Callable, List, Optional, Sequence, Union
import logging
import time
from functools import partial, reduce

from koukan.deadline import Deadline
from koukan.filter import (
    AsyncFilter,
    Mailbox,
    TransactionMetadata,
    WhichJson )
from koukan.filter_chain import FilterResult, Filter
from koukan.blob import Blob
from koukan.response import Response
from koukan.sender import Sender

from koukan.storage_schema import VersionConflictException

# Fan-out multi-rcpt from smtp gw and fan responses back in.
# Store&forward when smtp requires it i.e. mixed data responses

# This expects to be wired with AsyncFilterWrapper -> StorageWriterFilter
# for the upstream transactions.

# This can be used in 2 modes per msa
# true: store and forward all upstream temp errors
# false: store and forward only on mixed data responses

# TODO options try to avoid accept&bounce smtp extensions i.e. if the
# envelope contains an extension and we time out upstream and would
# have to emit a 2xx response here for store&forward, fail if we
# don't otherwise know that the destination accepts it.

class Recipient:
    filter : Optional[AsyncFilter] = None
    tx :  Optional[TransactionMetadata] = None
    sender : Sender

    def __init__(self, filter : Optional[AsyncFilter], sender : Sender):
        self.filter = filter
        self.sender = sender

    # returns True if this Recipient failed due to not being able to
    # start the upstream tx
    def _check_busy_err(self) -> bool:
        assert self.tx is not None
        if self.filter is not None:
            return False
        # 453-4.3.2 "system not accepting network messages" is also
        # plausible here but maybe more likely for the client to abort
        # the whole transaction rather than do what we want: stop
        # sending rcpts and continue to data.
        assert self.tx.mail_response is None
        self.tx.mail_response = Response(
            250, 'ok (exploder no-op, rcpt will fail)')
        assert not self.tx.rcpt_response
        self.tx.rcpt_response = [
            Response(451, '4.5.3 too many recipients')] * len(self.tx.rcpt_to)
        self.tx.check_preconditions()
        return True

    def first_update(self,
                     tx : TransactionMetadata,
                     i : int):
        self.tx = tx.copy_valid(WhichJson.EXPLODER_CREATE)
        self.tx.sender = self.sender
        self.tx.rcpt_to = [tx.rcpt_to[i]]
        self.tx.rcpt_response = []
        # TODO FilterChainWiring.exploder() passes block_upstream=True to
        # router_service.Service.create_storage_writer() which
        # returns None if _handle_new_tx() couldn't be scheduled on
        # the executor. FilterChainWiring then returns None instead of
        # wrapping the writer with AsyncFilterWrapper. That results in
        # this downstream error. It is possible sites with bursty
        # load and/or msa clients that are very averse to temporary
        # errors may want to store&forward in that situation. In that
        # case create_storage_writer wouldn't error out on the
        # executor overflow and AsyncFilterWrapper probably also wants
        # a hint to set the upstream response -> s&f timeout to 0.
        # TODO router_service.Service.create_storage_writer() has a
        # hard-coded timeout of 0 to schedule the OutputHandler on the
        # Executor. This should probably wait some fraction of the
        # rcpt_timeout.
        if self._check_busy_err():
            return
        assert self.filter is not None
        self.filter.update(self.tx, self.tx.copy())

    def update(self, delta : TransactionMetadata
               ) -> Optional[TransactionMetadata]:
        delta = delta.copy_valid(WhichJson.EXPLODER_UPDATE)
        assert self.tx is not None
        self.tx.merge_from(delta)
        if not delta:
            return TransactionMetadata()
        assert self.filter is not None
        return self.filter.update(self.tx, delta)

    def wait(self, timeout : Optional[float]):
        assert self.tx is not None
        assert self.filter is not None
        old_version = self.filter.version
        rv, t = self.filter.wait(old_version, timeout)
        logging.debug('%s', (rv, t))
        assert not rv or not t or self.filter.version != old_version
        if t is None:
            t = self.filter.get()
            logging.debug(t)
        assert t is not None
        orig = self.tx.copy()
        tt = t.copy()
        for ti in [orig, tt]:
            ti.body = None
        assert orig.maybe_delta(tt) is not None  # check buggy filter
        self.tx = t


FilterFactory = Callable[[], Optional[AsyncFilter]]

class Exploder(Filter):
    upstream_factory : FilterFactory
    # TODO these timeouts move to AsyncFilterWrapper
    rcpt_timeout : Optional[float] = None
    data_timeout : Optional[float] = None

    rcpt_ok = False

    recipients : List[Recipient]
    sender : Sender
    upstream_sender : Sender

    def __init__(self,
                 sender : Sender,
                 upstream_sender : Sender,
                 upstream_factory : FilterFactory,
                 rcpt_timeout : Optional[float] = None,
                 data_timeout : Optional[float] = None):
        self.upstream_factory = upstream_factory
        self.sender = sender
        self.upstream_sender = upstream_sender
        self.rcpt_timeout = rcpt_timeout
        self.data_timeout = data_timeout
        self.recipients = []

    @staticmethod
    def reduce_data_response(
            lhs : Optional[Recipient],
            rhs : Optional[Recipient]
    ) -> Optional[Recipient]:
        if lhs is None:
            return None
        assert lhs.tx is not None
        assert lhs.tx.rcpt_response[0] is not None
        assert rhs is not None
        assert rhs.tx is not None
        assert rhs.tx.rcpt_response[0] is not None
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

    def on_update(self, tx_delta : TransactionMetadata) -> FilterResult:
        tx = self.downstream_tx
        assert tx is not None
        assert tx.retry is None
        assert tx.notification is None

        # NOTE: OutputHandler may send but Storage currently does not
        # accept reusing !finalized blob. Exploder passes it through
        # but AsyncFilterWrapper buffers.

        if tx_delta.mail_from:
            # note: mail_response doesn't have EXPLODER_CREATE
            # validity so won't be copied to the upstream tx. If mail
            # then fails upstream,
            # AsyncFilterWrapper._set_precondition_resp() will return
            # that through rcpt_response.
            tx.mail_response = Response(250, 'MAIL ok (exploder noop)')

        for i in range(0, len(tx.rcpt_to)):
            if i >= len(self.recipients):
                rcpt = Recipient(self.upstream_factory(), self.upstream_sender)
                self.recipients.append(rcpt)
                rcpt.first_update(tx, i)
            else:
                rcpt = self.recipients[i]
                rcpt.update(tx_delta)

        assert len(self.recipients) == len(tx.rcpt_to)

        def inflight(r : Recipient):
            assert r.tx is not None
            return r.tx.req_inflight()
        rcpts = [ r for r in self.recipients if inflight(r) ]

        body = tx.maybe_body_blob()
        deadline = Deadline(
            self.data_timeout if (body is not None) and body.finalized()
            else self.rcpt_timeout)
        while rcpts and deadline.remaining():
            rcpt_next = []
            for rcpt in rcpts:
                dl = deadline.deadline_left()
                rcpt.wait(dl)
                assert rcpt.tx is not None
                if rcpt.tx.req_inflight():
                    rcpt_next.append(rcpt)
            rcpts = rcpt_next

        assert not rcpts
        assert not any([inflight(r) for r in self.recipients])

        # The only fields we directly propagate from the upstream
        # per-rcpt tx to the downstream is rcpt_response and
        # data_response if single-rcpt.
        for i,rcpt in enumerate(self.recipients):
            if i >= len(tx.rcpt_response):
                rtx = rcpt.tx
                assert rtx is not None
                tx.rcpt_response.append(rtx.rcpt_response[0])
        # common case: 1 rcpt, return the upstream data response directly
        if (body is not None and tx.data_response is None and
            len(self.recipients) == 1):
            rcpt = self.recipients[0]
            rtx = rcpt.tx
            assert rtx is not None
            if rtx.data_response is not None:
                tx.data_response = rtx.data_response

        if (body is None) or (tx.data_response is not None):
            return FilterResult()

        # If all rcpts with rcpt_response.ok() have the same
        # data_response.major_code(), return that.  The most likely
        # result is that they're all "250 accepted" or "550 message
        # content rejected". It's possible though probably unlikely
        # that the upstream responses might be different in spite of
        # having the same major code e.g. one recipient is over quota
        # and another recipient doesn't like the content. This may
        # obscure that from the person trying to troubleshoot from the
        # logs though the net result is the same. We could cat them
        # all together but that could be large and I don't think
        # aiosmtpd in the gateway will re-wrap responses automatically.
        r : Sequence[Optional[Recipient]] = self.recipients
        del rcpt
        data_resp_rcpt : Optional[Recipient] = reduce(Exploder.reduce_data_response, r)
        if data_resp_rcpt is not None:
            rtx = data_resp_rcpt.tx
            assert rtx is not None
            if rtx.data_response is not None:
                tx.data_response = Response(
                    rtx.data_response.code,
                    rtx.data_response.message + ' (Exploder same response)')
        elif body.finalized():
            retry_delta = TransactionMetadata(
                retry = {},
                # XXX this will blackhole if unset!
                notification={})
            for rcpt in self.recipients:
                assert rcpt.tx is not None
                assert rcpt.tx.rcpt_response[0] is not None
                assert rcpt.tx.data_response is not None
                if (rcpt.tx.rcpt_response[0].ok() and
                    (not rcpt.tx.data_response.ok()) and
                    rcpt.tx.retry is None):
                    rcpt.update(retry_delta)

            tx.data_response = Response(250, 'DATA ok (Exploder store&forward)')
        return FilterResult()
