# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Callable, List, Optional, Union
import logging
import time
from functools import partial, reduce

from koukan.deadline import Deadline
from koukan.filter import (
    AsyncFilter,
    Mailbox,
    SyncFilter,
    TransactionMetadata,
    WhichJson )
from koukan.blob import Blob
from koukan.response import Response

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
    filter : AsyncFilter
    tx :  Optional[TransactionMetadata] = None

    def __init__(self, filter : AsyncFilter):
        self.filter = filter

    def first_update(self,
                     tx : TransactionMetadata,
                     output_chain : str, i : int):
        self.tx = tx.copy_valid(WhichJson.EXPLODER_CREATE)
        self.tx.host = output_chain
        self.tx.rcpt_to = [tx.rcpt_to[i]]
        self.tx.rcpt_response = []
        return self.filter.update(self.tx, self.tx.copy())

    def update(self, delta : TransactionMetadata
               ) -> Optional[TransactionMetadata]:
        delta = delta.copy_valid(WhichJson.EXPLODER_UPDATE)
        assert self.tx.merge_from(delta) is not None
        if not delta:
            return TransactionMetadata()
        return self.filter.update(self.tx, delta)

    def wait(self, timeout : float):
        self.filter.wait(self.tx.version, timeout)
        t = self.filter.get()
        assert t is not None
        orig = self.tx.copy()
        tt = t.copy()
        for ti in [orig, tt]:
            del ti.version
            del ti.body_blob
        assert orig.delta(tt) is not None  # check buggy filter
        self.tx = t

FilterFactory = Callable[[], Optional[AsyncFilter]]

class Exploder(SyncFilter):
    sync_factory : FilterFactory
    output_chain : str
    # TODO these timeouts move to AsyncFilterWrapper
    rcpt_timeout : Optional[float] = None
    data_timeout : Optional[float] = None
    default_notification : Optional[dict] = None

    rcpt_ok = False
    mail_from : Mailbox = None

    recipients : List[Recipient]

    def __init__(self,
                 output_chain : str,
                 sync_factory : FilterFactory,
                 rcpt_timeout : Optional[float] = None,
                 data_timeout : Optional[float] = None,
                 default_notification : Optional[dict] = None):
        self.sync_factory = sync_factory
        self.output_chain = output_chain
        self.rcpt_timeout = rcpt_timeout
        self.data_timeout = data_timeout
        self.recipients = []
        self.default_notification = default_notification

    @staticmethod
    def reduce_data_response(
            lhs : Union[None, Recipient],
            rhs : Recipient) -> Union[None, Recipient]:
        if lhs is None:
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

    def on_update(self,
                  tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:
        tx_orig = tx.copy()
        tx_delta = tx_delta.copy()

        if tx_delta.mail_from:
            tx.mail_response = tx_delta.mail_response = Response(
                250, 'MAIL ok (exploder noop)')

        for i in range(0, len(tx.rcpt_to)):
            if i >= len(self.recipients):
                rcpt = Recipient(self.sync_factory())
                self.recipients.append(rcpt)
                rcpt.first_update(tx_orig, self.output_chain, i)
            else:
                rcpt = self.recipients[i]
                rcpt.update(tx_delta)

        assert len(self.recipients) == len(tx.rcpt_to)

        rcpts = [ r for r in self.recipients if r.tx.req_inflight() ]

        deadline = Deadline(
            self.data_timeout if tx.body_blob and tx.body_blob.finalized()
            else self.rcpt_timeout)
        while rcpts and deadline.remaining():
            rcpt_next = []
            for rcpt in rcpts:
                rcpt.wait(deadline.deadline_left())
                if rcpt.tx.req_inflight():
                    rcpt_next.append(rcpt)
            rcpts = rcpt_next

        assert not rcpts
        assert not any([r.tx.req_inflight() for r in self.recipients])

        for i,rcpt in enumerate(self.recipients):
            # The only field we directly propagate from the
            # upstream per-rcpt tx to the downstream is rcpt_response.
            if i >= len(tx.rcpt_response):
                tx.rcpt_response.append(rcpt.tx.rcpt_response[0])
        if tx.body_blob is None or tx.data_response is not None:
            return tx_orig.delta(tx)

        # if all rcpts with rcpt_response.ok() have the same
        # data_response.major_code(), return that
        rcpt = reduce(Exploder.reduce_data_response, self.recipients)
        if rcpt is not None and rcpt.tx.data_response is not None:
            tx.data_response = Response(
                rcpt.tx.data_response.code, 'exploder same response')
        elif tx.body_blob.finalized():
            retry_delta = TransactionMetadata(
                retry = {},
                # XXX this will blackhole if unset!
                notification=self.default_notification)
            for rcpt in self.recipients:
                if (rcpt.tx.rcpt_response[0].ok() and
                    (not rcpt.tx.data_response.ok()) and
                    rcpt.tx.retry is None):
                    rcpt.update(retry_delta)

            tx.data_response = Response(250, 'DATA ok (Exploder store&forward)')

        return tx_orig.delta(tx)
