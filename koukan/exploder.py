# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Callable, List, Optional, Union
import logging
from threading import Thread
import time
from functools import partial, reduce

from koukan.deadline import Deadline
from koukan.filter import (
    AsyncFilter,
    Mailbox,
    SyncFilter,
    TransactionMetadata )
from koukan.blob import Blob
from koukan.response import Response

from koukan.storage_schema import VersionConflictException

# Fan-out multi-rcpt from smtp gw and fan responses back in.
# Store&forward when smtp requires it i.e. mixed data responses

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
        self.tx = tx.copy()
        self.tx.rest_id = None
        self.tx.tx_db_id = None
        self.tx.mail_response = None
        self.tx.host = output_chain
        self.tx.rcpt_to = [tx.rcpt_to[i]]
        self.tx.rcpt_response = []
        for a in ['version', 'attempt_count']:
            if getattr(self.tx, a, None) is not None:
                delattr(self.tx, a)
        return self.filter.update(self.tx, self.tx.copy())

    def update(self, delta : TransactionMetadata
               ) -> Optional[TransactionMetadata]:
        delta = delta.copy()
        for a in ['version']:
            if getattr(delta, a, None) is not None:
                delattr(delta, a)
        # XXX this field shouldn't be here?
        if hasattr(delta, 'rcpt_to_list_offset'):
            del delta.rcpt_to_list_offset
        delta.rcpt_to = []
        delta.rcpt_response = []
        assert self.tx.merge_from(delta) is not None
        # xxx drop body if prev rcpt/data perm err?
        if not delta:
            logging.debug('noop')
            return TransactionMetadata()
        logging.debug(self.tx)
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
        if lhs is None or lhs.tx.data_response is None:
            return None
        if lhs.tx.data_response.major_code() == rhs.tx.data_response.major_code():
            return lhs

    def on_update(self,
                  tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:
        logging.info('Exploder.on_update %s', tx_delta)

        tx_orig = tx.copy()
        tx_delta = tx_delta.copy()
        # for t in [tx_orig, tx_delta]:
        #     for a in ['version', 'attempt_count']:
        #         if getattr(t, a, None) is not None:
        #             delattr(t, a)

        # TODO the old code had some complicated logic to try to pick
        # the best upstream mail response if we got it pipelined with
        # rcpts (which the aiosmtpd/gw implementation doesn't
        # currently support). This feels kind of academic since the
        # AsyncFilterWrapper precondition check will return any
        # upstream mail err in response to rcpt.

        # TODO -> MailOkFilter
        if tx_delta.mail_from:
            tx.mail_response = tx_delta.mail_response = Response(
                250, 'MAIL ok (exploder noop)')

        for i in range(0, len(tx.rcpt_to)):
            logging.debug(i)

            if i >= len(self.recipients):
                logging.debug(i)
                rcpt = Recipient(self.sync_factory())
                self.recipients.append(rcpt)
                rcpt.first_update(tx_orig, self.output_chain, i)
            else:
                rcpt = self.recipients[i]
                rcpt.update(tx_delta)
            logging.debug(rcpt.tx)
            # TODO need to propagate arbitrary fields from this
            # upstream_delta to downstream_delta?

        assert len(self.recipients) == len(tx.rcpt_to)

        rcpts = [ r for r in self.recipients if r.tx.req_inflight() ]

        deadline = Deadline(
            self.data_timeout if tx.body_blob and tx.body_blob.finalized()
            else self.rcpt_timeout)
        logging.debug('%s %s', rcpts, deadline.deadline_left())
        while rcpts and deadline.remaining():
            logging.debug('%s %s', rcpts, deadline.deadline_left())
            rcpt_next = []
            for rcpt in rcpts:
                rcpt.wait(deadline.deadline_left())
                logging.debug(rcpt.tx)
                if rcpt.tx.req_inflight():
                    rcpt_next.append(rcpt)
            rcpts = rcpt_next

        assert not rcpts
        assert not any([r.tx.req_inflight() for r in self.recipients])

        for i,rcpt in enumerate(self.recipients):
            if i >= len(tx.rcpt_response):
                tx.rcpt_response.append(rcpt.tx.rcpt_response[0])

        if tx.body_blob is None or tx.data_response is not None:
            return tx_orig.delta(tx)

        rcpt = reduce(Exploder.reduce_data_response,
                      [ r for r in self.recipients
                        if r.tx.rcpt_response[0].ok()])
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
