from typing import Callable, List, Optional
import logging
from threading import Thread
import time
from functools import partial

from deadline import Deadline
from executor import Executor
from filter import (
    AsyncFilter,
    Mailbox,
    SyncFilter,
    TransactionMetadata )
from blob import Blob
from response import Response

# Fan-out multi-rcpt from smtp gw and fan responses back in. Convert
# certain upstream temp errors to success for store&forward.

# This provides opportunistic waiting for the upstream to return a
# fast result so you don't accept if it would permfail upstream
# immediately and have to send a bounce.

# This has different modes for MSA vs MX. For RCPT:
# - in msa mode, we wait for a short time (say 5s) for an upstream
#   permanent failure and otherwise return a 250 downstream. Temp
#   failures are "upgraded" to 250 since the client expects us to
#   retry.
# - in MX mode, we wait for a longer time (say 30s) and return temp
#   errors (4xx) directly since the client is another mta that should
#   be prepared to retry

# Since the PRDR extension for SMTP has yet to get traction, we have
# to fan in the DATA results. Again, 2 modes
# - in MX mode, if all the upstream data results were the same (for the
#   rcpts that previously succeeded), we return that directly
# - in MSA mode, we only do that if they were all success or permfail,
#   tempfail gets upgraded per above

# In either case, if there were mixed results, SMTP requires us to
# return a 250, durably retry any temp failures, and emit bounces for
# all the ones that don't eventually succeed.

# TODO options try to avoid accept&bounce smtp extensions i.e. if the
# envelope contains an extension and we time out upstream and would
# have to emit a 2xx response here for store&forward, fail if we
# don't otherwise know that the destination accepts it.

class Recipient:
    upstream : AsyncFilter
    tx :  Optional[TransactionMetadata] = None

    # upgraded/returned downstream
    mail_response : Optional[Response] = None
    rcpt_response : Optional[Response] = None

    # error (reported downstream) that caused this recipient to fail
    # or final data response
    status : Optional[Response] = None

    # tempfail upstream was upgraded to success downstream -> enable
    # retries/notifications
    store_and_forward : bool = False
    output_chain: str
    rcpt_timeout : Optional[float] = None
    data_timeout : Optional[float] = None
    msa : bool
    rcpt : Mailbox

    def __init__(self,
                 output_chain: str,
                 upstream : AsyncFilter,
                 msa : bool,
                 rcpt : Mailbox):
        self.output_chain = output_chain
        self.upstream = upstream
        self.msa = msa
        self.rcpt = rcpt

    # TODO move to common location
    def _update(self, tx : TransactionMetadata,
                tx_delta : TransactionMetadata,
                deadline : Deadline
                ) -> Optional[TransactionMetadata]:
        tx_orig = tx.copy()
        upstream_tx = tx.copy()
        upstream_delta = self.upstream.update(upstream_tx, tx_delta)
        while deadline.remaining() and upstream_tx.req_inflight():
            self.upstream.wait(deadline.deadline_left())
            upstream_tx = self.upstream.get()
        if tx_orig.body_blob:
            del tx_orig.body_blob
        upstream_delta = tx_orig.delta(upstream_tx)
        tx.replace_from(upstream_tx)
        return upstream_delta

    def _on_rcpt(self,
                 tx : TransactionMetadata,
                 delta : TransactionMetadata,
                 deadline : Deadline):
        logging.info('exploder.Recipient._on_rcpt %s', self.rcpt)

        # just send the envelope, we'll deal with any data below
        # disable retry/notification for this first attempt even if it's
        # requested from downstream since we're may report errors downstream
        # synchronously
        # TODO save any downstream notification/retry params
        # TODO copy_valid(REST_CREATE) ?
        self.tx = tx.copy()
        if self.tx.tx_db_id:
            del self.tx.tx_db_id
        self.tx.mail_response = None
        self.tx.host = self.output_chain
        self.tx.rcpt_to = [self.rcpt]
        self.tx.rcpt_response = []
        self.tx.notification = None
        if self.tx.body:
            del self.tx.body
        if self.tx.body_blob:
            del self.tx.body_blob

        logging.debug('exploder.Recipient._on_rcpt() downstream_tx %s', self.tx)
        upstream_delta = self._update(self.tx, self.tx.copy(), deadline)

        logging.debug('exploder.Recipient._on_rcpt() %s', upstream_delta)

        if upstream_delta is None:
            self.mail_response = Response(
                400, 'exploder.Recipient._on_rcpt internal error: '
                'bad upstream response')
            return

        # this only handles 1 recipient so it doesn't have to worry
        # about the delta
        if upstream_delta.mail_response is None:
            self.mail_response = Response(
                450, 'exploder upstream timeout MAIL')
            assert not(upstream_delta.rcpt_response)  # buggy upstream
        else:
            self.mail_response = upstream_delta.mail_response

        assert self.mail_response is not None

        # if mail failed upstream, return this in the rcpt response
        # Exploder often has to return a no-op 250 to mail and this is
        # the real problem. The rcpt response after mail failed will
        # probably be "failed precondition/bad sequence of commands"
        if self.mail_response.err():
            self.rcpt_response = self.mail_response
        elif (len(upstream_delta.rcpt_response) != 1
            or upstream_delta.rcpt_response[0] is None):
            self.rcpt_response = Response(450, 'exploder upstream timeout RCPT')
        else:
            self.rcpt_response = upstream_delta.rcpt_response[0]

        # we continue to store&forward after upstream temp errors on
        # mail/rcpt for for msa but not mx here
        if self.msa:
            if self.mail_response.temp():
                self.mail_response = Response(
                    250, 'MAIL ok (exploder store&forward MAIL)')
                self.rcpt_response = Response(
                    250, 'RCPT ok (exploder store&forward MAIL)')
                self.store_and_forward = True
            if (self.mail_response.ok() and
                self.rcpt_response.temp()):
                self.rcpt_response = Response(
                    250, 'RCPT ok (exploder store&forward RCPT)')
                self.store_and_forward = True

        if self.mail_response.err():
            self.status = self.mail_response
        elif self.rcpt_response.err():
            self.status = self.rcpt_response
        logging.debug('exploder.Recipient._on_rcpt() %s %s',
                      self.mail_response, self.rcpt_response)


    def _append_upstream(self, blob, deadline):
        last = blob.finalized()
        assert self.status is None or self.store_and_forward

        # don't wait for a status if it's already failed
        # TODO maybe storage writer filter, etc, should know that and
        # not wait?
        deadline = Deadline(0) if self.store_and_forward else deadline
        logging.debug('exploder Recipient._append_upstream %d/%s timeout=%s',
                      blob.len(), blob.content_length(),
                      deadline.deadline_left())
        body_delta = TransactionMetadata()
        body_delta.body_blob = blob
        self.tx.merge_from(body_delta)
        upstream_delta = self._update(self.tx, body_delta, deadline)
        if upstream_delta is None:
            data_resp = Response(
                450, 'exploder Recipient._append_upstream internal error: '
                'bad upstream response')
        elif last and upstream_delta.data_response is None:
            # data resp None on !last -> ok/continue
            data_resp = Response(450, 'exploder upstream timeout DATA')
        else:
            data_resp = upstream_delta.data_response

        if self.msa and data_resp is not None and data_resp.temp():
            self.store_and_forward = True
            data_resp = None

        if last and self.store_and_forward:
            data_resp = Response(
                250, 'accepted (exploder store&forward DATA)')

        if last or (data_resp is not None and data_resp.err()):
            assert self.status is None
            self.status = data_resp

        logging.info('exploder Recipient._append_upstream %s', data_resp)
        assert last or data_resp is None or not data_resp.ok()
        assert not (last and data_resp is None)

    def cancel(self):
        logging.info('exploder.Recipient.cancel')
        delta = TransactionMetadata(cancelled=True)
        assert self.tx.merge_from(delta) is not None
        upstream_delta = self.upstream.update(self.tx, delta)


class Exploder(SyncFilter):
    output_chain : str
    factory : Callable[[], AsyncFilter]
    rcpt_timeout : Optional[float] = None
    data_timeout : Optional[float] = None
    msa : Optional[bool] = None
    max_attempts : Optional[int] = None
    default_notification : Optional[dict] = None
    executor : Executor

    rcpt_ok = False
    mail_from : Mailbox = None

    recipients : List[Recipient]

    def __init__(self, output_chain : str,
                 factory : Callable[[], AsyncFilter],
                 executor : Executor,
                 rcpt_timeout : Optional[float] = None,
                 data_timeout : Optional[float] = None,
                 msa : Optional[bool] = None,
                 default_notification : Optional[dict] = None):
        self.output_chain = output_chain
        self.factory = factory
        self.rcpt_timeout = rcpt_timeout
        self.data_timeout = data_timeout
        self.msa = msa
        self.recipients = []
        self.default_notification = default_notification
        self.executor = executor

    def _on_mail(self,
                 delta : TransactionMetadata,
                 upstream_delta : TransactionMetadata):
        # because of the way SMTP works, you have to return a response
        # to MAIL FROM before you know who the RCPT is so if they
        # didn't pipeline those together, we just accept MAIL and hope
        # for the best
        assert self.mail_from is None
        self.mail_from = delta.mail_from
        if not delta.rcpt_to:
            # otherwise we'll take care of it in _on_rcpt()
            upstream_delta.mail_response = Response(
                250, 'MAIL ok; exploder noop')

    # run fns possibly in parallel
    # sets elements of fns that could not be completed within deadline to None
    def _run(self, fns : List[Optional[Callable]], deadline : Deadline) -> bool:
        assert not any([f for f in fns if f is None])
        if len(fns) == 1:
            fns[0]()
            return True

        futures = [None] * len(fns)
        logging.debug('Exploder._run start %d', len(fns))
        for i,fn in enumerate(fns):
            logging.debug('Exploder._run deadline_left %s',
                          deadline.deadline_left())
            fut = self.executor.submit(fn, deadline.deadline_left())
            if fut is None:
                fns[i] = None
                continue
            futures[i] = fut
        logging.debug('Exploder._run join %s', len(futures))
        for i,fut in enumerate(futures):
            if fut is None:
                continue
            logging.debug('Exploder._run join deadline_left %s',
                          deadline.deadline_left())
            # the next hop is something internal (i.e. storage writer
            # filter) that should respect the timeout so timing out
            # here should be rare
            try:
                fut.result(deadline.deadline_left())
            except TimeoutError:
                fns[i] = None
        return not any([f for f in futures if f is None])

    def _on_rcpts(self,
                  downstream_tx : TransactionMetadata,
                  downstream_delta : TransactionMetadata,
                  updated_tx  : TransactionMetadata):
        # NOTE smtplib/gateway don't currently don't support SMTP
        # PIPELINING so it will only send one at a time here
        rcpts = []
        fns = []
        rcpt_response = [None] * len(downstream_delta.rcpt_to)
        deadline = Deadline(self.rcpt_timeout)
        for i,rcpt in enumerate(downstream_delta.rcpt_to):
            recipient = Recipient(self.output_chain,
                                  self.factory(),
                                  self.msa,
                                  rcpt)
            self.recipients.append(recipient)
            rcpts.append(recipient)
            fns.append(
                partial(lambda r: r._on_rcpt(downstream_tx, downstream_delta,
                                             deadline),
                        recipient))

        self._run(fns, deadline)

        for i, recipient in enumerate(rcpts):
            if fns[i] is None:
                recipient.mail_response = recipient.rcpt_response = Response(
                    450, 'server busy '
                    '(Exploder failed to schedule rcpt upstream)')

            mail_resp = recipient.mail_response
            rcpt_resp = recipient.rcpt_response

            logging.debug('Exploder()._on_rcpts() %d %s %s',
                          i, mail_resp, rcpt_resp)

            # TODO the following is moot until we get pipelining from the
            # smtp gw but for the record:
            # rejecting MAIL is probably relatively uncommon
            # multi-rcpt is relatively uncommon
            # mixed upstream responses to MAIL is probably very uncommon
            # BUT
            # if we get mail and the first rcpt in one call and the upstream
            # 5xx mail, we will return that downstream and fail the whole tx
            # so maybe just always return 250 for mail unless we get MAIL...DATA
            # in one shot with pipelining?

            # Return the best MAIL response we've seen so far
            # upstream.  Again, without pipelining in the current
            # smtplib gateway implementation, we will always return a
            # placeholder 250 in _on_mail() so this is mostly moot.
            if downstream_delta.mail_from is not None:
                logging.debug('Exploder._on_rcpts() %d mail_resp %s',
                              i, mail_resp)
                if (updated_tx.mail_response is None or
                    (updated_tx.mail_response.perm() and mail_resp.temp()) or
                    (updated_tx.mail_response.temp() and mail_resp.ok())):
                    updated_tx.mail_response = mail_resp

            # vs mail err (above)
            if rcpt_response[i] is None:
                rcpt_response[i] = rcpt_resp

        updated_tx.rcpt_response.extend(rcpt_response)
        assert len(updated_tx.rcpt_response) == len(updated_tx.rcpt_to)

    def on_update(self,
                  tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:
        logging.info('Exploder.on_update %s', tx_delta)

        if tx_delta.cancelled:
            logging.info('Exploder.on_update: cancel')
            self._run([r.cancel for r in self.recipients],
                      Deadline(self.rcpt_timeout))
            return

        updated_tx = tx.copy()
        if tx_delta.mail_from is not None:
            self._on_mail(tx_delta, updated_tx)

        self._on_rcpts(tx, tx_delta, updated_tx)

        if tx_delta.body_blob:
            updated_tx.data_response = self._append_data(tx_delta.body_blob)

        upstream_delta = tx.delta(updated_tx)
        assert tx.merge_from(upstream_delta) is not None
        return upstream_delta


    def _cutthrough_data(self) -> Optional[Response]:
        # for all the recipients that didn't return a downstream error
        # for mail/rcpt
        # if all the data responses are the same major code (2xx/4xx/5xx)
        # and none require store&forward (after some temp upstream
        # errors) return that response directly

        if any([r.store_and_forward for r in self.recipients]):
            return None

        s0 = self.recipients[0].status
        for i,ri in enumerate(self.recipients[1:]):
            if not ri.mail_response.ok() or not ri.rcpt_response.ok():
                continue
            si = ri.status
            if (si is None) != (s0 is None):
                return None
            if (s0 is not None) and (s0.major_code() != si.major_code()):
                return None
        else:
            if s0 is not None:
                s0.message = 'exploder same status: ' + s0.message
            logging.debug(
                'Exploder._cutthrough_data same status data_resp %s', s0)
            return s0

        return None

    def _append_data(self, blob : Blob) -> Optional[Response]:
        logging.info('Exploder._append_data %d %s',
                     blob.len(), blob.content_length())

        deadline = Deadline(self.data_timeout)
        fns = []
        for recipient in self.recipients:
            if recipient.status is not None and not recipient.status.ok():
                continue
            fns.append(
                partial(lambda r: r._append_upstream(blob, deadline),
                        recipient))

        if not self._run(fns, deadline):
            # hopefully this is rare
            # we may need to bring back separate
            # queues/priorities/limits in the Executor to ensure that
            return Response(450, 'server busy '
                            '(Exploder failed to schedule data upstream)')

        if (data_resp := self._cutthrough_data()) is not None:
            return data_resp

        if not blob.finalized():
            return None

        for i,recipient in enumerate(self.recipients):
            logging.debug('Exploder._append_data enable '
                          'notifications/retries %d', i)

            # TODO restore any saved downstream notification request
            if not recipient.rcpt_response.ok():
                continue
            if recipient.status.ok() and not recipient.store_and_forward:
                continue
            retry_delta = TransactionMetadata(
                retry = {},
                # XXX this will blackhole if unset!
                notification=self.default_notification)

            recipient.tx.merge_from(retry_delta)
            recipient.upstream.update(
                recipient.tx, retry_delta)

        return Response(250, 'accepted (exploder store&forward DATA)')

