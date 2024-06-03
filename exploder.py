from typing import Callable, List, Optional
import logging
from threading import Thread
import time

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
    # error or final data response
    status : Optional[Response] = None
    upstream : Optional[AsyncFilter] = None
    mail_response : Optional[Response] = None
    rcpt_response : Optional[Response] = None
    thread : Optional[Thread] = None
    tx :  Optional[TransactionMetadata] = None

    def __init__(self):
        pass

class Exploder(SyncFilter):
    output_chain : str
    factory : Callable[[], AsyncFilter]
    rcpt_timeout : Optional[float] = None
    data_timeout : Optional[float] = None
    msa : Optional[bool] = None
    max_attempts : Optional[int] = None
    default_notification : Optional[dict] = None

    rcpt_ok = False
    mail_from : Mailbox = None

    recipients : List[Recipient]

    def __init__(self, output_chain : str,
                 factory : Callable[[], AsyncFilter],
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

    def _on_mail(self,
                 delta : TransactionMetadata,
                 upstream_delta : TransactionMetadata):
        # because of the way SMTP works, you have to return a response
        # to MAIL FROM before you know who the RCPT is so if they
        # didn't pipeline those together, we just accept MAIL and hope
        # for the best
        # TODO possibly move this to a separate filter
        assert self.mail_from is None
        self.mail_from = delta.mail_from
        if not delta.rcpt_to:
            # otherwise we'll take care of it in _on_rcpt()
            upstream_delta.mail_response = Response(
                250, 'MAIL ok; exploder noop')

    def _on_rcpt(self,
                 tx : TransactionMetadata,
                 delta : TransactionMetadata,
                 rcpt : Mailbox, recipient : Recipient):
        logging.info('Exploder._on_rcpt %s', rcpt)

        # just send the envelope, we'll deal with any data below
        # TODO copy.copy() here?

        # disable notification for this first attempt even if it's
        # requested from downstream since we're going to report it downstream
        # synchronously
        # TODO save any downstream notification/retry params
        recipient.tx = tx.copy()
        recipient.tx.mail_response = None
        recipient.tx.host = self.output_chain
        recipient.tx.rcpt_to = [rcpt]
        recipient.tx.rcpt_response = []
        recipient.tx.notification = None

        recipient.upstream = self.factory()
        logging.debug('Exploder._on_rcpt() downstream_tx %s', recipient.tx)
        upstream_delta = recipient.upstream.update(
            recipient.tx, recipient.tx, self.rcpt_timeout)

        logging.debug('Exploder._on_rcpt() %s', upstream_delta)

        if upstream_delta is None:
            recipient.mail_response = Response(
                400, 'Exploder._on_rcpt internal error: upstream_delta is None')
            return

        # this only handles 1 recipient so it doesn't have to worry
        # about the delta
        if upstream_delta.mail_response is None:
            upstream_delta.mail_response = Response(
                400, 'exploder upstream timeout')
            assert not(upstream_delta.rcpt_response)  # buggy upstream
            upstream_delta.rcpt_response = [upstream_delta.mail_response]

        assert len(upstream_delta.rcpt_response) <= 1

        if (len(upstream_delta.rcpt_response) != 1
            or upstream_delta.rcpt_response[0] is None):
            upstream_delta.rcpt_response = [upstream_delta.mail_response]

        # we continue to store&forward after upstream temp errors on
        # mail/rcpt for for msa but not mx here
        assert recipient.mail_response is None
        recipient.mail_response = upstream_delta.mail_response
        rcpt_resp = upstream_delta.rcpt_response
        assert len(rcpt_resp) == 1 and rcpt_resp[0] is not None
        recipient.rcpt_response = rcpt_resp[0]
        logging.debug('_on_rcpt %s %s',
                      recipient.mail_response, rcpt_resp[0])

    def _on_rcpts(self,
                  downstream_tx : TransactionMetadata,
                  downstream_delta : TransactionMetadata,
                  upstream_delta  : TransactionMetadata):
        rcpts = []
        for i,rcpt in enumerate(downstream_delta.rcpt_to):
            recipient = Recipient()
            self.recipients.append(recipient)
            rcpts.append(recipient)
        upstream_delta.rcpt_response = [None] * len(downstream_delta.rcpt_to)

        # NOTE smtplib/gateway don't currently don't support SMTP
        # PIPELINING so it will only send one at a time here
        if len(downstream_delta.rcpt_to) == 1:
            self._on_rcpt(downstream_tx, downstream_delta,
                          downstream_delta.rcpt_to[0], rcpts[0])
        else:
            for i,rcpt in enumerate(downstream_delta.rcpt_to):
                recipient = rcpts[i]
                # TODO executor
                recipient.thread = Thread(
                    target=lambda: self._on_rcpt(
                        downstream_tx, downstream_delta,
                        rcpt, self.recipients[i]))
                recipient.thread.start()
                for r in self.recipients:
                    logging.debug('_on_rcpts_parallel join')
                    if not r.thread:
                        continue
                    # NOTE: we don't set a timeout on this join()
                    # because we expect the next hop is something
                    # internal (i.e. storage writer filter) that
                    # respects the timeout
                    r.thread.join()
                    r.thread = None

        for i, recipient in enumerate(rcpts):
            mail_resp = recipient.mail_response
            rcpt_resp = recipient.rcpt_response

            # In the absence of pipelining, we returned a noop 2xx to
            # MAIL downstream in _on_mail(). If mail failed upstream
            # for a rcpt, if rcpt had a response at all, it's
            # something like failed precondition/invalid sequence of
            # commands. In this case, return the upstream mail err as
            # the rcpt response here which is the real error and
            # ignore the rcpt response which is moot.

            logging.debug('Exploder()._on_rcpts() %d %s %s',
                          i, mail_resp, rcpt_resp)

            if mail_resp.err():
                assert rcpt_resp is None or rcpt_resp.err()
                recipient.status = mail_resp
                if self.msa and mail_resp.temp():
                    mail_resp = (
                        Response(250, 'MAIL ok (exploder async upstream)'))
                    rcpt_resp = (
                        Response(250, 'RCPT ok '
                                 '(exploder async upstream after MAIL temp)'))
                else:
                    upstream_delta.rcpt_response[i] = mail_resp

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
                if (upstream_delta.mail_response is None or
                    (upstream_delta.mail_response.perm() and mail_resp.temp()) or
                    (upstream_delta.mail_response.temp() and mail_resp.ok())):
                    upstream_delta.mail_response = mail_resp

            # rcpt err
            if recipient.status is None and rcpt_resp.err():
                recipient.status = rcpt_resp
                if self.msa and rcpt_resp.temp():
                    upstream_delta.rcpt_response[i] = (
                        Response(250, 'RCPT ok (exploder async upstream)'))
                    continue
            if upstream_delta.rcpt_response[i] is None:
                upstream_delta.rcpt_response[i] = rcpt_resp



    def on_update(self,
                  tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:
        logging.info('Exploder.on_update %s', tx_delta)

        upstream_delta = TransactionMetadata()
        if tx_delta.mail_from is not None:
            self._on_mail(tx_delta, upstream_delta)

        self._on_rcpts(tx, tx_delta, upstream_delta)

        if tx_delta.body_blob:
            upstream_delta.data_response = self._append_data(tx_delta.body_blob)

        assert tx.merge_from(upstream_delta) is not None
        return upstream_delta

    def _append_upstream(self, recipient : Recipient, blob, timeout):
        prev_resp = recipient.status
        logging.debug('Exploder._append_upstream prev_resp %s, timeout=%s',
                      prev_resp, timeout)
        assert prev_resp is None or not prev_resp.perm()
        body_delta = TransactionMetadata()
        body_delta.body_blob = blob
        last = blob.len() == blob.content_length()
        recipient.tx.merge_from(body_delta)
        upstream_delta = recipient.upstream.update(
            recipient.tx, body_delta, timeout)
        if upstream_delta is None:
            data_resp = Response(
                400, 'Exploder._append_upstream internal error: '
                'upstream_delta is None')
        else:
            data_resp = upstream_delta.data_response

        if last and data_resp is None:
            data_resp = Response(
                400, 'exploder upstream timeout DATA')
        logging.info('Exploder.append_data %s', data_resp)
        assert last or data_resp is None or not data_resp.ok()
        assert not (last and data_resp is None)
        if data_resp is not None and prev_resp is None:
            recipient.status = data_resp

    def _append_data(self, blob : Blob) -> Optional[Response]:
        logging.info('Exploder._append_data %d %s',
                     blob.len(), blob.content_length())

        if self.msa:
            # at least one recipient hasn't permfailed
            assert any([r.status is None or not r.status.perm()
                        for r in self.recipients])
        else:
            # at least one recipient hasn't failed
            assert any([r.status is None for r in self.recipients])

        for recipient in self.recipients:
            rcpt_status = recipient.status
            if rcpt_status and rcpt_status.perm():
                continue
            timeout = self.data_timeout
            if (rcpt_status is not None) and rcpt_status.temp():
                # TODO don't wait for a status since it's already
                # failed, but maybe storage writer filter, etc, should
                # know that and not wait?
                timeout = 0
            recipient.thread = Thread(
                target = lambda: self._append_upstream(
                    recipient, blob, timeout),
                daemon = True)
            recipient.thread.start()

        last = blob.len() == blob.content_length()

        for recipient in self.recipients:
            t = recipient.thread
            if t is None:
                continue
            # NOTE: we don't set a timeout on this join()
            # because we expect the next hop is something
            # internal (i.e. storage writer filter) that
            # respects the timeout
            t.join()
            assert not (last and recipient.status is None)

        r0 = self.recipients[0].status
        codes = [ int(r.status.code/100) if r.status is not None else None
                  for r in self.recipients]
        msa_async_temp = False
        if len([c for c in codes if c == codes[0]]) == len(codes):
            # same status for all recipients
            # but msa store-and-forward temp since the client expects that
            if self.msa:
                if r0 is None or r0.ok() or r0.perm():
                    logging.debug('Exploder msa same resp')
                    return r0
                elif r0.temp():
                    msa_async_temp = True
            else:
                logging.debug('Exploder mx same resp')
                return r0

        if not last:
            return None

        # temp or needs notification
        for recipient in [r for r in self.recipients if r.status.temp()]:
            if self.default_notification is not None:
                # The upstream tx could have permfailed right after we
                # timed out the first/sync attempt w/notification
                # disabled. The final upstream response will set
                # final_attempt_reason in the tx so it isn't recoverable.  But
                # we still need to send a notification.

                # For the time being, Storage._write() clears
                # final_attempt_reason if setting notification. This opens a
                # small window within which we will dupe/resend to the
                # upstream after it previously permfailed.

                # TODO It may actually not be that hard for
                # OutputHandler to skip directly to the notification
                # logic if it has a final upstream response in the
                # previous attempt? Or add another notification_done bool
                # on the db tx that this sets to false that load_one can
                # OR into the query?

                # TODO restore any saved downstream notification request
                retry_delta = TransactionMetadata(
                    notification=self.default_notification,
                    retry={})
                recipient.tx.merge_from(retry_delta)
                recipient.upstream.update(
                    recipient.tx, retry_delta,
                    timeout=0)  # i.e. don't wait on inflight

        message = None
        if msa_async_temp:
            message = 'exploder async msa upstream temp'
        else:
            message = 'exploder async mixed upstream responses'
        return Response(
            250, 'accepted (' + message + ')')


    def abort(self):
        pass
