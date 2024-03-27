from typing import Callable, List, Optional
import logging
from threading import Thread
import time

from filter import Filter, Mailbox, TransactionMetadata
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

class Recipient:
    # error or final data response
    status : Optional[Response] = None
    upstream : Optional[Filter] = None
    mail_response : Optional[Response] = None
    rcpt_response : Optional[Response] = None
    thread : Optional[Thread] = None

    def __init__(self):
        pass

class Exploder(Filter):
    output_chain : str
    factory : Callable[[], Filter]

    rcpt_ok = False
    mail_from : Mailbox = None

    recipients : List[Recipient]

    def __init__(self, output_chain : str,
                 factory : Callable[[], Filter],
                 rcpt_timeout : Optional[float] = None,
                 data_timeout : Optional[float] = None,
                 msa : Optional[bool] = None,
                 max_attempts : Optional[int] = None):
        self.output_chain = output_chain
        self.factory = factory
        self.rcpt_timeout = rcpt_timeout
        self.data_timeout = data_timeout
        self.msa = msa
        self.max_attempts = max_attempts
        self.recipients = []

    def _on_mail(self, delta):
        # because of the way SMTP works, you have to return a response
        # to MAIL FROM before you know who the RCPT is so if they
        # didn't pipeline those together, we just accept MAIL and hope
        # for the best
        # TODO possibly move this to a separate filter
        assert self.mail_from is None
        self.mail_from = delta.mail_from
        if not delta.rcpt_to:
            # otherwise we'll take care of it in _on_rcpt()
            delta.mail_response = Response(250)

    def _on_rcpt(self, delta, rcpt : Mailbox, recipient : Recipient):
        logging.info('Exploder._on_rcpt %s', rcpt)

        # just send the envelope, we'll deal with any data below
        # TODO copy.copy() here?
        upstream_tx = TransactionMetadata(host = self.output_chain,
                                   mail_from = self.mail_from,
                                   rcpt_to = [rcpt],
                                   remote_host = delta.remote_host)
        recipient.upstream = self.factory()
        recipient.upstream.on_update(upstream_tx, self.rcpt_timeout)

        # we accept upstream temp errors on mail/rcpt for
        # store&forward for msa but not mx here
        assert recipient.mail_response is None
        recipient.mail_response = upstream_tx.mail_response
        rcpt_resp = upstream_tx.rcpt_response
        assert len(rcpt_resp) == 1 and rcpt_resp[0] is not None
        recipient.rcpt_response = rcpt_resp[0]
        logging.debug('_on_rcpt %s %s', upstream_tx.mail_response, rcpt_resp[0])

    def _on_rcpts(self, delta):
        rcpts = []
        for i,rcpt in enumerate(delta.rcpt_to):
            recipient = Recipient()
            self.recipients.append(recipient)
            rcpts.append(recipient)
        delta.rcpt_response = [None] * len(delta.rcpt_to)

        # NOTE smtplib/gateway don't currently don't support SMTP
        # PIPELINING so it will only send one at a time here
        if len(delta.rcpt_to) == 1:
            self._on_rcpt(delta, delta.rcpt_to[0], rcpts[0])
        else:
            for i,rcpt in enumerate(delta.rcpt_to):
                recipient = rcpts[i]
                recipient.thread = Thread(
                    target=lambda: self._on_rcpt(
                        delta, rcpt, self.recipients[i]))
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

            logging.debug('_on_rcpt %d %s %s', i, mail_resp, rcpt_resp)

            # Probably the most likely reason for MAIL to fail in 2024 is SPF?
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
                    delta.rcpt_response[i] = mail_resp

            # Return the best MAIL response we've seen so far
            # upstream.  Again, without pipelining in the current
            # smtplib gateway implementation, we will always return a
            # placeholder 250 in _on_mail() so this is mostly moot.

            # It seems possible to get mixed upstream MAIL responses
            # due to differing levels of SPF enforcement for say a
            # multi-rcpt msa transaction to different destination
            # domains. If the client pipelined multiple recipients
            # (which again, the gateway doesn't currently support),
            # you could see nondeterminstic behavior here depending on
            # the ordering.

            # In that case the real problem is that the client is
            # sending mail that fails SPF and the MSA is accepting
            # it.
            if delta.mail_from is not None:
                if (delta.mail_response is None or
                    (delta.mail_response.perm() and mail_resp.temp()) or
                    (delta.mail_response.temp() and mail_resp.ok())):
                    delta.mail_response = mail_resp

            # rcpt err
            if recipient.status is None and rcpt_resp.err():
                recipient.status = rcpt_resp
                if self.msa and rcpt_resp.temp():
                    delta.rcpt_response[i] = (
                        Response(250, 'RCPT ok (exploder async upstream)'))
                    continue
            if delta.rcpt_response[i] is None:
                delta.rcpt_response[i] = rcpt_resp



    def on_update(self, delta):
        logging.info('Exploder.on_update %s', delta.to_json())

        if delta.mail_from is not None:
            self._on_mail(delta)

        self._on_rcpts(delta)

        if delta.body_blob:
            delta.data_response = self._append_data(delta.body_blob)

    def _append_upstream(self, recipient : Recipient, blob, timeout):
        prev_resp = recipient.status
        logging.debug('Exploder._append_upstream %s timeout=%s',
                      prev_resp, timeout)
        assert prev_resp is None or not prev_resp.perm()
        body_tx = TransactionMetadata()
        body_tx.body_blob = blob
        recipient.upstream.on_update(body_tx, timeout)
        data_resp = body_tx.data_response
        logging.info('Exploder.append_data %s', data_resp)
        last = blob.len() == blob.content_length()
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
            if rcpt_status and rcpt_status.temp():
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

        # TODO when we add bounce emission, we need to enable bounces
        # for mx if we didn't return a sync error above.

        for recipient in [r for r in self.recipients if r.status.temp()]:
            if self.max_attempts:
                recipient.upstream.on_update(
                    TransactionMetadata(max_attempts=self.max_attempts))

        message = None
        if msa_async_temp:
            message = 'exploder async msa upstream temp'
        else:
            message = 'exploder async mixed upstream responses'
        return Response(
            250, 'accepted (' + message + ')')


    def abort(self):
        pass
