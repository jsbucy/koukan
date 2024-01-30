from typing import Callable, List, Optional
import logging
from threading import Thread
import time

from filter import Filter, Mailbox, TransactionMetadata
from blob import Blob
from response import Response

# Fan-out multi-rcpt from smtp gw and fan responses back in

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

class Exploder(Filter):
    output_chain : str
    factory : Callable[[str], Filter]

    upstream_chain : List[Filter]
    rcpt_ok = False
    mail_from : Mailbox = None
    ok_rcpt = False
    ok_rcpts : List[bool]
    # rcpts that tempfailed/timed out that we're accepting async for msa
    async_rcpts : List[bool]
    upstream_data_resp = List[Response]

    def __init__(self, output_chain : str,
                 factory : Callable[[], Filter],
                 rcpt_timeout : Optional[float] = None,
                 data_timeout : Optional[float] = None,
                 msa : Optional[bool] = None):
        self.output_chain = output_chain
        self.factory = factory
        self.upstream_chain = []
        self.ok_rcpts = []
        self.async_rcpts = []
        self.upstream_data_resp = []
        self.rcpt_timeout = rcpt_timeout
        self.data_timeout = data_timeout
        self.msa = msa

    def _on_mail(self, delta):
        # because of the way SMTP works, you have to return a response
        # to MAIL FROM before you know who the RCPT is so if they
        # didn't pipeline those together, we just accept MAIL and hope
        # for the best
        # TODO possibly move this to a separate filter
        assert self.mail_from is None
        self.mail_from = delta.mail_from
        if not delta.rcpt_to:
            delta.mail_response = Response(250)

    def _on_rcpt(self, delta, i : int, rcpt : Mailbox):
        logging.info('Exploder._on_rcpt %s', rcpt)

        # just send the envelope, we'll deal with any data below
        tx_i = TransactionMetadata(host = self.output_chain,
                                   mail_from = self.mail_from,
                                   rcpt_to = [rcpt])
        endpoint_i = self.factory()
        # fan this out if there were multiple though gw/smtplib
        # doesn't do pipelining so it will only send us one at
        # a time

        # this is where we do the "accept" part of "accept&bounce"
        # for msa (or multi-rcpt mx) i.e. convert a 4xx or timeout
        # to 250 in the hope that it will succeed later
        endpoint_i.on_update(tx_i, self.rcpt_timeout)
        mail_resp = tx_i.mail_response
        rcpt_resp = tx_i.rcpt_response[0] if tx_i.rcpt_response else None
        async_rcpt = False
        if self.msa and (rcpt_resp is None or rcpt_resp.temp()):
            mail_resp = Response(250, 'MAIL ok (exploder async upstream)')
            rcpt_resp = Response(250, 'RCPT ok (exploder async upstream)')
            async_rcpt = True
        self.async_rcpts.append(async_rcpt)
        logging.info('Exploder._on_rcpt upstream %s %s',
                     mail_resp, rcpt_resp)

        self.upstream_chain.append(endpoint_i)

        if delta.mail_from and not delta.mail_response:
            delta.mail_response = mail_resp

        # hopefully rare
        if mail_resp.err():
            delta.rcpt_response.append(tx_i.mail_response)
            self.ok_rcpts.append(False)
            return

        delta.rcpt_response.append(rcpt_resp)
        self.ok_rcpts.append(rcpt_resp.ok())
        if rcpt_resp.ok():
            self.ok_rcpt = True


    def on_update(self, delta):
        logging.info('Exploder.on_update %s', delta.to_json())

        if delta.mail_from is not None:
            self._on_mail(delta)

        for i,rcpt in enumerate(delta.rcpt_to):
            self._on_rcpt(delta, i, rcpt)

    def _append_upstream(self, i, endpoint_i, last, blob, timeout):
        logging.debug('Exploder.append_data %d %s %s timeout=%s', i,
                      self.ok_rcpts[i], self.upstream_data_resp[i], timeout)
        prev_resp = self.upstream_data_resp[i]
        assert prev_resp is None or not prev_resp.perm()
        resp = endpoint_i.append_data(last, blob, timeout)
        logging.info('Exploder.append_data %d %s', i, resp)
        if resp is not None:
            self.upstream_data_resp[i] = resp


    def append_data(self, last : bool, blob : Blob) -> Response:
        logging.info('Exploder.append_data %s %s', last, self.async_rcpts)
        assert self.ok_rcpt
        if not self.upstream_data_resp:
            self.upstream_data_resp = len(self.ok_rcpts) * [None]

        threads = []
        for i,endpoint_i in enumerate(self.upstream_chain):
            if not self.ok_rcpts[i]:
                continue
            timeout = self.data_timeout
            if self.async_rcpts[i]:
                timeout = 0
            t = Thread(target = lambda: self._append_upstream(
                i, endpoint_i, last, blob, timeout),
                       daemon = True)
            t.start()
            threads.append(t)

        start = time.monotonic()
        for t in threads:
            deadline_left = None
            if self.data_timeout is not None:
                deadline_left = self.data_timeout - (time.monotonic() - start)
            # TODO we expect the upstream StorageWriterFilter to
            # respect the timeouts so it is unexpected for this to
            # time out
            t.join(timeout=deadline_left)
        logging.debug('upstream data resp %s', self.upstream_data_resp)

        if not last:
            for i,endpoint_i in enumerate(self.upstream_chain):
                resp = self.upstream_data_resp[i]
                if resp is not None:
                    if resp.perm():
                        self.ok_rcpts[i] = False
                    elif resp.temp():
                        self.async_rcpts[i] = True
                    else:
                        # TODO not sure if this is ever expected
                        logging.warn(
                            'Exploder.append_data unexpected early response')

            if any(self.ok_rcpts):
                return None

        r0 = None
        for i,ok_rcpt in enumerate(self.ok_rcpts):
            if not ok_rcpt:
                continue
            data_resp = self.upstream_data_resp[i]
            if data_resp is None:
                if self.async_rcpts[i]:
                    data_resp = Response(250, 'exploder async resp')
                else:
                    data_resp = Response(450, 'exploder upstream timeout data')

            if data_resp.temp():
                self.async_rcpts[i] = True

            if r0 is None:
                r0 = data_resp
                logging.debug('Exploder i=%d r0 %s', i, r0)
                continue
            if data_resp.code/100 != r0.code/100:
                logging.debug('Exploder i=%d resp %s', i, data_resp)
                break
        else:
            if not any(self.async_rcpts):
                if self.msa:
                    if r0.ok() or r0.perm():
                        logging.debug('Exploder msa same resp')
                        return r0
                else:
                    logging.debug('Exploder mx same resp')
                    return r0

        for i,async_rcpt in enumerate(self.async_rcpts):
            if not async_rcpt:
                continue
            # xxx retry params from yaml
            self.upstream_chain[i].on_update(
                TransactionMetadata(max_attempts=100))

        return Response(
            250, 'accepted (exploder async mixed upstream responses)')


    def abort(self):
        pass
