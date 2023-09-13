from aiosmtpd.smtp import SMTP
from aiosmtpd.controller import Controller
import ssl

import asyncio
import time
import logging

from functools import partial

from blob import InlineBlob

from typing import Optional
from typing import Tuple

from response import ok_resp, to_smtp_resp

from smtp_auth import Authenticator

from response import Response

MSA_RCPT_WAIT=5
MX_RCPT_WAIT=30
MSA_DATA_WAIT=5
MX_DATA_WAIT=300

class SmtpHandler:
    def __init__(self, endpoint_factory, msa, max_rcpt=None):
        self.endpoint_factory = endpoint_factory
        self.msa = msa
        self.max_rcpt = max_rcpt

    # XXX the Endpoint stack is sync, this should spawn threads?

    async def handle_RSET(self, server, session, envelope):
        envelope.transactions = []
        return b'250 ok'

    async def handle_MAIL(
            self, server, session, envelope, address, options):
        envelope.transactions = []
        envelope.mail_from = address
        envelope.mail_options.extend(options)
        envelope.msa_async = False
        session.total_rcpt_wait = 0
        return b'250 MAIL ok'


    def start(rresp, trans, local, remote,
              mail_from, transaction_esmtp,
              rcpt, rcpt_esmtp):
        logging.info('SmtpHandler.start')
        rresp[0] = trans.start(local, remote, mail_from, transaction_esmtp,
                               rcpt, rcpt_esmtp)
        logging.info('SmtpHandler.start done %s', rresp[0])

    async def handle_RCPT(
            self, server, session, envelope, address, rcpt_options):
        # TODO multi-rcpt doesn't completely work until durable retry
        # is implemented in the router
        if self.max_rcpt and (len(envelope.rcpt_tos) > self.max_rcpt):
            return b'452-4.5.3 too many recipients'

        transaction = self.endpoint_factory()
        start_time = time.monotonic()

        rresp = [None]
        fut = self.loop.run_in_executor(
            None, lambda: SmtpHandler.start(rresp, transaction,
                None, None,
                envelope.mail_from, envelope.mail_options,
                address, rcpt_options))
        timeout = None
        if self.msa:
            # msa rcpt timeout is cumulative
            timeout = max(MSA_RCPT_WAIT - session.total_rcpt_wait, 0)
        else:  # mx
            timeout = MX_RCPT_WAIT
        # XXX pass this timeout to RestEndpoint
        if timeout is not None:
            #done, pending =
            await asyncio.wait([fut], timeout=timeout)
        resp = rresp[0]
        if resp is None:
            resp = Response(400, 'upstream rcpt timeout')
        logging.info('rcpt start_response done %s', resp)
        session.total_rcpt_wait += max(time.monotonic() - start_time, 0)

        if self.msa:
            if resp.perm():
                return resp.to_smtp_resp()
            elif resp.temp():
                envelope.msa_async = True
        else:  # mx
            if resp.err():
                return resp.to_smtp_resp()

        envelope.transactions.append(transaction)
        envelope.rcpt_tos.append(address)

        return '250 RCPT ok'

    def append_data(resp, i, trans, last, blob):
        logging.info('SmtpHandler.append_data %d last=%s len=%d',
                     i, last, blob.len())
        resp[i] = trans.append_data(last, blob)
        logging.info('SmtpHandler.append_data %d %s', i, resp[i])

    def set_durable(rresp, i, trans):
        rresp[i] = trans.set_durable()

    async def handle_DATA(self, server, session, envelope):
        # framework enforces this
        assert(envelope.rcpt_tos)

        sstatus = [ None ] * len(envelope.transactions)

        blob = InlineBlob(envelope.content)

        # TODO cf inflight waiting in rest endpoint code
        futures = []
        for i,t in enumerate(envelope.transactions):
            logging.info('SmtpHandler.handle_DATA dispatch append %d', i)
            futures.append(self.loop.run_in_executor(
                None, partial(lambda i, t: SmtpHandler.append_data(
                    sstatus, i, t, last=True, blob=blob),
                              i, t)))

        timeout = None
        if self.msa:
            if not envelope.msa_async:
                timeout = MSA_DATA_WAIT
        else:  # mx
            timeout = MX_DATA_WAIT
        # xxx pass this timeout to RestEndpoint.append_data
        s0 = status = None
        same_major = None
        if timeout is not None:
            #done, pending =
            await asyncio.wait(
                futures, timeout=timeout, return_when=asyncio.ALL_COMPLETED)
            status = []
            for i,s in enumerate(sstatus):
                if s is None:
                    s = Response(400, 'upstream data timeout')
                major = s.code/100
                if i == 0:
                    s0 = s
                    same_major = major
                else:
                    if major != same_major:
                        same_major = None
                status.append(s)
            s0 = status[0]

        if self.msa:
            if not envelope.msa_async:
                if same_major == 2 or same_major == 5:
                    return s0.to_smtp_resp()
        else:  # mx
            if same_major is not None:
                return s0.to_smtp_resp()

        rresp = [None] * len(envelope.transactions)
        futures = []
        for i,t in enumerate(envelope.transactions):
            futures.append(self.loop.run_in_executor(
                None, partial(lambda i, t:
                              SmtpHandler.set_durable(rresp, i, t), i, t)))
        done, pending = await asyncio.wait(
            futures, timeout=5, return_when=asyncio.ALL_COMPLETED)
        if any(map(lambda r: r is None or r.err(), rresp)):
            return b'400 set_durable timeout'
        return b'250 smtp gw accepted async'


class ControllerTls(Controller):
    def __init__(self, handler, host, port, ssl_context, auth):
        self.tls_controller_context = ssl_context
        self.auth = auth
        super(Controller, self).__init__(
            handler, hostname=host, port=port)

    def factory(self):
        return SMTP(self.handler, #require_starttls=True,
                    tls_context=self.tls_controller_context,
                    authenticator=self.auth)


def service(endpoint, msa,
            hostname="localhost", port=9025, cert=None, key=None,
            auth_secrets_path=None, max_rcpt=None):
    # DEBUG logs message contents!
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')

    if cert and key:
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_context.load_cert_chain(cert, key)
    else:
        ssl_context = None
    auth = Authenticator(auth_secrets_path) if auth_secrets_path else None
    handler = SmtpHandler(endpoint, msa, max_rcpt)
    controller = ControllerTls(handler,
                               hostname, port, ssl_context,
                               auth)
    handler.loop = controller.loop
    controller.start()
