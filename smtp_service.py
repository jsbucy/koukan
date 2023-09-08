from aiosmtpd.smtp import SMTP
from aiosmtpd.controller import Controller
import ssl

import asyncio
import time
import logging

from blob import InlineBlob

from typing import Optional
from typing import Tuple

from response import ok_resp, to_smtp_resp

from smtp_auth import Authenticator


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
        start_time = time.time()

        rresp = [None]
        fut = self.loop.run_in_executor(
            None, lambda: SmtpHandler.start(rresp, transaction,
                None, None,
                envelope.mail_from, envelope.mail_options,
                address, rcpt_options))

        # msa wait for a total of 30s across all rcpts
        #   maybe msa shouldn't wait at all, if you want first-class outbox,
        #   use the first-class api, msa could be coming from device
        #   on intermittent mobile connection
        # mx wait for 30s for each rcpt
        if self.msa:
          if session.total_rcpt_wait < 30:
            timeout = 1
          else:
            timeout = 0
        else:  # mx
            timeout = 30
        logging.info('rcpt start_response')
        done, pending = await asyncio.wait([fut], timeout=timeout)
        if pending and not self.msa:
            return b'400 upstream rcpt timeout'
        resp = rresp[0]
        logging.info('rcpt start_response done %s', resp)
        session.total_rcpt_wait += max(time.time() - start_time, 0)

        if resp and ((not self.msa and resp.err()) or
            (self.msa and resp.perm())):
            return resp.to_smtp_resp()

        envelope.transactions.append(transaction)
        envelope.rcpt_tos.append(address)

        return '250 RCPT ok'

    def append_data(resp, i, trans, last, blob):
        resp[i] = trans.append_data(last, blob)
        logging.info('append_data %d %s', i, resp[i])

    async def handle_DATA(self, server, session, envelope):
        # have all recipients

        status = [ None ] * len(envelope.transactions)

        blob = InlineBlob(envelope.content)

        # TODO cf inflight waiting in rest endpoint code
        futures = []
        for i,t in enumerate(envelope.transactions):
            futures.append(self.loop.run_in_executor(
                None, lambda: SmtpHandler.append_data(
                    status, i, t, last=True, blob=blob)))

        timeout = None
        if self.msa:
            timeout = 5
        else:
            timeout = 300

        done, pending = await asyncio.wait(futures, timeout=timeout,
                                           return_when=asyncio.ALL_COMPLETED)

        if self.msa and pending:
            for t in envelope.transactions:
                t.set_durable()
            return b'250 smtp gw submission accepted upstream timeout will retry'

        logging.info('handle_DATA %s', status)
        if len(pending) == len(envelope.transactions):
            # TODO: abort transactions
            return Response(400, 'all upstream transactions timed out')
        # if every transaction has the same status, return that
        s0 = status[0]
        for s in status[1:]:
            if (s0 is None) != (s is None): break
            elif s0.code/100 != s.code/100: break
        else:
            if s0.ok():
                return b'250 smtp gw accepted upstream sync success'
            elif self.msa and s0.temp():
                pass
                #return b'250 smtp gw submission accepted upstream temp will retry'
            else:
                return s0.to_smtp_resp()

        for t in envelope.transactions:
            t.set_durable()
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
