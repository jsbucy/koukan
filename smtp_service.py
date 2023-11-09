from aiosmtpd.smtp import SMTP
from aiosmtpd.controller import Controller
import ssl

import asyncio
import time
import logging

from functools import partial

from blob import InlineBlob

from typing import Optional, List, Tuple


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

        self.next_blob_id = 0

    # TODO would be nice to abort the upstream transaction if the
    # client goes away, handle_QUIT(), subclass aiosmtpd.smtp.SMTP and
    # override connection_lost()?

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


    def start(self, rresp : List[Optional[Response]], trans, local, remote,
              mail_from, transaction_esmtp,
              rcpt, rcpt_esmtp):
        logging.info('SmtpHandler.start')
        rresp[0] = trans.start(local, remote, mail_from, transaction_esmtp,
                               rcpt, rcpt_esmtp)
        logging.info('SmtpHandler.start done %s', rresp[0])

    async def handle_RCPT(
            self, server, session, envelope, address, rcpt_options):
        if self.max_rcpt and (len(envelope.rcpt_tos) > self.max_rcpt):
            return b'452-4.5.3 too many recipients'

        transaction = self.endpoint_factory()
        start_time = time.monotonic()

        rresp : List[Optional[Response]] = [None]
        fut = server.loop.run_in_executor(
            None, lambda: self.start(rresp, transaction,
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
                logging.info('msa_async')
                envelope.msa_async = True
                # '250 msa rcpt upstream temp continue'
        else:  # mx
            if resp.err():
                return resp.to_smtp_resp()

        envelope.transactions.append(transaction)
        envelope.rcpt_tos.append(address)

        return '250 RCPT ok'

    def append_data(self, resp, i, trans, last, blob, mx_multi_rcpt):
        logging.info('SmtpHandler.append_data %d last=%s len=%d',
                     i, last, blob.len())
        resp[i] = trans.append_data(last, blob, mx_multi_rcpt=mx_multi_rcpt)
        logging.info('SmtpHandler.append_data %d %s', i, resp[i])

    def set_durable(self, rresp, i, trans):
        logging.info('SmtpHandler.set_durable %d', i)
        r = trans.set_durable()
        logging.info('SmtpHandler.set_durable %d %s', i, r)
        rresp[i] = r

    def get_blob_id(self):
        id = 'gw_blob_%d' % self.next_blob_id
        self.next_blob_id += 1
        return id

    async def handle_DATA(self, server, session, envelope):
        # framework enforces this
        assert(envelope.rcpt_tos)

        sstatus = [ None ] * len(envelope.transactions)

        blob = InlineBlob(envelope.content, id=self.get_blob_id())

        mx_multi_rcpt = (not self.msa) and (len(envelope.transactions) > 1)

        # TODO cf inflight waiting in rest endpoint code
        futures = []
        for i,t in enumerate(envelope.transactions):
            logging.info('SmtpHandler.handle_DATA dispatch append %d', i)
            futures.append(server.loop.run_in_executor(
                None, partial(
                    lambda i, t: self.append_data(
                        sstatus, i, t, last=True, blob=blob,
                        mx_multi_rcpt=mx_multi_rcpt),
                    i, t)))

        # xxx pass this timeout to RestEndpoint.append_data
        timeout = None
        if self.msa:
            # if rcpt already tempfailed (self.msa_async), that
            # propagates to json final transaction status so the get
            # for that after the last append will return immediately.
            timeout = MSA_DATA_WAIT
        else:  # mx
            timeout = MX_DATA_WAIT
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
                logging.info(s)
                major = int(s.code/100)
                if i == 0:
                    s0 = s
                    same_major = major
                else:
                    if major != same_major:
                        same_major = None
                status.append(s)
        logging.info('same_major %s', same_major)
        if self.msa:
            if not envelope.msa_async:
                if same_major == 2 or same_major == 5:
                    assert(s0 is not None)
                    return s0.to_smtp_resp()
        else:  # mx
            if same_major is not None:
                return s0.to_smtp_resp()

        rresp = [None] * len(envelope.transactions)
        futures = []
        for i,t in enumerate(envelope.transactions):
            futures.append(server.loop.run_in_executor(
                None, partial(lambda i, t:
                              self.set_durable(rresp, i, t), i, t)))
        #done, pending =
        await asyncio.wait(
            futures, timeout=5, return_when=asyncio.ALL_COMPLETED)
        if any(map(lambda r: r is None or r.err(), rresp)):
            return b'400 set_durable timeout/err'
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
