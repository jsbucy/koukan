from aiosmtpd.smtp import SMTP
from aiosmtpd.controller import Controller
import ssl

import asyncio
import time
import logging

import requests

from typing import Optional
from typing import Tuple

from response import ok_resp, to_smtp_resp

from smtp_auth import Authenticator


class SmtpHandler:
    def __init__(self, endpoint_factory, msa, max_rcpt):
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
        return b'250 MAIL ok'


    async def handle_RCPT(
            self, server, session, envelope, address, rcpt_options):
        # TODO multi-rcpt doesn't completely work until durable retry
        # is implemented in the router
        if self.max_rcpt and (len(envelope.rcpt_tos) > self.max_rcpt):
            return '452-4.5.3 too many recipients'

        transaction = self.endpoint_factory()
        resp = transaction.start(
            None, None,
            envelope.mail_from, envelope.mail_options,
            address, rcpt_options)
        if resp.ok():
            envelope.transactions.append(transaction)
            envelope.rcpt_tos.append(address)

        return resp.to_smtp_resp()

    async def handle_DATA(self, server, session, envelope):
        # have all recipients

        resp, blob_id = envelope.transactions[0].append_data(
            d=envelope.content, last=True)
        for t in envelope.transactions[1:]:
            if blob_id:
                resp,_ = t.append_data(d=None, blob_id=blob_id, last=True)
            else:
                resp,_ = t.append_data(d=envelope.content, blob_id=None, last=True)

        # submission is fire&forget
        #if self.msa:
        #    for t in envelope.transactions:
        #        t.set_durable(bounce=True)
        #    return b'250 smtp gw submission accepted'

        status = [ None for _ in envelope.transactions ]
        for i in range(0,31):
            for i,t in enumerate(envelope.transactions):
                if not status[i]:
                    if i < 30:
                        status[i] = t.get_status()
                    else:
                        status[i] = Response(400, 'smtp gw transaction timeout')
            if None not in status:
                break
            time.sleep(10)

        # we don't want to time out here in the common case and leave
        # the operation running since the client will probably retry =
        # dupes? waiting is typically endpoint unreachable or
        # contention? need to propagate a timeout downstream?

        # if every transaction has the same status, return that
        s0 = status[i]
        for s in status[1:]:
            if s0.code/100 != s.code/100:
                break
        else:
            return s0.to_smtp_resp()

        for t in envelope.transactions:
            t.set_durable(bounce=True)
        return b'250 smtp gw accepted'


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
    controller = ControllerTls(SmtpHandler(endpoint, msa, max_rcpt),
                               hostname, port, ssl_context,
                               auth)
    controller.start()
