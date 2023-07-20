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
    def __init__(self, endpoint_factory):
        self.endpoint_factory = endpoint_factory

    async def handle_EHLO(
            self, server, session, envelope, hostname, responses):
        # TODO no hook for connect, can override SMTP.connection_made()
        if not hasattr(session, "connection"):
            session.endpoint = self.endpoint_factory()
            greeting = session.endpoint.on_connect(session.peer, None)
            if greeting.err():
                return [greeting.to_smtp_resp()]
        connection = session.endpoint
        ehlo_resp, esmtp = connection.on_ehlo(hostname)
        if ehlo_resp.err():
            return [ehlo_resp.to_smtp_resp()]

        session.host_name = hostname
        # TODO esmtp
        return responses

    # XXX
    async def handle_RSET(self, server, session, envelope):
        pass

    async def handle_MAIL(
            self, server, session, envelope, address, options):
        connection = session.endpoint
        resp, _ = connection.start_transaction(
            address, options, forward_path=[])
        if resp.ok():
            envelope.mail_from = address
            envelope.mail_options.extend(options)
        print(resp)
        return resp.to_smtp_resp()


    async def handle_RCPT(
            self, server, session, envelope, address, rcpt_options):
        resp = session.endpoint.add_rcpt(address, rcpt_options)
        if resp.ok():
            envelope.rcpt_tos.append(address)
        return resp.to_smtp_resp()

    async def handle_DATA(self, server, session, envelope):
        # no hook for data chunks but could override handler in SMTP
        # object to get at it

        data_resp = session.endpoint.append_data(last=True, chunk_id=0)
        if data_resp.err():
            return data_resp.to_smtp_resp()
        chunk_resp,result_len = session.endpoint.append_data_chunk(
            chunk_id=0, offset=0,
            d=envelope.content, last=True)
        if chunk_resp.err():
            return chunk_resp.to_smtp_resp()
        elif result_len < len(envelope.content):
            return Resposne(400, "didn't put all data")
        return session.endpoint.get_transaction_status().to_smtp_resp()


class ControllerTls(Controller):
    def __init__(self, handler, host, port, ssl_context, auth):
        self.tls_controller_context = ssl_context
        self.auth = auth
        super(Controller, self).__init__(
            handler, hostname=host, port=port)

    def factory(self):
        return SMTP(self.handler, require_starttls=True,
                    tls_context=self.tls_controller_context,
                    authenticator=self.auth)


def service(endpoint, hostname="localhost", port=9025, cert=None, key=None,
            auth_secrets_path=None):
    # DEBUG logs message contents!
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(message)s')

    if cert is None or key is None:
        ssl_context = None
    else:
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_context.load_cert_chain(cert, key)
    auth = Authenticator(auth_secrets_path) if auth_secrets_path else None
    controller = ControllerTls(SmtpHandler(endpoint),
                               hostname, port, ssl_context,
                               auth)
    controller.start()
    while True:
        time.sleep(60)  # or wait for signal
