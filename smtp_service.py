import asyncio
import time
import logging
from typing import Optional, List, Tuple
from functools import partial
import ssl

from aiosmtpd.smtp import SMTP
from aiosmtpd.controller import Controller

from blob import Blob, InlineBlob
from response import ok_resp, to_smtp_resp
from smtp_auth import Authenticator
from response import Response
from filter import HostPort, Mailbox, TransactionMetadata

class SmtpHandler:
    def __init__(self, endpoint_factory, max_rcpt=None,
                 timeout_mail=10,
                 timeout_rcpt=60,
                 timeout_data=330):
        self.endpoint_factory = endpoint_factory
        self.max_rcpt = max_rcpt
        self.next_blob_id = 0
        self.timeout_mail = timeout_mail
        self.timeout_rcpt = timeout_rcpt
        self.timeout_data = timeout_data

    # TODO would be nice to abort the upstream transaction if the
    # client goes away, handle_QUIT(), subclass aiosmtpd.smtp.SMTP and
    # override connection_lost()?

    async def handle_RSET(self, server, session, envelope):
        envelope.endpoint = None
        envelope.tx = None
        return b'250 ok'

    def _update_tx(self, endpoint, tx):
        logging.info('SmtpHandler._update_tx')
        endpoint.on_update(tx)
        logging.info('SmtpHandler._update_tx done')

    async def handle_MAIL(
            self, server, session, envelope, mail_from, mail_esmtp):
        envelope.endpoint = self.endpoint_factory()
        envelope.tx = TransactionMetadata()

        # TODO propagate session.host_name into tx ehlo host
        logging.debug('handle_MAIL %s %s', session.host_name, session.peer)
        if session.peer:
            # TODO this is always an ascii IP? try to reverse resolve
            # and pass along for Received: headers if nothing else
            envelope.tx.remote_host = HostPort.from_seq(session.peer)

        # can't quite dig
        # session.transport.get_extra_info('sockname') out of aiosmtpd
        # envelope.tx.local_host = HostPort.from_seq(...)
        envelope.tx.mail_from = Mailbox(mail_from, mail_esmtp)
        fut = server.loop.run_in_executor(
            None, lambda: self._update_tx(envelope.endpoint, envelope.tx))
        await asyncio.wait([fut], timeout=self.timeout_mail)
        logging.info('mail resp %s', envelope.tx.mail_response)
        if envelope.tx.mail_response is None:
            return b'450 MAIL upstream timeout/internal err'
        if envelope.tx.mail_response.ok():
            envelope.mail_from = mail_from
            envelope.mail_options.extend(mail_esmtp)
            envelope.rcpt_i = 0
        return envelope.tx.mail_response.to_smtp_resp()

    async def handle_RCPT(
            self, server, session, envelope, rcpt_to, rcpt_esmtp):

        if self.max_rcpt and (len(envelope.rcpt_tos) > self.max_rcpt):
            return b'452-4.5.3 too many recipients (max %d)' % self.max_rcpt

        envelope.tx = TransactionMetadata(
            rcpt_to=[Mailbox(rcpt_to, rcpt_esmtp)])
        fut = server.loop.run_in_executor(
            None, lambda: self._update_tx(envelope.endpoint, envelope.tx))

        await asyncio.wait([fut], timeout=self.timeout_rcpt)

        logging.info('handle_RCPT rcpt_response %s', envelope.tx.rcpt_response)

        # for now without pipelining we send one rcpt upstream at a time
        if len(envelope.tx.rcpt_response) != 1:
            return b'450 RCPT upstream timeout/internal err'
        rcpt_resp = envelope.tx.rcpt_response[0]

        envelope.rcpt_i += 1
        if rcpt_resp is None:
            return b'450 RCPT upstream timeout/internal err'
        if rcpt_resp.ok():
            envelope.rcpt_tos.append(rcpt_to)
            #XXX envelope.rcpt_options.append(rcpt_esmtp)
        return rcpt_resp.to_smtp_resp()

    def append_data(self, envelope, blob : Blob):
        logging.info('SmtpHandler.append_data len=%d', blob.len())
        tx = TransactionMetadata()
        tx.body_blob = blob
        envelope.endpoint.on_update(tx)
        envelope.tx.data_response = tx.data_response
        logging.info('SmtpHandler.append_data %s', envelope.tx.data_response)

    def get_blob_id(self):
        id = 'gw_blob_%d' % self.next_blob_id
        self.next_blob_id += 1
        return id

    async def handle_DATA(self, server, session, envelope):
        # framework enforces this
        assert(envelope.rcpt_tos)

        blob = InlineBlob(envelope.content, id=self.get_blob_id())

        fut = server.loop.run_in_executor(
                None, lambda: self.append_data(envelope, blob=blob))
        await asyncio.wait([fut], timeout=self.timeout_data)
        return envelope.tx.data_response.to_smtp_resp()


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


def service(endpoint,
            hostname="localhost", port=9025, cert=None, key=None,
            auth_secrets_path=None, max_rcpt=None,
            rcpt_timeout=None, data_timeout=None):
    # DEBUG logs message contents!
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')

    if cert and key:
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_context.load_cert_chain(cert, key)
    else:
        ssl_context = None
    auth = Authenticator(auth_secrets_path) if auth_secrets_path else None
    handler = SmtpHandler(endpoint, max_rcpt, rcpt_timeout, data_timeout)
    controller = ControllerTls(handler,
                               hostname, port, ssl_context,
                               auth)
    handler.loop = controller.loop
    controller.start()
