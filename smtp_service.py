from typing import Callable

import asyncio
import time
import logging
from typing import Optional, List, Tuple
from functools import partial
import ssl

from aiosmtpd.smtp import SMTP
from aiosmtpd.controller import Controller

from blob import Blob, InlineBlob
from response import ok_resp, to_smtp_resp, Response
from smtp_auth import Authenticator
from filter import EsmtpParam, Filter, HostPort, Mailbox, TransactionMetadata

connection_id = 0

class SmtpHandler:
    endpoint_factory : Callable[[], Filter]
    smtp : Optional[SMTP] = None
    cx_id : str  # connection id, log token

    endpoint : Optional[Filter] = None
    tx : Optional[TransactionMetadata] = None

    def __init__(self, endpoint_factory : Callable[[], Filter],
                 timeout_mail=10,
                 timeout_rcpt=60,
                 timeout_data=330):
        self.endpoint_factory = endpoint_factory
        self.next_blob_id = 0
        self.timeout_mail = timeout_mail
        self.timeout_rcpt = timeout_rcpt
        self.timeout_data = timeout_data

        global connection_id
        self.cx_id = 'cx%d' % ++connection_id

    # TODO would be nice to abort the upstream transaction if the
    # client goes away, handle_QUIT(), subclass aiosmtpd.smtp.SMTP and
    # override connection_lost()?

    async def handle_RSET(self, server, session, envelope):
        self.endpoint = None
        self.tx = None
        return b'250 ok'

    def _update_tx(self, endpoint, tx):
        logging.info('SmtpHandler._update_tx %s', self.cx_id)
        endpoint.on_update(tx)
        logging.info('SmtpHandler._update_tx %s done', self.cx_id)

    async def handle_MAIL(
            self, server, session, envelope, mail_from : str,
            mail_esmtp : List[str]):
        self.endpoint = self.endpoint_factory()
        self.tx = TransactionMetadata()

        self.tx.smtp_meta = {
            'ehlo_host': session.host_name,
            'esmtp': session.extended_smtp,
            'tls': session.ssl is not None,
            'auth': session.authenticated
        }

        local_socket = None
        if self.smtp is not None:
            local_socket = self.smtp.transport.get_extra_info('sockname')

        logging.info('SmtpHandler.handle_MAIL %s %s %s %s',
                     self.cx_id, session.host_name, session.peer, local_socket)
        if session.peer:
            self.tx.remote_host = HostPort.from_seq(session.peer)

        if local_socket:
            self.tx.local_host = HostPort.from_seq(local_socket)

        params = [EsmtpParam.from_str(s) for s in mail_esmtp]
        self.tx.mail_from = Mailbox(mail_from, params)
        fut = server.loop.run_in_executor(
            None, lambda: self._update_tx(self.endpoint, self.tx))
        await asyncio.wait([fut], timeout=self.timeout_mail)
        logging.info('SmtpHandler.handle_MAIL %s mail resp %s',
                     self.cx_id, self.tx.mail_response)
        if self.tx.mail_response is None:
            return b'450 MAIL upstream timeout/internal err'
        if self.tx.mail_response.ok():
            # aiosmtpd expects this
            envelope.mail_from = mail_from
            envelope.mail_options.extend(mail_esmtp)
        return self.tx.mail_response.to_smtp_resp()

    async def handle_RCPT(
            self, server, session, envelope, rcpt_to, rcpt_esmtp):

        params = [EsmtpParam.from_str(s) for s in rcpt_esmtp]

        self.tx = TransactionMetadata(
            rcpt_to=[Mailbox(rcpt_to, params)])
        fut = server.loop.run_in_executor(
            None, lambda: self._update_tx(self.endpoint, self.tx))

        await asyncio.wait([fut], timeout=self.timeout_rcpt)

        logging.info('SmtpHandler.handle_RCPT %s rcpt_response %s',
                     self.cx_id, self.tx.rcpt_response)

        # for now without pipelining we send one rcpt upstream at a time
        if len(self.tx.rcpt_response) != 1:
            return b'450 RCPT upstream timeout/internal err'
        rcpt_resp = self.tx.rcpt_response[0]

        if rcpt_resp is None:
            return b'450 RCPT upstream timeout/internal err'
        if rcpt_resp.ok():
            # aiosmtpd expects this
            envelope.rcpt_tos.append(rcpt_to)
            envelope.rcpt_options.append(rcpt_esmtp)
        return rcpt_resp.to_smtp_resp()

    def append_data(self, envelope, blob : Blob):
        logging.info('SmtpHandler.append_data %s len=%d',
                     self.cx_id, blob.len())
        tx = TransactionMetadata()
        tx.body_blob = blob
        self.endpoint.on_update(tx)
        self.tx.data_response = tx.data_response
        logging.info('SmtpHandler.append_data %s %s',
                     self.cx_id, self.tx.data_response)

    def get_blob_id(self):
        id = 'gw_blob_%d' % self.next_blob_id
        self.next_blob_id += 1
        return id

    async def handle_DATA(self, server, session, envelope):
        blob = InlineBlob(envelope.content, id=self.get_blob_id())

        fut = server.loop.run_in_executor(
                None, lambda: self.append_data(envelope, blob=blob))
        await asyncio.wait([fut], timeout=self.timeout_data)
        return self.tx.data_response.to_smtp_resp()


class ControllerTls(Controller):
    def __init__(self, host, port, ssl_context, auth,
                 endpoint_factory, max_rcpt, rcpt_timeout, data_timeout):
        self.tls_controller_context = ssl_context
        self.auth = auth
        self.endpoint_factory = endpoint_factory
        self.max_rcpt = max_rcpt
        self.rcpt_timeout = rcpt_timeout
        self.data_timeout = data_timeout
        # The aiosmtpd docs don't discuss this directly but it seems
        # like this handler= is only used by the default implementation of
        # factory() which is moot if you override it like this.
        super(Controller, self).__init__(
            handler=None, hostname=host, port=port)

    def factory(self):
        handler = SmtpHandler(
            self.endpoint_factory, self.max_rcpt, self.rcpt_timeout,
            self.data_timeout)
        handler.loop = self.loop
        smtp = SMTP(handler,
                    #require_starttls=True,
                    enable_SMTPUTF8 = True,  # xxx config
                    tls_context=self.tls_controller_context,
                    authenticator=self.auth)
        handler.smtp = smtp
        return smtp

def service(endpoint_factory,
            hostname="localhost", port=9025, cert=None, key=None,
            auth_secrets_path=None, max_rcpt=None,
            rcpt_timeout=None, data_timeout=None):
    if cert and key:
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_context.load_cert_chain(cert, key)
    else:
        ssl_context = None
    auth = Authenticator(auth_secrets_path) if auth_secrets_path else None
    controller = ControllerTls(
        hostname, port, ssl_context,
        auth,
        endpoint_factory, max_rcpt, rcpt_timeout, data_timeout)
    controller.start()
