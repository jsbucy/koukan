# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Callable, List, Optional, Tuple
import asyncio
import time
import logging

from functools import partial
import ssl
from threading import Lock

from aiosmtpd.smtp import (
    Envelope,
    Session,
    SMTP,
    ProxyData )
from aiosmtpd.controller import Controller

from koukan.blob import Blob, InlineBlob
from koukan.response import Response
from koukan.smtp_auth import Authenticator
from koukan.filter import (
    EsmtpParam,
    HostPort,
    Mailbox,
    SyncFilter,
    TransactionMetadata )
from koukan.executor import Executor

_next_cx = 0
_next_cx_mu = Lock()
def next_cx():
    global _next_cx, _next_cx_mu
    with _next_cx_mu:
        rv = _next_cx
        _next_cx += 1
    return rv

class SmtpHandler:
    endpoint_factory : Callable[[], SyncFilter]
    executor : Executor
    smtp : Optional[SMTP] = None
    cx_id : str  # connection id, log token

    endpoint : Optional[SyncFilter] = None
    tx : Optional[TransactionMetadata] = None

    local_socket = None
    peername = None

    ehlo = False
    quit = False

    proxy_protocol = False
    remote_host : Optional[HostPort] = None
    local_host : Optional[HostPort] = None

    prev_chunk : bytes
    chunk_size : int

    def __init__(self, endpoint_factory : Callable[[], SyncFilter],
                 executor : Executor,
                 timeout_mail=10,
                 timeout_rcpt=60,
                 timeout_data=330,
                 chunk_size = 2**16):
        self.endpoint_factory = endpoint_factory
        self.executor = executor

        self.timeout_mail = timeout_mail
        self.timeout_rcpt = timeout_rcpt
        self.timeout_data = timeout_data

        self.cx_id = 'cx%d' % next_cx()
        self.prev_chunk = b''
        self.chunk_size = chunk_size

    def set_smtp(self, smtp):
        self.smtp = smtp
        logging.info('SmtpHandler %s created', self.cx_id)
        # self.smtp.transport doesn't get set up until after
        # Controller.factory returns?

    # TODO would be nice to abort the upstream transaction if the
    # client goes away, subclass aiosmtpd.smtp.SMTP and
    # override connection_lost()? In the meantime this gives us a
    # little visibility.
    def __del__(self):
        if self.ehlo and not self.quit:
            logging.info('SmtpHandler.__del__ (never quit) %s', self.cx_id)
        self._cancel()

    async def handle_PROXY(self, server : SMTP,
                           session : Session,
                           envelope : Envelope,
                           proxy_data : ProxyData) -> bool:
        logging.info('%s proxy data %s', self.cx_id, proxy_data)
        self.proxy_protocol = True
        if proxy_data.src_addr:
            self.remote_host = HostPort.from_seq(
                (str(proxy_data.src_addr), proxy_data.src_port))
        if proxy_data.dst_addr:
            self.local_host = HostPort.from_seq(
                (str(proxy_data.dst_addr), proxy_data.src_port))
        return True

    def _ehlo(self, hostname, esmtp):
        self.local_socket = self.smtp.transport.get_extra_info('sockname')
        self.peername = self.smtp.transport.get_extra_info('peername')
        self.ehlo = True
        logging.info('SmtpHandler %s %s %s %s %s',
                     self.cx_id, 'EHLO' if esmtp else 'HELO',
                     hostname,
                     self.peername, self.local_socket)

    async def handle_EHLO(self, server : SMTP,
                          session : Session,
                          envelope :  Envelope,
                          hostname : str,
                          responses : List[str]) -> List[str]:
        session.host_name = hostname
        self._ehlo(hostname, esmtp=True)
        return responses

    async def handle_HELO(self, server : SMTP,
                          session : Session,
                          envelope : Envelope,
                          hostname : str) -> str:
        session.host_name = hostname
        self._ehlo(hostname, esmtp=False)
        return '250 {}'.format(server.hostname)

    def _cancel(self):
        self.prev_chunk = b''
        if self.endpoint is None and self.tx is None:
            return
        if self.tx is not None:
            fut = self.executor.submit(
                partial(self._update_tx,
                        self.cx_id, self.endpoint, self.tx,
                        TransactionMetadata(cancelled=True)), timeout=0)
        self.endpoint = self.tx = None

    async def handle_QUIT(self, server : SMTP,
                          session : Session,
                          envelope : Envelope) -> str:
        logging.info('SmtpHandler.handle_QUIT %s', self.cx_id)
        self._cancel()
        self.quit = True
        # smtplib throws if the QUIT response isn't exactly 221
        return '221 ok'

    async def handle_RSET(self, server : SMTP,
                          session : Session,
                          envelope : Envelope) -> str:
        logging.info('SmtpHandler.handle_RSET %s', self.cx_id)
        self._cancel()
        return '250 ok'

    def _update_tx(self, cx_id, endpoint, tx, tx_delta):
        logging.debug('SmtpHandler._update_tx %s', cx_id)
        upstream_delta = endpoint.on_update(tx, tx_delta)
        logging.debug('SmtpHandler._update_tx %s done', cx_id)

    async def handle_MAIL(self, server : SMTP,
                          session : Session,
                          envelope : Envelope,
                          mail_from : str,
                          mail_esmtp : List[str]) -> str:
        self.endpoint = self.endpoint_factory()
        self.tx = TransactionMetadata()

        updated_tx = TransactionMetadata()
        updated_tx.smtp_meta = {
            'ehlo_host': session.host_name,
            'esmtp': session.extended_smtp,
            'tls': session.ssl is not None,
            'auth': session.authenticated
        }

        logging.info('SmtpHandler.handle_MAIL %s %s %s',
                     self.cx_id, mail_from, mail_esmtp)
        if not self.proxy_protocol:
            if self.peername:
                self.remote_host = HostPort.from_seq(self.peername)
            if self.local_socket:
                self.local_host = HostPort.from_seq(self.local_socket)

        if self.remote_host is not None:
            updated_tx.remote_host = self.remote_host
        if self.local_host is not None:
            updated_tx.local_host = self.local_host

        params = [EsmtpParam.from_str(s) for s in mail_esmtp]
        updated_tx.mail_from = Mailbox(mail_from, params)
        tx_delta = self.tx.delta(updated_tx)
        self.tx = updated_tx
        fut = self.executor.submit(
            lambda: self._update_tx(
                self.cx_id, self.endpoint, self.tx, tx_delta), timeout=0)
        if fut is None:
            return '450 server busy'
        await asyncio.wait([asyncio.wrap_future(fut)],
                           timeout=self.timeout_mail)
        logging.debug('handle_MAIL wait fut done')
        logging.info('SmtpHandler.handle_MAIL %s resp %s',
                     self.cx_id, self.tx.mail_response)
        if self.tx.mail_response is None:
            return '450 MAIL upstream timeout/internal err'
        if self.tx.mail_response.ok():
            # aiosmtpd expects this
            envelope.mail_from = mail_from
            envelope.mail_options.extend(mail_esmtp)
        return self.tx.mail_response.to_smtp_resp()

    async def handle_RCPT(self, server : SMTP,
                          session : Session,
                          envelope : Envelope,
                          rcpt_to : str,
                          rcpt_esmtp : List[str]) -> str:
        logging.info('SmtpHandler.handle_RCPT %s %s %s',
                     self.cx_id, rcpt_to, rcpt_esmtp)
        params = [EsmtpParam.from_str(s) for s in rcpt_esmtp]

        rcpt_num = len(self.tx.rcpt_to)
        updated_tx = self.tx.copy()
        updated_tx.rcpt_to.append(Mailbox(rcpt_to, params))
        tx_delta = self.tx.delta(updated_tx)
        self.tx = updated_tx
        fut = self.executor.submit(
            lambda: self._update_tx(
                self.cx_id, self.endpoint, self.tx, tx_delta), timeout=0)
        if fut is None:
            return '450 server busy'
        await asyncio.wait([asyncio.wrap_future(fut)],
                           timeout=self.timeout_rcpt)

        logging.info('SmtpHandler.handle_RCPT %s response %s',
                     self.cx_id, self.tx.rcpt_response)

        # for now without pipelining we send one rcpt upstream at a time
        if len(self.tx.rcpt_response) != len(self.tx.rcpt_to):
            return '450 RCPT upstream timeout/internal err'
        rcpt_resp = self.tx.rcpt_response[rcpt_num]

        if rcpt_resp is None:
            return '450 RCPT upstream timeout/internal err'
        if rcpt_resp.ok():
            # aiosmtpd expects this
            envelope.rcpt_tos.append(rcpt_to)
            envelope.rcpt_options.append(rcpt_esmtp)
        return rcpt_resp.to_smtp_resp()

    async def handle_DATA(self, server : SMTP,
                          session : Session,
                          envelope : Envelope) -> str:
        logging.info('SmtpHandler.handle_DATA %s %d bytes',
                     self.cx_id, len(envelope.content))

        blob = InlineBlob(envelope.content, last=True)

        updated_tx = self.tx.copy()
        updated_tx.body = blob
        tx_delta = self.tx.delta(updated_tx)
        self.tx = updated_tx
        fut = self.executor.submit(
            lambda: self._update_tx(
                self.cx_id, self.endpoint, self.tx, tx_delta), timeout=0)
        if fut is None:
            return '450 server busy'
        await asyncio.wait([asyncio.wrap_future(fut)],
                           timeout=self.timeout_data)
        logging.info('SmtpHandler.handle_DATA %s resp %s',
                     self.cx_id, self.tx.data_response)

        data_resp = self.tx.data_response.to_smtp_resp()
        self.tx = None
        return data_resp

    async def handle_DATA_CHUNK(self, server : SMTP,
                                session : Session,
                                envelope : Envelope,
                                data : bytes,
                                decoded_data : Optional[str],
                                last : bool):
        logging.info('SmtpHandler.handle_DATA_CHUNK %s %d bytes, last: %s',
                     self.cx_id, len(data), last)

        if ((len(self.prev_chunk) + len(data)) < self.chunk_size) or last:
            self.prev_chunk += data
            if not last:
                return None
            data = b''

        # In the current aiosmtpd dotstuff implementation, the
        # last==True chunk will always be empty but (at least the way
        # our chunked PUTs work) we can't do a 0 length PUT to set the
        # content-range overall length. So hold back the most recent
        # chunk so there's something to send if the last chunk is
        # empty
        emit = bool(self.prev_chunk)
        if self.prev_chunk:
            if self.tx.body is None:
                self.tx.body = InlineBlob(self.prev_chunk, last=last)
            else:
                body = self.tx.body
                assert isinstance(body, InlineBlob)
                assert body.content_length() is None
                body.append(self.prev_chunk, last)

        self.prev_chunk = data
        if not emit:
            return None

        logging.debug(self.tx.body)
        tx_delta = TransactionMetadata(body = self.tx.body)
        fut = self.executor.submit(
            lambda: self._update_tx(
                self.cx_id, self.endpoint, self.tx, tx_delta), timeout=0)
        if fut is None:
            return '450 server busy'
        await asyncio.wait([asyncio.wrap_future(fut)],
                           timeout=self.timeout_data)
        logging.info('SmtpHandler.handle_DATA_CHUNK %s resp %s',
                     self.cx_id, self.tx.data_response)
        self.tx.body.trim_front(self.tx.body.len())

        if self.tx.data_response is None:
            assert not last
            return None
        data_resp = self.tx.data_response.to_smtp_resp()
        self.tx = None
        return data_resp


SmtpHandlerFactory = Callable[[], SmtpHandler]
class ControllerTls(Controller):
    smtp_handler_factory : SmtpHandlerFactory

    def __init__(self, host, port, ssl_context, auth,
                 smtp_handler_factory : SmtpHandlerFactory,
                 proxy_protocol_timeout : Optional[int] = None,
                 enable_bdat=False):
        self.tls_controller_context = ssl_context
        self.proxy_protocol_timeout = proxy_protocol_timeout
        self.auth = auth
        self.smtp_handler_factory = smtp_handler_factory

        # The aiosmtpd docs don't discuss this directly but it seems
        # like this handler= is only used by the default implementation of
        # factory() which is moot if you override it like this.
        super(Controller, self).__init__(
            handler=None, hostname=host, port=port)

    def factory(self):
        handler = self.smtp_handler_factory()
        handler.loop = self.loop

        # TODO aiosmtpd supports LMTP so we could add that though it
        # is not completely trivial due to LMTP's per-recipient data
        # responses https://github.com/jsbucy/koukan/issues/2
        smtp = SMTP(handler,
                    #require_starttls=True,
                    enable_SMTPUTF8 = True,  # xxx config
                    tls_context=self.tls_controller_context,
                    authenticator=self.auth,
                    proxy_protocol_timeout=self.proxy_protocol_timeout)
        handler.set_smtp(smtp)
        return smtp

def service(smtp_handler_factory : SmtpHandlerFactory,
            hostname="localhost", port=9025,
            cert=None, key=None,
            auth_secrets_path=None,
            proxy_protocol_timeout : Optional[int] = None,
            enable_bdat = False
            ) -> ControllerTls:
    if cert and key:
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_context.load_cert_chain(cert, key)
    else:
        ssl_context = None
    auth = Authenticator(auth_secrets_path) if auth_secrets_path else None
    controller = ControllerTls(
        hostname, port, ssl_context,
        auth,
        proxy_protocol_timeout = proxy_protocol_timeout,
        smtp_handler_factory = smtp_handler_factory,
        enable_bdat = enable_bdat)

    controller.start()
    return controller
