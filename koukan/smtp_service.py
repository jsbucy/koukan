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
    TransactionMetadata )
from koukan.filter_chain import FilterChain
from koukan.executor import Executor

_next_cx = 0
_next_cx_mu = Lock()
def next_cx():
    global _next_cx, _next_cx_mu
    with _next_cx_mu:
        rv = _next_cx
        _next_cx += 1
    return rv

ChainFactory = Callable[[], FilterChain]

class SmtpHandler:
    chain_factory : ChainFactory
    executor : Executor
    smtp : Optional[SMTP] = None
    cx_id : str  # connection id, log token

    chain : Optional[FilterChain] = None

    local_socket = None
    peername = None

    ehlo = False
    quit = False

    proxy_protocol = False
    remote_host : Optional[HostPort] = None
    local_host : Optional[HostPort] = None
    refresh_interval : int
    last_refresh : float = 0
    chunk_size : int

    def __init__(self, chain_factory : ChainFactory,
                 executor : Executor,
                 timeout_mail=10,
                 timeout_rcpt=60,
                 timeout_data=330,
                 refresh_interval=30,
                 chunk_size=2**20):
        self.chain_factory = chain_factory
        self.executor = executor

        self.timeout_mail = timeout_mail
        self.timeout_rcpt = timeout_rcpt
        self.timeout_data = timeout_data

        self.cx_id = 'cx%d' % next_cx()
        self.refresh_interval = refresh_interval
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
                (str(proxy_data.dst_addr), proxy_data.dst_port))
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
        if self.chain is None:
            return
        if self.chain is not None:
            self.chain.tx.cancelled = True
            fut = self.executor.submit(
                partial(self._update_tx,
                        self.cx_id, self.chain), timeout=0)
        self.chain = None

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

    def _update_tx(self, cx_id, chain):
        logging.debug('SmtpHandler._update_tx %s', cx_id)
        chain.update()
        logging.debug('SmtpHandler._update_tx %s done', cx_id)

    async def handle_MAIL(self, server : SMTP,
                          session : Session,
                          envelope : Envelope,
                          mail_from : str,
                          mail_esmtp : List[str]) -> str:
        self.chain = self.chain_factory()
        self.chain.init(TransactionMetadata())
        assert self.chain.tx is not None
        self.chain.tx.smtp_meta = {
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
            self.chain.tx.remote_host = self.remote_host
        if self.local_host is not None:
            self.chain.tx.local_host = self.local_host

        params = [EsmtpParam.from_str(s) for s in mail_esmtp]
        self.chain.tx.mail_from = Mailbox(mail_from, params)
        fut = self.executor.submit(
            lambda: self._update_tx(self.cx_id, self.chain), timeout=0)
        if fut is None:
            return '450 server busy'
        await asyncio.wait([asyncio.wrap_future(fut)],
                           timeout=self.timeout_mail)
        logging.debug('handle_MAIL wait fut done')
        logging.info('SmtpHandler.handle_MAIL %s resp %s',
                     self.cx_id, self.chain.tx.mail_response)
        if self.chain.tx.mail_response is None:
            return '450 MAIL upstream timeout/internal err'
        if self.chain.tx.mail_response.ok():
            # aiosmtpd expects this
            envelope.mail_from = mail_from
            envelope.mail_options.extend(mail_esmtp)
        return self.chain.tx.mail_response.to_smtp_resp()

    async def handle_RCPT(self, server : SMTP,
                          session : Session,
                          envelope : Envelope,
                          rcpt_to : str,
                          rcpt_esmtp : List[str]) -> str:
        assert self.chain is not None
        assert self.chain.tx is not None

        logging.info('SmtpHandler.handle_RCPT %s %s %s',
                     self.cx_id, rcpt_to, rcpt_esmtp)
        params = [EsmtpParam.from_str(s) for s in rcpt_esmtp]

        rcpt_num = len(self.chain.tx.rcpt_to)
        self.chain.tx.rcpt_to.append(Mailbox(rcpt_to, params))
        fut = self.executor.submit(
            lambda: self._update_tx(self.cx_id, self.chain), timeout=0)
        if fut is None:
            return '450 server busy'
        await asyncio.wait([asyncio.wrap_future(fut)],
                           timeout=self.timeout_rcpt)

        logging.info('SmtpHandler.handle_RCPT %s response %s',
                     self.cx_id, self.chain.tx.rcpt_response)

        # for now without pipelining we send one rcpt upstream at a time
        if len(self.chain.tx.rcpt_response) != len(self.chain.tx.rcpt_to):
            return '450 RCPT upstream timeout/internal err'
        rcpt_resp = self.chain.tx.rcpt_response[rcpt_num]

        if rcpt_resp is None:
            return '450 RCPT upstream timeout/internal err'
        if rcpt_resp.ok():
            # aiosmtpd expects this
            envelope.rcpt_tos.append(rcpt_to)
            envelope.rcpt_options.extend(rcpt_esmtp)
        return rcpt_resp.to_smtp_resp()

    async def handle_DATA(self, server : SMTP,
                          session : Session,
                          envelope : Envelope) -> str:
        assert isinstance(envelope.content, bytes)
        resp = await self.handle_DATA_CHUNK(
            server, session, envelope,
            envelope.content, decoded_data=None, last=True)
        assert resp is not None
        return resp

    async def handle_DATA_CHUNK(self, server : SMTP,
                                session : Session,
                                envelope : Envelope,
                                data : bytes,
                                decoded_data : Optional[str],
                                last : bool) -> Optional[str]:
        assert self.chain is not None
        assert self.chain.tx is not None
        now = time.monotonic()
        if self.chain.tx.body is None:
            self.chain.tx.body = InlineBlob(b'')
            self.last_refresh = now
        body = self.chain.tx.body
        assert isinstance(body, InlineBlob)
        assert body.content_length() is None
        assert isinstance(self.chain.tx.body, InlineBlob)
        if last or (self.chain.tx.body.available() + len(data) <= self.chunk_size):
            body.append(data, last)
            data = b''
        stale = now - self.last_refresh > self.refresh_interval
        if not last and not stale and not data:
            return None
        self.last_refresh = now

        logging.info('SmtpHandler.handle_DATA_CHUNK %s %d bytes, last: %s',
                     self.cx_id, body.available(), last)

        fut = self.executor.submit(
            lambda: self._update_tx(self.cx_id, self.chain), timeout=0)
        if fut is None:
            return '450 server busy'
        await asyncio.wait([asyncio.wrap_future(fut)],
                           timeout=self.timeout_data)
        logging.info('SmtpHandler.handle_DATA_CHUNK %s resp %s',
                     self.cx_id, self.chain.tx.data_response)
        self.chain.tx.body.trim_front(self.chain.tx.body.len())

        if data:
            body.append(data, last)

        if self.chain.tx.data_response is None:
            assert not last
            return None
        data_resp = self.chain.tx.data_response.to_smtp_resp()
        self.tx = None
        return data_resp

    # TODO send heartbeat update
    # async def handle_NOOP(server : SMTP,
    #                       session : Session,
    #                       envelope : Envelope,
    #                       arg: Any):
    #     pass


SmtpHandlerFactory = Callable[[], SmtpHandler]
class ControllerTls(Controller):
    # don't clash with aiosmtpd.Controller.ssl_context!
    controller_tls_ssl_context : Optional[ssl.SSLContext] = None
    smtp_handler_factory : SmtpHandlerFactory
    enable_bdat = False
    smtps = False

    def __init__(self, host : str, port : int,
                 ssl_context : Optional[ssl.SSLContext],
                 auth,
                 smtp_handler_factory : SmtpHandlerFactory,
                 proxy_protocol_timeout : Optional[int] = None,
                 enable_bdat=False,
                 chunk_size : Optional[int] = None,
                 smtps=False):
        self.controller_tls_ssl_context = ssl_context
        self.proxy_protocol_timeout = proxy_protocol_timeout
        self.auth = auth
        self.smtp_handler_factory = smtp_handler_factory
        self.enable_bdat = enable_bdat
        self.chunk_size = chunk_size
        self.smtps = smtps

        if proxy_protocol_timeout is not None and smtps:
            logging.warning('proxy_protocol is not compatible with smtps in aiosmtpd https://github.com/aio-libs/aiosmtpd/issues/559')

        # The aiosmtpd docs don't discuss this directly but it seems
        # like this handler= is only used by the default implementation of
        # factory() which is moot if you override it like this.
        super(Controller, self).__init__(
            handler=None, hostname=host, port=port,
            ssl_context=ssl_context if smtps else None)

    def factory(self):
        handler = self.smtp_handler_factory()
        handler.loop = self.loop

        kwargs = {}
        if self.enable_bdat:
            kwargs['enable_BDAT'] = True
        if self.chunk_size:
            kwargs['chunk_size'] = self.chunk_size

        # TODO aiosmtpd supports LMTP so we could add that though it
        # is not completely trivial due to LMTP's per-recipient data
        # responses https://github.com/jsbucy/koukan/issues/2
        ssl_context = (self.controller_tls_ssl_context if not self.smtps
                       else None)
        smtp = SMTP(handler,
                    #require_starttls=True,
                    enable_SMTPUTF8 = True,  # xxx config
                    tls_context=ssl_context,
                    authenticator=self.auth,
                    proxy_protocol_timeout=self.proxy_protocol_timeout,
                    **kwargs)
        handler.set_smtp(smtp)
        return smtp

def service(smtp_handler_factory : SmtpHandlerFactory,
            hostname="localhost", port=9025,
            cert=None, key=None,
            auth_secrets_path=None,
            proxy_protocol_timeout : Optional[int] = None,
            enable_bdat = False,
            chunk_size : Optional[int] = None,
            smtps = False
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
        enable_bdat = enable_bdat,
        chunk_size = chunk_size,
        smtps = smtps)

    controller.start()
    return controller
