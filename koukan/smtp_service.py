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
from koukan.sender import Sender

_next_cx = 0
_next_cx_mu = Lock()
def next_cx():
    global _next_cx, _next_cx_mu
    with _next_cx_mu:
        rv = _next_cx
        _next_cx += 1
    return rv

ChainFactory = Callable[[], FilterChain]

class Upstream:
    chain : FilterChain
    cx_id : str
    def __init__(self, chain, cx_id):
        self.chain = chain
        self.cx_id = cx_id

    # caller must ensure no concurrent calls to update()/cancel()
    def update(self):
        logging.debug('SmtpHandler._update_tx %s', self.cx_id)
        self.chain.update()
        logging.debug('SmtpHandler._update_tx %s done', self.cx_id)

    def cancel(self):
        assert self.chain.tx is not None
        tx = self.chain.tx
        assert not tx.cancelled and tx.data_response is None
        tx.cancelled = True
        logging.debug('%s cancel', self.cx_id)
        self.chain.update()
        logging.debug('%s cancel done', self.cx_id)

class SmtpHandler:
    chain_factory : ChainFactory
    executor : Executor
    smtp : Optional[SMTP] = None
    cx_id : str  # connection id, log token

    upstream : Optional[Upstream] = None

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
    sender : str
    tag : str

    def __init__(self, chain_factory : ChainFactory,
                 executor : Executor,
                 sender : str,
                 tag : str,
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
        self.sender = sender
        self.tag = tag

    def set_smtp(self, smtp):
        self.smtp = smtp
        logging.info('SmtpHandler %s created', self.cx_id)
        # self.smtp.transport doesn't get set up until after
        # Controller.factory returns?

    # Try to be polite and abort the upstream transaction if the
    # client goes away. Doing this wth the finalizer may not be timely but is
    # better than nothing.
    # TODO would be better to get an upcall from aiosmtpd but it
    # doesn't have a convenient hook for this? Possible to poll after
    # idle timeout?
    def __del__(self):
        if not self.ehlo or not self.quit:
            return
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
        return '250 ' + server.hostname

    def _cancel(self):
        if self.upstream is None:
            return
        upstream = self.upstream
        self.upstream = None
        if upstream.chain.tx is None:
            return
        tx = upstream.chain.tx
        if tx.cancelled or tx.data_response is not None:
            return
        logging.debug('%s cancel', self.cx_id)
        # This can be called from __del__() so this closure must not
        # access this SmtpHandler.
        # if we can't schedule this, there's nothing more we can do
        # TODO short wait ok here?
        cfut = self.executor.submit(upstream.cancel, timeout=0)
        if cfut is None:
            logging.debug('%s _cancel() could not schedule', self.cx_id)

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

    async def _update_upstream(self, timeout) -> Optional[str]:
        assert self.upstream is not None
        # TODO wait up to 10% of timeout to schedule rather than fastfail?
        cfut = self.executor.submit(self.upstream.update, timeout=0)
        if cfut is None:
            logging.debug('%s _update_upstream could not schedule', self.cx_id)
            return '450 server busy'
        afut = asyncio.wrap_future(cfut)
        done, pending = await asyncio.wait([afut], timeout=timeout)
        if afut in done:
            return None
        logging.debug('%s _update_upstream() timeout', self.cx_id)
        # Upstream is expected to respect the timeout so this is not
        # expected and may lead to an Executor watchdog timeout. The
        # upstream is an an undefined state at this point so don't try
        # to cancel here.
        self.upstream = None
        return '450 upstream timeout/internal err'

    async def handle_MAIL(self, server : SMTP,
                          session : Session,
                          envelope : Envelope,
                          mail_from : str,
                          mail_esmtp : List[str]) -> str:
        self.upstream = Upstream(self.chain_factory(), self.cx_id)
        self.upstream.chain.init(TransactionMetadata(
            sender=Sender(name=self.sender, tag=self.tag)))
        assert self.upstream.chain.tx is not None
        tx = self.upstream.chain.tx
        tx.smtp_meta = {
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
            tx.remote_host = self.remote_host
        if self.local_host is not None:
            tx.local_host = self.local_host

        params = [EsmtpParam.from_str(s) for s in mail_esmtp]
        # aiosmtpd.SMTP.smtp_MAIL() uses
        # email._header_value_parser.get_angle_addr() to consume the
        # address from the command line which strips angle brackets
        # from a non-empty address but returns '<>' verbatim.
        # aiosmtpd expects envelope.mail_from to be non-empty <> for
        # the rcpt command precondition check but the rest of our
        # stack expects '' for rfc5321 "null reverse-path"
        tx.mail_from = Mailbox(
            mail_from if mail_from != '<>' else '', params)
        if err := await self._update_upstream(self.timeout_mail):
            return err
        logging.info('SmtpHandler.handle_MAIL %s resp %s',
                     self.cx_id, tx.mail_response)
        if tx.mail_response is None:  # FilterChain/RestEndpoint bug
            # don't try to cancel after bug in upstream
            logging.debug('%s no mail_response', self.cx_id)
            self.upstream = None
            return '450 service unavailable'
        if tx.mail_response.ok():
            # aiosmtpd expects this
            envelope.mail_from = mail_from
            envelope.mail_options.extend(mail_esmtp)
        return tx.mail_response.to_smtp_resp()

    async def handle_RCPT(self, server : SMTP,
                          session : Session,
                          envelope : Envelope,
                          rcpt_to : str,
                          rcpt_esmtp : List[str]) -> str:
        if self.upstream is None:  # previous internal error
            return '450 service unavailable'
        assert self.upstream.chain.tx is not None
        tx = self.upstream.chain.tx
        logging.info('SmtpHandler.handle_RCPT %s %s %s',
                     self.cx_id, rcpt_to, rcpt_esmtp)
        params = [EsmtpParam.from_str(s) for s in rcpt_esmtp]

        rcpt_num = len(tx.rcpt_to)
        tx.rcpt_to.append(Mailbox(rcpt_to, params))
        if err := await self._update_upstream(self.timeout_rcpt):
            return err
        logging.info('SmtpHandler.handle_RCPT %s response %s',
                     self.cx_id, tx.rcpt_response)

        # For now without pipelining we send one rcpt upstream at a time.
        # No response after update -> bug.
        if ((len(tx.rcpt_response) != len(tx.rcpt_to)) or
            (rcpt_resp := tx.rcpt_response[rcpt_num]) is None):
            logging.debug('%s no rcpt_response %d', self.cx_id, rcpt_num)
            # don't try to cancel after bug in upstream
            self.upstream = None
            return '450 service unavailable'
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
        if self.upstream is None:  # previous internal error
            return '450 service unavailable'
        assert self.upstream.chain.tx is not None
        tx = self.upstream.chain.tx
        now = time.monotonic()
        if tx.body is None:
            tx.body = InlineBlob(b'')
            self.last_refresh = now
        body = tx.body
        assert isinstance(body, InlineBlob)
        assert body.content_length() is None
        assert isinstance(tx.body, InlineBlob)

        # Don't assume any relationship between aiosmtpd.SMTP.chunk_size and
        # SmtpHandler.chunk_size.
        while data:
            chunk_size = min(self.chunk_size - tx.body.available(), len(data))
            chunk_last = last and (chunk_size == len(data))
            body.append(data[0:chunk_size], chunk_last)
            data = data[chunk_size:]
            stale = now - self.last_refresh > self.refresh_interval
            if not last and not stale and not data:
                return None
            self.last_refresh = now

            logging.info('SmtpHandler.handle_DATA_CHUNK %s %d bytes, last: %s',
                         self.cx_id, body.available(), chunk_last)

            if err := await self._update_upstream(self.timeout_data):
                return err
            logging.info('SmtpHandler.handle_DATA_CHUNK %s resp %s',
                         self.cx_id, tx.data_response)
            tx.body.trim_front(tx.body.len())

            if tx.data_response is not None:
                data_resp = tx.data_response.to_smtp_resp()
                self.tx = None
                return data_resp
            if chunk_last:  # (and no data_response) -> bug
                logging.debug('%s no data_response', self.cx_id)
                # don't try to cancel after bug in upstream
                self.upstream = None
                return '450 service unavailable'
        return None

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
                    ident='ESMTP koukan smtp gateway',
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
