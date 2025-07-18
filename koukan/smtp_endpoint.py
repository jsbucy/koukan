# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Dict, List, Optional, Tuple
from types import ModuleType
import logging
import time
import ipaddress

from koukan.blob import Blob, BlobReader
from koukan.response import Response, Esmtp
from koukan.filter import (
    EsmtpParam,
    HostPort,
    SyncFilter,
    TransactionMetadata )

class Factory:
    def __init__(self, smtplib : ModuleType, ehlo_hostname, timeout, protocol,
                 enable_bdat, chunk_size):
        self.ehlo = ehlo_hostname
        self.timeout = timeout
        self.protocol = protocol
        self.smtplib = smtplib
        self.enable_bdat = enable_bdat
        self.chunk_size = chunk_size

    def new(self):
        return SmtpEndpoint(
            self.smtplib, self.ehlo, self.timeout, self.protocol,
            self.enable_bdat, self.chunk_size)

class SmtpEndpoint(SyncFilter):
    MAX_WITHOUT_SIZE = 8 * 1024 * 1024
    smtp : Optional[Any] = None
    good_rcpt : bool = False
    timeout : int = 30
    protocol : str
    any_rcpt = False
    body_reader : Optional[BlobReader] = None
    smtplib : ModuleType
    enable_bdat : bool
    chunk_size : int
    body : Optional[bytes] = None

    def __init__(self,
                 smtplib : ModuleType,
                 ehlo_hostname, timeout : Optional[int] = None,
                 protocol : str = 'smtp',
                 enable_bdat = False,
                 chunk_size = 2**16):
        # TODO this should come from the rest transaction -> start()
        self.ehlo_hostname = ehlo_hostname
        self.rcpt_resp = []
        if timeout is not None:
            self.timeout = timeout
        assert protocol in ['smtp', 'lmtp']
        self.protocol = protocol
        self.smtplib = smtplib
        self.enable_bdat = enable_bdat
        self.chunk_size = chunk_size

    def _shutdown(self):
        # SmtpEndpoint is a per-request object but we could return the
        # connection to a cache here if the previous transaction was
        # successful

        logging.info('SmtpEndpoint._shutdown')
        if self.smtp is None:
            return

        try:
            self.smtp.quit()
        except self.smtplib.SMTPException as e:
            logging.info('SmtpEndpoint._shutdown %s', e)
        self.smtp = None

    def _connect(self, tx : TransactionMetadata) -> Response:
        if tx.remote_host is None:
            return Response(
                400, 'SmtpEndpoint: bad request: no remote_host')

        try:
            ipaddress.ip_address(tx.remote_host.host)
        except ValueError:
            return Response(
                400, 'SmtpEndpoint: bad request: '
                'remote_host.host is not a valid IP address')

        if self.protocol == 'smtp':
            kwargs = {}
            if self.enable_bdat:
                kwargs['enable_bdat'] = True
            self.smtp = self.smtplib.SMTP(timeout=self.timeout, **kwargs)
        elif self.protocol == 'lmtp':
            self.smtp = self.smtplib.LMTP(timeout=self.timeout)
        else:
            raise ValueError()

        try:
            # TODO workaround bug in smtplib py<3.11
            # https://stackoverflow.com/questions/51768041/python3-smtp-valueerror-server-hostname-cannot-be-an-empty-string-or-start-with
            # passing the hostname to SMTP() swallows the greeting
            # on success :/
            self.smtp._host = tx.remote_host.host
            resp = Response.from_smtp(
                self.smtp.connect(tx.remote_host.host, tx.remote_host.port))
        except (self.smtplib.SMTPException, ConnectionError) as e:
            logging.info('SmtpEndpoint.connect %s %s', e, tx.remote_host)
            return Response(400, 'SmtpEndpoint: connect error')

        # TODO all of these smtplib.SMTP calls on self.smtp can throw
        # e.g. on tcp reset/server hung up
        # LMTP sends LHLO here
        resp = Response.from_smtp(self.smtp.ehlo(self.ehlo_hostname))
        if resp.err():
            self._shutdown()
            return resp

        # TODO save/log greeting, ehlo resp?

        if 'starttls' not in self.smtp.esmtp_features:
            return Response()

        # this returns the smtp response to the starttls command
        # and throws on tls negotiation failure?
        starttls_resp = Response.from_smtp(self.smtp.starttls())
        if starttls_resp.err():
            self._shutdown()
            return starttls_resp
        ehlo_resp = Response.from_smtp(self.smtp.ehlo(self.ehlo_hostname))
        if ehlo_resp.err():
            self._shutdown()

        return ehlo_resp

    def on_update(self, tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:
        if tx_delta.cancelled:
            self._shutdown()
            return TransactionMetadata()

        upstream_delta = self._update(tx, tx_delta)
        assert tx.merge_from(upstream_delta) is not None
        return upstream_delta

    def _check_esmtp(self, params : List[EsmtpParam]) -> Optional[Response]:
        for i,e in enumerate(params):
            if e.keyword.lower() == 'body':
                if not self.smtp.has_extn(e.value):
                    return Response(504, 'body=%s and not advertised' % e.value)
            elif e.keyword.lower() == 'size':
                req_size = int(e.value)
                if not self.smtp.has_extn('size'):
                    if req_size > SmtpEndpoint.MAX_WITHOUT_SIZE:
                        return Response(
                            504, 'size=%d, not advertised upstream ' % req_size)
                    del params[i]
                    continue
                server_size = int(self.smtp.esmtp_features['size'])
                if req_size > server_size:
                    return Response(
                        504, 'size=%d > upstream %d ' % (req_size, server_size))
            elif not self.smtp.has_extn(e.keyword):
                return Response(
                    504, 'smtp_endpoint: MAIL esmtp param not advertised '
                    'by peer: %s' % e.keyword)
        return None

    def _update(self, tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:

        upstream_delta = TransactionMetadata()

        if tx_delta.mail_from is not None:
            resp = self._connect(tx)
            if resp.err():
                upstream_delta.mail_response = resp
                self._shutdown()
                return
            assert self.smtp is not None
            if err := self._check_esmtp(tx_delta.mail_from.esmtp):
                upstream_delta.mail_response = err
            else:
                mailbox = tx_delta.mail_from.mailbox
                esmtp = [e.to_str() for e in tx_delta.mail_from.esmtp]
                logging.debug('SmtpEndpoint %s MAIL FROM %s %s',
                              tx.rest_id, mailbox, esmtp)
                upstream_delta.mail_response = Response.from_smtp(
                    self.smtp.mail(mailbox, esmtp))
                logging.debug('SmtpEndpoint %s mail resp %s',
                              tx.rest_id, upstream_delta.mail_response)
            if upstream_delta.mail_response.err():
                self._shutdown()
                return upstream_delta

        for rcpt in tx_delta.rcpt_to:
            # smtplib.LMTP doesn't support multi-rcpt transactions
            # https://github.com/python/cpython/issues/76984
            # as of this writing (2024/10) there is a PR in review to fix
            if self.protocol == 'lmtp' and self.any_rcpt:
                upstream_delta.rcpt_response.append(
                    Response(450, 'lmtp multi-rcpt unimplemented'))
                continue
            self.any_rcpt = True
            bad_ext = None

            if err := self._check_esmtp(rcpt.esmtp):
                upstream_delta.rcpt_response.append(err)
                continue
            else:
                esmtp = [e.to_str() for e in rcpt.esmtp]
                logging.debug('SmtpEndpoint %s RCPT TO %s %s',
                              tx.rest_id, rcpt.mailbox, esmtp)
                resp = Response.from_smtp(self.smtp.rcpt(rcpt.mailbox, esmtp))
            logging.debug('SmtpEndpoint %s rcpt resp %s', tx.rest_id, resp)
            if resp.ok():
                self.good_rcpt = True
            upstream_delta.rcpt_response.append(resp)

        if (tx_delta.body is not None) and not isinstance(tx_delta.body, Blob):
            upstream_delta.data_response = Response(
                500, 'BUG: message_builder in SmtpEndpoint')
        body = tx.maybe_body_blob()
        if not tx.data_response and (body is not None):
            logging.info('SmtpEndpoint %s append_data len=%d',
                         tx.rest_id, body.len())
            if not self.good_rcpt:
                upstream_delta.data_response = Response(
                    554, 'no valid recipients (SmtpEndpoint)')  # 5321/3.3
                return upstream_delta

            if self.body_reader is None:
                self.body_reader = BlobReader(tx.body)

            if not hasattr(self.smtp, 'data_chunk'):
                # low-performance/backward-compatibility path:
                # SyncFilterAdapter assumes that each on_update() call
                # consumes all the data from the blob and calls
                # blob.trim_front() after
                if self.body is None:
                    self.body = b''
                self.body += self.body_reader.read()
                if not tx.body.finalized():
                    return upstream_delta
                upstream_delta.data_response = Response.from_smtp(
                    self.smtp.data(self.body))
                self.body = None
                logging.info('SmtpEndpoint %s data_response %s',
                             tx.rest_id, upstream_delta.data_response)
                return upstream_delta

            chunk_last = False
            data_resp = None
            while not chunk_last and (data_resp is None or data_resp.ok()):
                chunk = self.body_reader.read(self.chunk_size)
                if tx.body.content_length() is not None:
                    chunk_last = (self.body_reader.tell() ==
                                  tx.body.content_length())
                if not chunk_last and not chunk:
                    break
                resp = self.smtp.data_chunk(chunk, chunk_last)
                if resp is not None:
                    data_resp = Response.from_smtp(resp)
            # BDAT will return a 250 for every chunk but our stack
            # generally assumes data_resp != None means it's done;
            # returning a !last 250 here will probably trigger an
            # early-return elsewhere.
            if chunk_last or (data_resp is not None and data_resp.err()):
                upstream_delta.data_response = data_resp
                self._shutdown()

            logging.info('SmtpEndpoint %s data_resp %s',
                         tx.rest_id, upstream_delta.data_response)

        return upstream_delta

    def abort(self):
        raise NotImplementedError()
