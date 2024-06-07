from typing import Dict, List, Optional, Tuple
from smtplib import SMTP, SMTPException
import logging
import psutil
import time

from blob import Blob
from response import Response, Esmtp
from filter import (
    HostPort,
    SyncFilter,
    TransactionMetadata )

class Factory:
    def __init__(self):
        pass

    def new(self, ehlo_hostname):
        return SmtpEndpoint(ehlo_hostname)

class SmtpEndpoint(SyncFilter):
    smtp : Optional[SMTP] = None
    good_rcpt : bool = False

    def __init__(self, ehlo_hostname):
        # TODO this should come from the rest transaction -> start()
        self.ehlo_hostname = ehlo_hostname
        self.rcpt_resp = []

    def _shutdown(self):
        # SmtpEndpoint is a per-request object but we could return the
        # connection to a cache here if the previous transaction was
        # successful

        logging.info('SmtpEndpoint._shutdown')
        if self.smtp is None: return

        try:
            self.smtp.quit()
        except SMTPException as e:
            logging.info('SmtpEndpoint._shutdown %s', e)
        self.smtp = None

    def _connect(self, tx : TransactionMetadata) -> Response:
        if tx.remote_host is None:
            return Response(
                400, 'SmtpEndpoint: bad request: no remote_host')

        self.smtp = SMTP()
        try:
            # TODO workaround bug in smtplib py<3.11
            # https://stackoverflow.com/questions/51768041/python3-smtp-valueerror-server-hostname-cannot-be-an-empty-string-or-start-with
            # passing the hostname to SMTP() swallows the greeting
            # on success :/
            self.smtp._host = tx.remote_host.host
            resp = Response.from_smtp(
                self.smtp.connect(tx.remote_host.host, tx.remote_host.port))
        except SMTPException as e:
            logging.info('SmtpEndpoint.connect %s %s', e, tx.remote_host)
            return Response(400, 'SmtpEndpoint: connect error')

        # TODO all of these smtplib.SMTP calls on self.smtp can throw
        # e.g. on tcp reset/server hung up
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
        logging.info('SmtpEndpoint.on_update %s', tx.remote_host)

        upstream_delta = self._update(tx, tx_delta)
        assert tx.merge_from(upstream_delta) is not None
        return upstream_delta

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
            for e in tx_delta.mail_from.esmtp:
                if not self.smtp.has_extn(e.keyword):
                    upstream_delta.mail_response = Response(
                        504, 'smtp_endpoint: MAIL esmtp param not advertised '
                        'by peer: %s' % e.keyword)
                    break
            else:
                upstream_delta.mail_response = Response.from_smtp(
                    self.smtp.mail(
                        tx_delta.mail_from.mailbox,
                        [e.to_str() for e in tx_delta.mail_from.esmtp]))
            logging.debug('SmtpClient mail_resp %s',
                          upstream_delta.mail_response)
            if upstream_delta.mail_response.err():
                self._shutdown()
                return upstream_delta
        assert self.smtp is not None

        for rcpt in tx_delta.rcpt_to:
            bad_ext = None
            for e in rcpt.esmtp:
                if not self.smtp.has_extn(e.keyword):
                    bad_ext = e
                    break
            if bad_ext is not None:
                upstream_delta.rcpt_response.append(Response(
                    504, 'smtp_endpoint: RCPT esmtp param not advertised '
                    'by peer: %s' % bad_ext.keyword))
                continue

            resp = Response.from_smtp(
                self.smtp.rcpt(rcpt.mailbox,
                               [e.to_str() for e in rcpt.esmtp]))
            if resp.ok():
                self.good_rcpt = True
            upstream_delta.rcpt_response.append(resp)

        if tx_delta.body_blob is not None and (
                tx_delta.body_blob.len() ==
                tx_delta.body_blob.content_length()):
            logging.info('SmtpEndpoint.append_data len=%d',
                         tx_delta.body_blob.len())
            if not self.good_rcpt:
                upstream_delta.data_response = Response(
                    554, 'no valid recipients (SmtpEndpoint)')  # 5321/3.3
            else:
                upstream_delta.data_response = Response.from_smtp(
                    self.smtp.data(tx_delta.body_blob.read(0)))
            logging.info('SmtpEndpoint data_resp %s',
                         upstream_delta.data_response)

            self._shutdown()

        return upstream_delta

    def abort(self):
        raise NotImplementedError()
