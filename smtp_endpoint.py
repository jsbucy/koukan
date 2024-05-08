from typing import Dict, List, Optional, Tuple
from smtplib import SMTP, SMTPException
import logging
import psutil
import time

from blob import Blob
from response import Response, Esmtp
from filter import Filter, HostPort, TransactionMetadata

class Factory:
    def __init__(self):
        pass

    def new(self, ehlo_hostname):
        return SmtpEndpoint(ehlo_hostname)

class SmtpEndpoint(Filter):
    smtp : Optional[SMTP] = None
    good_rcpt : bool = False
    version : int

    def __init__(self, ehlo_hostname):
        # TODO this should come from the rest transaction -> start()
        self.ehlo_hostname = ehlo_hostname
        self.rcpt_resp = []

        self.version = 0

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

    def on_update(self, tx : TransactionMetadata):
        self.version += 1
        logging.info('SmtpEndpoint.on_update %s', tx.remote_host)

        if tx.mail_from is not None:
            resp = self._connect(tx)
            if resp.err():
                tx.mail_response = resp
                self._shutdown()
                return
            assert self.smtp is not None
            for e in tx.mail_from.esmtp:
                if not self.smtp.has_extn(e.keyword):
                    return Response(
                        504, 'smtp_endpoint: MAIL esmtp param not advertised '
                        'by peer: %s' % e.keyword)

            tx.mail_response = Response.from_smtp(
                self.smtp.mail(tx.mail_from.mailbox,
                               [e.to_str() for e in tx.mail_from.esmtp]))
            logging.debug('SmtpClient mail_resp %s', tx.mail_response)
            if tx.mail_response.err():
                self._shutdown()
                return
        assert self.smtp is not None

        for rcpt in tx.rcpt_to:
            bad_ext = None
            for e in rcpt.esmtp:
                if not self.smtp.has_extn(e.keyword):
                    bad_ext = e
                    break
            if bad_ext is not None:
                tx.rcpt_response.append(Response(
                    504, 'smtp_endpoint: RCPT esmtp param not advertised '
                    'by peer: %s' % bad_ext.keyword))
                continue

            resp = Response.from_smtp(
                self.smtp.rcpt(rcpt.mailbox,
                               [e.to_str() for e in rcpt.esmtp]))
            if resp.ok():
                self.good_rcpt = True
            tx.rcpt_response.append(resp)

        if tx.body_blob is not None and (
                tx.body_blob.len() == tx.body_blob.content_length()):
            logging.info('SmtpEndpoint.append_data len=%d', tx.body_blob.len())
            if not self.good_rcpt:
                tx.data_response = Response(
                    554, 'no valid recipients (SmtpEndpoint)')  # 5321/3.3
            else:
                tx.data_response = Response.from_smtp(
                    self.smtp.data(tx.body_blob.read(0)))
            logging.info('SmtpEndpoint data_resp %s', tx.data_response)
            self._shutdown()

    def abort(self):
        raise NotImplementedError()
