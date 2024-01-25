from typing import Dict, List, Optional, Tuple
import smtplib
import logging
import psutil
import time

from blob import Blob
from response import Response, Esmtp
from filter import TransactionMetadata

factory_rest_id_next = 0

class Factory:
    def __init__(self):
        # only the router sends to this
        proc_self = psutil.Process()
        self.rest_id_base = '%s.%s.' % (
            proc_self.pid, int(proc_self.create_time()))

    def new(self, ehlo_hostname):
        global factory_rest_id_next
        rest_id = self.rest_id_base + str(factory_rest_id_next)
        factory_rest_id_next += 1
        return SmtpEndpoint(rest_id, ehlo_hostname)

# rest or smtp services call upon a single instance of this
class SmtpEndpoint:
    smtp : Optional[smtplib.SMTP] = None
    data : Optional[bytes] = None
    mail_resp : Optional[Response] = None
    rcpt_resp : List[Response]
    data_response : Optional[Response] = None
    received_last: bool  = False  # xxx rest service endpoint abc
    blobs_received : int = -1
    rest_id : Optional[str] = None
    idle_start : Optional[float] = None

    def __init__(self, rest_id, ehlo_hostname):
        self.rest_id = rest_id
        # TODO this should come from the rest transaction -> start()
        self.ehlo_hostname = ehlo_hostname
        self.rcpt_resp = []

        self.creation = int(time.time() * 1000000)
        self.version = 0

    def etag(self):
        # xxx hash
        return '%d.%d' % (self.creation, self.version)

    def _shutdown(self):
        # SmtpEndpoint is a per-request object but we could return the
        # connection to a cache here if the previous transaction was
        # successful

        logging.info('SmtpEndpoint._shutdown %s', self.rest_id)
        if self.smtp is None: return

        try:
            self.smtp.quit()
        except Exception as e:
            logging.info('SmtpEndpoint._shutdown %s %s', self.rest_id, e)
        self.smtp = None

    def connect(self, host, port):
        self.smtp = smtplib.SMTP()
        try:
            # TODO workaround bug in smtplib py<3.11
            # https://stackoverflow.com/questions/51768041/python3-smtp-valueerror-server-hostname-cannot-be-an-empty-string-or-start-with
            # passing the hostname to SMTP() swallows the greeting
            # on success :/
            self.smtp._host = host
            return self.smtp.connect(host, port)
        except Exception as e:
            logging.info('SmtpEndpoint.connect %s %s %d', e, host, port)
            return [400, b'connect error']


    def start(self, tx : TransactionMetadata):
        self.version += 1
        self.idle_start = time.monotonic()
        logging.info('SmtpEndpoint.start %s %s', self.rest_id, tx.remote_host)
        resp = Response.from_smtp(
            self.connect(host=tx.remote_host.host, port=tx.remote_host.port))
        if resp.err():
            self._shutdown()
            return resp

        resp = Response.from_smtp(self.smtp.ehlo(self.ehlo_hostname))
        if resp.err():
            self._shutdown()
            return resp

        if 'starttls' in self.smtp.esmtp_features:
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

        # TODO buffer partial here
        self.mail_resp = Response.from_smtp(self.smtp.mail(tx.mail_from.mailbox))
        if self.mail_resp.err():
            self._shutdown()
            return self.mail_resp

        self.data = bytes()

        good_rcpt = False
        for rcpt in tx.rcpt_to:
            resp = Response.from_smtp(self.smtp.rcpt(rcpt.mailbox))
            if resp.ok():
                good_rcpt = True
            self.rcpt_resp.append(resp)
        if not good_rcpt:
            self._shutdown()
        return self.rcpt_resp

    def append_data(self, last : bool, blob : Blob):
        self.version += 1
        self.idle_start = time.monotonic()
        logging.info('SmtpEndpoint.append_data last=%s len=%d',
                     last, blob.len())
        self.data += blob.contents()
        self.blobs_received += 1
        if not last:
            return None
        else:
            self.received_last = True

        self.data_response = Response.from_smtp(self.smtp.data(self.data))
        logging.info(self.data_response)
        self._shutdown()
        return self.data_response
