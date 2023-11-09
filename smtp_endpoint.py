
from typing import Dict, List, Optional, Tuple

import smtplib

from blob import Blob

from response import Response, Esmtp

import logging

import psutil

import time

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
    smtp : smtplib.SMTP
    data : bytes = None
    start_resp : Response = None
    final_status : Response = None
    received_last = False  # xxx rest service endpoint abc
    blobs_received = -1
    rest_id = None
    idle_start = None

    def __init__(self, rest_id, ehlo_hostname):
        self.rest_id = rest_id
        # TODO this should come from the rest transaction -> start()
        self.ehlo_hostname = ehlo_hostname

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


    def start(self,
              local_host, remote_host,
              mail_from, transaction_esmtp,
              rcpt_to, rcpt_esmtp=None):
        self.idle_start = time.monotonic()
        logging.info('SmtpEndpoint.start %s %s', self.rest_id, remote_host)
        resp = Response.from_smtp(
            self.connect(host=remote_host[0], port=remote_host[1]))
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

        resp = Response.from_smtp(self.smtp.mail(mail_from))
        if resp.err():
            self._shutdown()
            return resp

        self.data = bytes()

        self.start_resp = Response.from_smtp(self.smtp.rcpt(rcpt_to))
        if self.start_resp.err():
            self._shutdown()
        return self.start_resp

    def append_data(self, last : bool, blob : Blob):
        self.idle_start = time.monotonic()
        logging.info('SmtpEndpoint.append_data last=%s len=%d',
                     last, blob.len())
        self.data += blob.contents()
        self.blobs_received += 1
        if not last:
            return None
        else:
            self.received_last = True

        self.final_status = Response.from_smtp(self.smtp.data(self.data))
        logging.info(self.final_status)
        self._shutdown()
        return self.final_status

    def get_start_result(self, timeout=None):
        return self.start_resp
    def get_final_status(self, timeout=None):
        return self.final_status
