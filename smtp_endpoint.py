
from typing import Dict, List, Optional, Tuple

import smtplib

from blob import Blob

from response import Response, Esmtp

import logging

import psutil


class Factory:
    def __init__(self):
        # only the router sends to this
        proc_self = psutil.Process()
        self.rest_id_base = '%s.%s.' % (
            proc_self.pid, int(proc_self.create_time()))
        self.rest_id_next = 0

    def new(self, ehlo_hostname):
        rest_id = self.rest_id_base + str(self.rest_id_next)
        self.rest_id_next += 1
        return SmtpEndpoint(rest_id, ehlo_hostname)

# rest or smtp services call upon a single instance of this
class SmtpEndpoint:
    smtp : smtplib.SMTP
    data : bytes = None
    start_resp : Response = None
    final_status : Response = None

    def __init__(self, rest_id, ehlo_hostname):
        self.rest_id = rest_id
        # TODO this should come from the rest transaction -> start()
        self.ehlo_hostname = ehlo_hostname

    def generate_rest_id(self):
        return self.rest_id


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
            print('SmtpEndpoint.connect', e, host, port)
            return [400, b'connect error']


    def start(self,
              local_host, remote_host,
              mail_from, transaction_esmtp,
              rcpt_to, rcpt_esmtp=None):
        logging.info('SmtpEndpoint.start %s', remote_host)
        resp = Response.from_smtp(
            self.connect(host=remote_host[0], port=remote_host[1]))
        if resp.err(): return resp

        resp = Response.from_smtp(self.smtp.ehlo(self.ehlo_hostname))
        if resp.err(): return resp

        if 'starttls' in self.smtp.esmtp_features:
            # this returns the smtp response to the starttls command
            # and throws on tls negotiation failure?
            starttls_resp = Response.from_smtp(self.smtp.starttls())
            if starttls_resp.err(): return starttls_resp
            ehlo_resp = Response.from_smtp(self.smtp.ehlo(self.ehlo_hostname))
            if ehlo_resp.err(): return ehlo_resp

        resp = Response.from_smtp(self.smtp.mail(mail_from))
        if resp.err(): return resp

        self.data = bytes()

        return Response.from_smtp(self.smtp.rcpt(rcpt_to))

    def append_data(self, last : bool, blob : Blob):
        print('SmtpEndpoint.append_data last=', last, "len=", blob.len())
        self.data += blob.contents()
        if not last:
            return None

        self.final_status = Response.from_smtp(self.smtp.data(self.data))
        print(self.final_status)
        return self.final_status

    def get_start_result(self, timeout=None):
        return None
    def get_final_status(self, timeout=None):
        return None  # XXX this is always sync
