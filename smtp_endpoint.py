
from typing import Dict, List, Optional, Tuple

import smtplib

from response import Response, Esmtp

# rest or smtp services call upon a single instance of this
class SmtpEndpoint:
    smtp : smtplib.SMTP
    data : bytes = None
    final_status : Response

    def __init__(self):
        pass

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
        resp = Response.from_smtp(
            self.connect(host=remote_host[0], port=remote_host[1]))
        if resp.err(): return resp

        resp = Response.from_smtp(self.smtp.ehlo('fixme.ehlo.hostname'))
        if resp.err(): return resp

        if 'starttls' in self.smtp.esmtp_features:
            # this returns the smtp response to the starttls command
            # and throws on tls negotiation failure?
            starttls_resp = Response.from_smtp(self.smtp.starttls())
            if starttls_resp.err(): return starttls_resp
            ehlo_resp = Response.from_smtp(self.smtp.ehlo(hostname))
            if ehlo_resp.err(): return ehlo_resp

        resp = Response.from_smtp(self.smtp.mail(mail_from))
        if resp.err(): return resp

        self.data = bytes()

        return Response.from_smtp(self.smtp.rcpt(rcpt_to))


    def append_data(self, last : bool, d : bytes = None, blob_id=None):
        print('SmtpEndpoint.append_data last=',
              last, "d=", d is None, "blob_id=", blob_id is None)
        self.data += d
        if not last:
            return Response(), None

        self.final_status = Response.from_smtp(self.smtp.data(self.data))
        print(self.final_status)
        return Response(), None

    def get_status(self):
        print('SmtpEndpoint.get_transaction_status')
        return self.final_status

