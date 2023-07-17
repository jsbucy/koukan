
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import smtplib

import mx_resolution

from response import Response, Esmtp

# rest or smtp services call upon a single instance of this
class SmtpEndpoint:
    resolve_mx = False
    smtp : smtplib.SMTP
    chunk_id = None
    data = None

    def __init__(self, host, port, resolve_mx=False):
        self.host = host
        self.port = port
        self.resolve_mx = resolve_mx
        self.smtp = smtplib.SMTP(host)

    def on_connect(self, remote_host, local_host) -> Response:
        # go down the list until we get a good banner
        # one might want to keep retrying until you get past
        # ehlo+starttls+ehlo but that requires endpoint api changes
        # or move mx fallback into a separate endpoint that is more
        # explicitly a "router"
        if self.resolve_mx:
            for host in mx_resolution.resolve(self.host):
                print(host)
                resp = Response.from_smtp(
                    self.smtp.connect(host=self.host, port=self.port))
                if resp.ok():
                    return resp
        else:
            return Response.from_smtp(
                self.smtp.connect(host=self.host, port=self.port))


    def on_ehlo(self, hostname) -> Tuple[Response, Optional[Esmtp]]:
        ehlo_resp = Response.from_smtp(self.smtp.ehlo(hostname))
        if ehlo_resp.err(): return ehlo_resp
        # smtp.esmtp_features ~
        # {'size': '33554432', '8bitmime': '', 'smtputf8': ''}

        if 'starttls' not in self.smtp.esmtp_features:
            return ehlo_resp, self.smtp.esmtp_features

        # this returns the smtp response to the starttls command
        # and throws on tls negotiation failure?
        starttls_resp = Response.from_smtp(self.smtp.starttls())
        if starttls_resp.err(): return starttls_resp

        ehlo_resp = Response.from_smtp(self.smtp.ehlo(hostname))
        if ehlo_resp.err(): return ehlo_resp
        return ehlo_resp, self.smtp.esmtp_features

    # -> (resp, rcpt_status)
    def start_transaction(self, reverse_path, esmtp_options=None,
                          forward_path = None):
        resp = Response.from_smtp(self.smtp.mail(reverse_path))
        if resp.err(): return resp, None
        rcpt_status = None
        if forward_path:
            rcpt_status = []
            for (rcpt,esmtp) in forward_path:
                rcpt_status.append(self.add_rcpt(rcpt, esmtp))
        return resp, rcpt_status

    # -> resp
    def add_rcpt(self, forward_path, esmtp_options=None):
        print('SmtpEndpoint.add_rcpt ', forward_path)
        resp = Response.from_smtp(self.smtp.rcpt(forward_path))
        if resp.ok():
            self.chunk_id = 0
            self.data = bytes()
        return resp

    # -> (resp, chunk_id)
    def append_data(self, last : bool, chunk_id = None):
        print('SmtpEndpoint.append_data', last, chunk_id)
        self.chunk_id += 1
        self.last_chunk = last
        return Response(), str(self.chunk_id)

    # -> (resp, len)
    def append_data_chunk(self, chunk_id, offset,
                          d : bytes, last : bool):
        print('SmtpEndpoint.append_data_chunk', chunk_id, offset, len(d), last)
        assert(str(self.chunk_id) == chunk_id)
        if offset > len(self.data):
            print('hole', offset, len(self.data))
            return Response(500, 'hole'), len(self.data)
        self.data += d[offset - len(self.data):]
        if last and self.last_chunk:
            self.final_status = Response.from_smtp(self.smtp.data(self.data))
            print(self.final_status)
        return Response(), len(self.data)

    def get_transaction_status(self):
        print('SmtpEndpoint.get_transaction_status')
        return self.final_status

