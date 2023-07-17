
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
    data : bytes = None
    current_chunk : bytes = None
    ok_rcpt = False

    def __init__(self, host, port, resolve_mx=False):
        self.host = host
        self.port = port
        self.resolve_mx = resolve_mx
        self.smtp = smtplib.SMTP(host, port)

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
        print('SmtpEndpoint.add_rcpt ', resp)
        if resp.ok():
            self.ok_rcpt = True
        return resp

    # -> resp
    def append_data(self, last : bool, chunk_id : int, d : bytes = None):
        print('SmtpEndpoint.append_data last=', last,
              "chunk_id=", chunk_id, self.chunk_id)

        if not self.ok_rcpt:
            return Response(500, 'smtp_endpoint no rcpt')

        if chunk_id == self.chunk_id:  # noop
            return Response()
        if self.chunk_id is None and chunk_id == 0:
            self.data = bytes()
            self.chunk_id = 0
        elif chunk_id != self.chunk_id + 1:
            return Response(500, 'smtp_endpoint: bad chunk id')

        self.current_chunk = bytes()
        self.chunk_id = chunk_id
        self.last_chunk = last
        if d is not None:
            resp, chunk_len = self.append_data_chunk(
                self.chunk_id, offset=0, d=d, last=True)
            # this condition -> smtp connection timeout
            assert(chunk_len == len(d))
        return Response()

    # -> (resp, len)
    def append_data_chunk(self, chunk_id, offset, d : bytes, last : bool):
        print('SmtpEndpoint.append_data_chunk', chunk_id, offset, len(d), last)
        assert(self.chunk_id == chunk_id)
        if offset > len(self.current_chunk):
            print('hole', offset, len(self.data))
            return Response(500, 'hole'), len(self.data)
        self.current_chunk += d[offset - len(self.current_chunk):]
        current_chunk_len = len(self.current_chunk)
        if last:
            self.data += self.current_chunk
            self.current_chunk = None
        if last and self.last_chunk:
            self.final_status = Response.from_smtp(self.smtp.data(self.data))
            print(self.final_status)
        return Response(), current_chunk_len

    def get_transaction_status(self):
        print('SmtpEndpoint.get_transaction_status')
        return self.final_status

