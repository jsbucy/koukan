
import dkim

from response import Response, Esmtp

from typing import Optional, Tuple

class Endpoint:
    chunk_id = None

    def __init__(self, domain, selector, privkey, next):
        f = open(privkey, "rb")
        self.domain = domain
        self.selector = selector
        self.privkey = f.read()
        self.next = next

    def on_connect(self, remote_host, local_host) -> Response:
        return self.next.on_connect(remote_host, local_host)

    def on_ehlo(self, hostname) -> Tuple[Response, Optional[Esmtp]]:
        return self.next.on_ehlo(hostname)

    # -> (resp, rcpt_status)
    def start_transaction(self, reverse_path, esmtp_options=None,
                          forward_path = None):
        self.ok_resp = False
        resp, rcpt_status = self.next.start_transaction(
            reverse_path, esmtp_options, forward_path)
        for r in rcpt_status:
            if r.ok():
                self.ok_resp = True
        return resp, rcpt_status

    def add_rcpt(self, forward_path, esmtp_options=None) -> Response:
        resp = self.next.add_rcpt(forward_path, esmtp_options)
        if resp.ok():
            self.ok_rcpt = True
        return resp

    def append_data(self, last : bool, chunk_id : int, d : bytes = None) -> Response:
        if not self.ok_rcpt:
            return Response(500, 'smtp_endpoint no rcpt')

        if chunk_id == self.chunk_id:  # noop
            return Response()
        if self.chunk_id is None and chunk_id == 0:
            self.data = bytes()
            self.chunk_id = 0
        elif chunk_id != self.chunk_id + 1:
            return Response(500, 'DkimEndpoint.append_data: bad chunk id')

        self.current_chunk = bytes()
        self.chunk_id = chunk_id
        self.last_chunk = last
        if d is not None:
            self.data += d
        return Response()

    def append_data_chunk(self, chunk_id : int, offset : int,
                          d : bytes, last : bool) -> Tuple[Response, int]:

        assert(self.chunk_id == chunk_id)
        if offset > len(self.current_chunk):
            print('hole', offset, len(self.data))
            return (Response(500, 'DkimEndpoint.append_data_chunk: hole'),
                    len(self.data))
        self.current_chunk += d[offset - len(self.current_chunk):]
        current_chunk_len = len(self.current_chunk)
        if last:
            self.data += self.current_chunk
            self.current_chunk = None
        if last and self.last_chunk:
            sig = dkim.sign(self.data, self.selector, self.domain, self.privkey,
                            include_headers=['From', 'Date', 'Message-ID'])

            # identity=None, canonicalize=('relaxed', 'simple'),
            # signature_algorithm='rsa-sha256', include_headers=None,
            # length=False, logger=None, linesep='\r\n', tlsrpt=False)

            resp = self.next.append_data(chunk_id=0, d=sig, last=False)
            if resp.err(): return resp
            resp = self.next.append_data(chunk_id=1, last=True)
            if resp.err(): return resp
            data_len = 0
            while data_len < len(self.data):
                resp, data_len = self.next.append_data_chunk(
                    chunk_id=1, offset=0, d=self.data[data_len:], last=True)
                if resp.err():
                    return resp
            return Response()


    def get_transaction_status(self) -> Response:
        return self.next.get_transaction_status()
