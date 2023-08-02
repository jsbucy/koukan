
import dkim

from response import Response, Esmtp

from typing import Any, Optional, Tuple

class DkimEndpoint:
    data : bytes = None

    def __init__(self, domain, selector, privkey, next):
        f = open(privkey, "rb")
        self.domain = domain
        self.selector = selector
        self.privkey = f.read()
        self.next = next

    def start(self,
              local_host, remote_host,
              mail_from, transaction_esmtp,
              rcpt_to, rcpt_esmtp):
        self.ok_resp = False
        resp = self.next.start(
            local_host, remote_host,
            mail_from, transaction_esmtp,
            rcpt_to, rcpt_esmtp)
        if resp.ok():
            self.data = bytes()
        return resp

    def append_data(self, last : bool,
                    d : Optional[bytes] = None,
                    blob_id : Optional[Any] = None):
        # TODO it would be pretty easy for this to accept blob_ids
        # though dkimpy wants to get the entire message as a single
        # bytes value, you would have to bring the whole thing into
        # memory here but at least only for the (very short) duration
        # of the signing operation and could propagate them on out the
        # back
        self.data += d
        if not last:
            return Response()

        sig = dkim.sign(self.data, self.selector, self.domain, self.privkey,
                        include_headers=[b'From', b'Date', b'Message-ID'])

        # identity=None, canonicalize=('relaxed', 'simple'),
        # signature_algorithm='rsa-sha256', include_headers=None,
        # length=False, logger=None, linesep='\r\n', tlsrpt=False)

        return self.next.append_data(d=(sig + self.data), last=True)


    def get_status(self) -> Response:
        return self.next.get_status()
