
import dkim

from response import Response, Esmtp
from blob import Blob

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

    def append_data(self, last : bool, blob : Blob):
        # TODO dkimpy wants to get the entire message as a single
        # bytes value, a better interface for this would be to push
        # chunks into it, I don't think that would be a huge change
        self.data += blob.contents()
        if not last:
            return Response()

        sig = dkim.sign(self.data, self.selector, self.domain, self.privkey,
                        include_headers=[b'From', b'Date', b'Message-ID'])

        # identity=None, canonicalize=('relaxed', 'simple'),
        # signature_algorithm='rsa-sha256', include_headers=None,
        # length=False, logger=None, linesep='\r\n', tlsrpt=False)

        resp = self.next.append_data(last=False, blob=InlineBlob(sig))
        if resp.err: return resp
        return self.next.append_data(last=True, blob=blob)


    def get_status(self) -> Response:
        return self.next.get_status()
