from typing import Any, Optional, Tuple

import dkim

from response import Response, Esmtp
from blob import Blob, InlineBlob
from filter import Filter

class DkimEndpoint(Filter):
    data : bytes = None

    def __init__(self, domain, selector, privkey, next : Filter):
        f = open(privkey, "rb")
        self.domain = domain
        self.selector = selector
        self.privkey = f.read()
        self.next = next
        self.blobs = []

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

    def sign(self):
        data = b''
        for blob in self.blobs:
            data += blob.contents()
        # TODO dkimpy wants to get the entire message as a single
        # bytes value, a better interface for this would be to push
        # chunks into it, I don't think that would be a huge change

        # identity=None, canonicalize=('relaxed', 'simple'),
        # signature_algorithm='rsa-sha256', include_headers=None,
        # length=False, logger=None, linesep='\r\n', tlsrpt=False)

        return dkim.sign(data, self.selector, self.domain, self.privkey,
                        include_headers=[b'From', b'Date', b'Message-ID'])
        

    def append_data(self, last : bool, blob : Blob):
        self.blobs.append(blob)
        if not last:
            return Response()
        sig = self.sign()

        blobs = [ InlineBlob(sig) ] + self.blobs
        for i,blob in enumerate(blobs):
            last = (i == len(blobs)-1)
            resp = self.next.append_data(last=last, blob=blob)
            if resp.err() or last: return resp

    def abort(self):
        if self.next: self.next.abort()
