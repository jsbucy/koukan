from typing import Any, Optional, Tuple
import copy
import logging

import dkim

from response import Response, Esmtp
from blob import Blob, InlineBlob, CompositeBlob
from filter import Filter, TransactionMetadata

class DkimEndpoint(Filter):
    data : bytes = None

    def __init__(self, domain, selector, privkey, next : Filter):
        f = open(privkey, "rb")
        self.domain = domain
        self.selector = selector
        self.privkey = f.read()
        self.next = next
        self.blobs = []

    def on_update(self, tx : TransactionMetadata):
        self.ok_resp = False
        if tx.body_blob is None or (
                tx.body_blob.len() != tx.body_blob.content_length()):
            return self.next_on_update(tx)

        upstream_tx = copy.copy(tx)
        self.blobs = [tx.body_blob]
        upstream_body = CompositeBlob()
        sig = InlineBlob(self.sign())
        upstream_body.append(sig, 0, sig.len())
        upstream_body.append(tx.body_blob, 0, tx.body_blob.len(), True)
        upstream_tx.body_blob = upstream_body

        self.next.on_update(upstream_tx)
        tx.mail_response = upstream_tx.mail_response
        tx.rcpt_response = upstream_tx.rcpt_response
        tx.data_response = upstream_tx.data_response

    def sign(self):
        data = b''
        for blob in self.blobs:
            data += blob.read(0)
        # TODO dkimpy wants to get the entire message as a single
        # bytes value, a better interface for this would be to push
        # chunks into it, I don't think that would be a huge change

        # identity=None, canonicalize=('relaxed', 'simple'),
        # signature_algorithm='rsa-sha256', include_headers=None,
        # length=False, logger=None, linesep='\r\n', tlsrpt=False)

        sig = dkim.sign(data, self.selector, self.domain, self.privkey,
                        include_headers=[b'From', b'Date', b'Message-ID'])
        return sig

    def append_data(self, last : bool, blob : Blob) -> Response:
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
