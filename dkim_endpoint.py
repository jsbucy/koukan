from typing import Any, Optional, Tuple
import copy
import logging

import dkim

from response import Response, Esmtp
from blob import Blob, InlineBlob, CompositeBlob
from filter import Filter, TransactionMetadata

class DkimEndpoint(Filter):
    data : bytes = None

    def __init__(self, domain : str, selector : str, privkey, next : Filter):
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
            return self.next.on_update(tx)

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

        # TODO need to handle IDN here?
        sig = dkim.sign(data, self.selector.encode('us-ascii'),
                        self.domain.encode('us-ascii'),
                        self.privkey,
                        include_headers=[b'From', b'Date', b'Message-ID'])
        return sig


    def abort(self):
        if self.next: self.next.abort()
