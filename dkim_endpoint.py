from typing import Any, Optional, Tuple
import copy
import logging

import dkim

from response import Response, Esmtp
from blob import Blob, InlineBlob, CompositeBlob
from filter import (
    SyncFilter,
    TransactionMetadata )

class DkimEndpoint(SyncFilter):
    data : bytes = None
    upstream_tx : Optional[TransactionMetadata] = None
    upstream : SyncFilter

    def __init__(self, domain : str, selector : str, privkey,
                 upstream : SyncFilter):
        self.domain = domain
        self.selector = selector
        with open(privkey, "rb") as f:
            self.privkey = f.read()
        self.upstream = upstream

    def on_update(self, tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:
        if self.upstream_tx is None:
            self.upstream_tx = tx.copy()
        else:
            assert self.upstream_tx.merge_from(tx_delta) is not None

        downstream_delta = tx_delta.copy()

        body_blob = tx_delta.body_blob
        if body_blob is not None and (
                body_blob.len() == body_blob.content_length()):
            upstream_body = CompositeBlob()
            sig = InlineBlob(self.sign(body_blob))
            upstream_body.append(sig, 0, sig.len())
            upstream_body.append(tx.body_blob, 0, body_blob.len(), True)
            body_blob = upstream_body
        else:
            body_blob = None
        self.upstream_tx.body_blob = downstream_delta.body_blob = body_blob

        if not(downstream_delta):
            return TransactionMetadata()

        upstream_delta = self.upstream.on_update(
            self.upstream_tx, downstream_delta)
        assert tx.merge_from(upstream_delta) is not None
        return upstream_delta

    def sign(self, blob : Blob):
        data = blob.read(0)
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


