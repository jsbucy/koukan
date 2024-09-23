from typing import Any, Optional, Tuple
import copy
import logging

import dkim

from koukan.response import Response, Esmtp
from koukan.blob import Blob, InlineBlob, CompositeBlob
from koukan.filter import (
    SyncFilter,
    TransactionMetadata )

class DkimEndpoint(SyncFilter):
    data : bytes = None
    upstream : SyncFilter
    body_blob : Optional[Blob] = None
    err = False

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
        built = False
        if (self.body_blob is None and
            not self.err and
            tx.body_blob is not None
            and tx.body_blob.finalized()):
            self.body_blob = CompositeBlob()
            sig = self.sign(tx.body_blob)
            if sig is None:
                self.err = True
                err = TransactionMetadata(data_response=Response(
                    500, 'signing failed (DkimEndpoint'))
                tx.merge_from(err)
                return err
            sig_blob = InlineBlob(sig)
            self.body_blob.append(sig_blob, 0, sig_blob.len())
            self.body_blob.append(tx.body_blob, 0, tx.body_blob.len(), True)
            built = True

        downstream_tx = tx.copy()
        downstream_delta = tx_delta.copy()
        downstream_tx.body_blob = self.body_blob
        downstream_delta.body_blob = self.body_blob if built else None

        if bool(downstream_delta):
            upstream_delta = self.upstream.on_update(
                downstream_tx, downstream_delta)
        else:
            upstream_delta = TransactionMetadata()
        assert tx.merge_from(upstream_delta) is not None
        return upstream_delta

    def sign(self, blob : Blob) -> Optional[bytes]:
        data = blob.read(0)
        # TODO dkimpy wants to get the entire message as a single
        # bytes value, a better interface for this would be to push
        # chunks into it, I don't think that would be a huge change

        # identity=None, canonicalize=('relaxed', 'simple'),
        # signature_algorithm='rsa-sha256', include_headers=None,
        # length=False, logger=None, linesep='\r\n', tlsrpt=False)

        # TODO need to handle IDN here?

        try:
            return dkim.sign(data, self.selector.encode('us-ascii'),
                             self.domain.encode('us-ascii'),
                             self.privkey,
                             include_headers=[b'From', b'Date', b'Message-ID'])
        except dkim.DKIMException as e:
            logging.info('failed to sign %s', e)
            return None