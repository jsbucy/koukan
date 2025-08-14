# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Optional, Tuple
import copy
import logging

import dkim

from koukan.response import Response, Esmtp
from koukan.blob import Blob, InlineBlob, CompositeBlob
from koukan.filter import (
    TransactionMetadata )
from koukan.filter_chain import FilterResult, ProxyFilter

class DkimEndpoint(ProxyFilter):
    domain : str
    selector :str

    def __init__(self, domain : str, selector : str, privkey):
        self.domain = domain
        self.selector = selector
        with open(privkey, "rb") as f:
            self.privkey = f.read()

    def on_update(self, tx_delta : TransactionMetadata) -> FilterResult:
        body = tx_delta.maybe_body_blob()
        if body is not None:
            tx_delta.body = None
        if body is not None and not body.finalized():
            body = None
        self.upstream.merge_from(tx_delta)

        if body is None:
            return FilterResult()

        assert self.upstream.body is None

        sig = self.sign(body)
        if sig is not None:
            self.upstream.body = CompositeBlob()
            sig_blob = InlineBlob(sig)
            self.upstream.body.append(sig_blob, 0, sig_blob.len())
            self.upstream.body.append(body, 0, body.len(), True)
        delta = None
        if sig is None:
            delta = TransactionMetadata(
                data_response = Response(500, 'signing failed (DkimEndpoint'))
        return FilterResult(delta)

    def sign(self, blob : Blob) -> Optional[bytes]:
        data = blob.pread(0)
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
