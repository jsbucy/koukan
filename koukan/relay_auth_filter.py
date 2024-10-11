# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional
from koukan.filter import (
    SyncFilter,
    TransactionMetadata )
from koukan.response import Response

# Filter that fails MAIL in the absence of a positive signal to
# authorize relaying.
class RelayAuthFilter(SyncFilter):
    upstream : SyncFilter
    smtp_auth : Optional[bool] = False

    def __init__(self, upstream : SyncFilter,
                 # allow relaying if smtp auth present
                 smtp_auth : Optional[bool] = False):
        self.upstream = upstream
        self.smtp_auth = smtp_auth

    def on_update(self, tx : TransactionMetadata,
               tx_delta : TransactionMetadata
               ) -> Optional[TransactionMetadata]:
        if tx_delta.mail_from is not None:
            if (not self.smtp_auth or
                tx.smtp_meta is None or
                not tx.smtp_meta.get('auth', False)):
                upstream_delta = TransactionMetadata(
                    mail_response = Response(550, '5.7.1 not authorized'))
                tx.merge_from(upstream_delta)
                return upstream_delta
        return self.upstream.on_update(tx, tx_delta)


    def abort(self):
        pass
