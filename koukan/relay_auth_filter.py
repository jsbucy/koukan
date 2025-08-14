# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional
from koukan.filter import TransactionMetadata
from koukan.filter_chain import FilterResult, Filter
from koukan.response import Response

# Filter that fails transaction in the absence of a positive signal to
# authorize relaying.
class RelayAuthFilter(Filter):
    smtp_auth : Optional[bool] = False

    def __init__(self,
                 # allow relaying if smtp auth present
                 smtp_auth : Optional[bool] = False):
        self.smtp_auth = smtp_auth

    def on_update(self, tx_delta : TransactionMetadata) -> FilterResult:
        tx = self.downstream
        if tx_delta.mail_from is not None:
            assert tx.mail_response is None
            if (not self.smtp_auth or
                tx.smtp_meta is None or
                not tx.smtp_meta.get('auth', False)):
                tx.mail_response = Response(550, '5.7.1 not authorized')

        return FilterResult()
