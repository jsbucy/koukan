# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional
import logging

from koukan.filter import TransactionMetadata
from koukan.filter_chain import Filter, FilterResult
from koukan.response import Response

from koukan.remote_host_filter import RemoteHostFilter, RemoteHostFilterResult
from koukan.message_validation_filter import (
    MessageValidationFilter, MessageValidationFilterResult )

# this should go last before exploder in the downstream chain
# need one for upstream chain?
class IngressPolicy(Filter):
    max_received_headers : int
    min_validity : MessageValidationFilterResult.Status = MessageValidationFilterResult.Status.MEDIUM

    def __init__(self, max_received_headers : int = 30,
                 min_validity : Optional[str] = None):
        self.max_received_headers = max_received_headers
        if min_validity:
            self.min_validity = MessageValidationFilterResult.Status[min_validity]

    def on_update(self, tx_delta : TransactionMetadata):
        tx = self.downstream_tx
        assert tx is not None
        out = tx.filter_output
        assert out is not None

        if tx_delta.mail_from:
            rh = out.get(RemoteHostFilter.fullname(), None)
            if (rh is None) or (not isinstance(rh, RemoteHostFilterResult)):
                tx.mail_response = Response(
                    450, 'internal error: expected remote host filter result')
                return FilterResult()
            remote_host : RemoteHostFilterResult = rh
            if not remote_host.fcrdns:
                tx.mail_response = Response(550, 'fcrdns required')
                return FilterResult()
            if not remote_host.remote_hostname:
                tx.mail_response = Response(550, 'ptr error?')
                return FilterResult()

            if tx.smtp_meta is None or (
                    ehlo := tx.smtp_meta.get('ehlo_host', None)) is None:
                tx.mail_response = Response(
                    450, 'internal error: expected smtp meta ehlo')
                return FilterResult()

            # XXX case
            # XXX should RemoteHostFilter drop trailing dot from dns?
            if ehlo != remote_host.remote_hostname.rstrip('.'):
                tx.mail_response = Response(
                    550, 'ehlo must match remote hostname')
                return FilterResult()

        body = tx_delta.maybe_body_blob()
        if body and body.finalized():
            valid = tx.get_filter_output(MessageValidationFilter.fullname())
            if not isinstance(valid, MessageValidationFilterResult):
                tx.data_response = Response(
                    450, 'internal error: expected MessageValidationFilterResult')
                return FilterResult()
            if err := valid.check_validity(self.min_validity):
                tx.data_response = Response(
                    550, '5.6.0 invalid message content: ' + err)
                return FilterResult()
            elif valid.received_header_count > self.max_received_headers:
                tx.data_response = Response(
                    550, '5.4.6 message has too many received: '
                    'headers and is likely looping')
                return FilterResult()

        return FilterResult()


