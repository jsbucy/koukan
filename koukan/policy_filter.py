# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional
import logging
from enum import IntEnum

from koukan.filter import TransactionMetadata
from koukan.filter_chain import Filter, FilterResult
from koukan.response import Response

from koukan.remote_host_filter import RemoteHostFilter, RemoteHostFilterResult
from koukan.message_validation_filter import (
    MessageValidationFilter, MessageValidationFilterResult )

# this should go last before exploder in the downstream chain
class PolicyFilter(Filter):
    # for the time being, it seems simpler for this to just have a
    # handful of modes and conditional logic to toggle checks per the
    # mode
    class Mode(IntEnum):
        INGRESS = 0
        SUBMISSION = 1

    max_received_headers : int
    min_validity : MessageValidationFilterResult.Status = MessageValidationFilterResult.Status.MEDIUM
    mode : Mode = Mode.INGRESS

    def __init__(self, max_received_headers : int = 30,
                 min_validity : Optional[str] = None,
                 mode : Optional[str] = None):
        self.max_received_headers = max_received_headers
        if min_validity:
            self.min_validity = MessageValidationFilterResult.Status[min_validity]
        if mode:
            self.mode = PolicyFilter.Mode[mode]

    def _check_remote_host(self, tx, tx_delta, out) -> bool:
        if (self.mode != self.Mode.INGRESS) or not tx_delta.mail_from:
            return True
        assert tx.sender is not None
        remote_host = out.get(RemoteHostFilter.fullname(), None)
        if not isinstance(remote_host, RemoteHostFilterResult):
            tx.mail_response = Response(
                450, 'internal error: expected remote host filter result')
            return False
        if not remote_host.fcrdns:
            tx.mail_response = Response(550, 'fcrdns required')
            return False
        if not remote_host.remote_hostname:
            tx.mail_response = Response(550, 'ptr error?')
            return False

        if tx.smtp_meta is None or (
                ehlo := tx.smtp_meta.get('ehlo_host', None)) is None:
            tx.mail_response = Response(
                450, 'internal error: expected smtp meta ehlo')
            return False

        # XXX case
        # XXX should RemoteHostFilter drop trailing dot from dns?
        if ehlo != remote_host.remote_hostname.rstrip('.'):
            tx.mail_response = Response(
                550, 'ehlo must match remote hostname')
            return False
        return True

    def _check_body(self, tx, tx_delta, out):
        valid = tx.get_filter_output(MessageValidationFilter.fullname())
        if valid is None:
            return True
        if not isinstance(valid, MessageValidationFilterResult):
            tx.data_response = Response(
                450, 'internal error: expected MessageValidationFilterResult')
            return False
        if err := valid.check_validity(self.min_validity):
            tx.data_response = Response(
                550, '5.6.0 invalid message content: ' + err)
            return False
        elif valid.received_header_count > self.max_received_headers:
            tx.data_response = Response(
                550, '5.4.6 message has too many received: '
                'headers and is likely looping')
            return False
        return True

    def on_update(self, tx_delta : TransactionMetadata):
        tx = self.downstream_tx
        assert tx is not None
        out = tx.filter_output

        # no filter added any output
        if out is None:
            out = {}

        for i in range(0,1):
            if not self._check_remote_host(tx, tx_delta, out):
                break
            if not self._check_body(tx, tx_delta, out):
                break
        return FilterResult()
