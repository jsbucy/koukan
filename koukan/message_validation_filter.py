# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0

from typing import List, Optional
import logging
from enum import IntEnum

from tempfile import TemporaryFile
from email.parser import BytesParser
import email.policy
from email.errors import (
    MissingHeaderBodySeparatorDefect )

from koukan.blob import Blob
from koukan.filter import TransactionMetadata
from koukan.filter_chain import Filter, FilterResult

class MessageValidationFilterResult:
    received_header_count = 0

    class Status(IntEnum):
        # major problem with headers e.g. MissingHeaderBodySeparatorDefect
        NONE = 0
        # headers minimally well-formed
        BASIC = 1
        # exactly 1 well-formed From, Date, Message-ID
        # suggested default for ingress
        MEDIUM = 2
        # no defects reported by email parser
        # suggested default for submission
        # we may relax specific defects that are found to be pedantic
        # in practice
        HIGH = 3
    status : Status

    err : Optional[str] = None

    def __init__(self, received_header_count : Optional[int] = None):
        self.status = self.Status.HIGH
        if received_header_count is not None:
            self.received_header_count = received_header_count

    # Sets the first error that caused the message not to qualify for status.
    # Status is monotonic; errors should be checked from low to high
    def add_error(self, status, err : str):
        assert self.status >= status
        assert self.err is None
        self.err = err
        self.status = status - 1
        self.err = err

    def check_validity(self, status : Status) -> Optional[str]:
        if self.status >= status:
            return None
        if self.err:
            return self.err
        assert False, 'err not populated'

class MessageValidationFilter(Filter):

    def __init__(self, max_header_bytes = 1048576,
                 max_mime_tree_depth = 20):
        self.max_header_bytes = max_header_bytes
        self.max_mime_tree_depth = max_mime_tree_depth

    def _check_mime_tree_defects(self, part, depth):
        if part.defects:
            return True

        for k,v in part.items():
            if v.defects:
                return True

        for subpart in part.iter_parts():
            if (depth + 1) > self.max_mime_tree_depth:
                return True
            if self._check_mime_tree_defects(subpart, depth + 1):
                return True
        return False

    def on_update(self, tx_delta : TransactionMetadata):
        assert self.downstream_tx is not None
        if not isinstance(tx_delta.body, Blob) or not tx_delta.body.finalized():
            return FilterResult()

        result = self._check(tx_delta.body)
        self.downstream_tx.add_filter_output(self.fullname(), result)
        return FilterResult()

    def _check(self, body_blob : Blob) -> MessageValidationFilterResult:
        result = MessageValidationFilterResult()
        with TemporaryFile('w+b') as file:
            b = body_blob.pread(0)
            assert b is not None
            # If we can't find the end of the headers in the first 1M,
            # the message is probably garbage. Don't risk some cpu/mem
            # complexity blowup trying to parse it.
            if b.find(b'\r\n\r\n', 0, self.max_header_bytes) == -1:
                result.add_error(
                    MessageValidationFilterResult.Status.BASIC,
                    'grossly excessive/malformed headers')
                return result
            file.write(b)
            file.flush()
            file.seek(0)
            parser = BytesParser(policy=email.policy.SMTP)
            parsed = parser.parse(file)

        for d in parsed.defects:
            if isinstance(d, MissingHeaderBodySeparatorDefect):
                result.add_error(MessageValidationFilterResult.Status.BASIC,
                                 'MissingHeaderBodySeparatorDefect')
                return result

        for k,v in parsed.items():
            if k.lower() == 'received':
                result.received_header_count += 1

        headers = set()
        for k,v in parsed.items():
            for header in ['from', 'date', 'message-id']:
                if k.lower() == header:
                    if header in headers or v.defects:
                        result.add_error(
                            MessageValidationFilterResult.Status.MEDIUM,
                            'invalid ' + header)
                        return result
                    else:
                        headers.add(header)

        if result.status >= MessageValidationFilterResult.Status.MEDIUM:
            for header in ['from', 'date', 'message-id']:
                if header not in headers:
                    result.add_error(
                        MessageValidationFilterResult.Status.MEDIUM,
                        'missing ' + header)
                    return result

        if result.status >= MessageValidationFilterResult.Status.HIGH:
            if self._check_mime_tree_defects(parsed, 0):
                result.add_error(
                    MessageValidationFilterResult.Status.HIGH, 'mime tree')
                return result
        return result
