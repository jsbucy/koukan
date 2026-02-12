# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0

from typing import Any, Dict, List, Optional, Type
import logging
from enum import IntEnum
import string

from tempfile import TemporaryFile
from email.parser import BytesParser, Parser
import email.policy
from email.contentmanager import ContentManager, get_non_text_content
from email.errors import (
    MessageDefect,
    MissingHeaderBodySeparatorDefect,
    NonASCIILocalPartDefect,
    NonPrintableDefect,
    UndecodableBytesDefect )
from email.headerregistry import Address, AddressHeader

from koukan.blob import Blob
from koukan.filter import TransactionMetadata, get_esmtp_param
from koukan.filter_chain import Filter, FilterResult
from koukan.matcher_result import MatcherResult
from koukan.filter_output import FilterOutput
from koukan.rest_schema import WhichJson

class MessageValidationFilterResult(FilterOutput):
    received_header_count = None
    # NOTE Address parses addr_spec in __init__ and Address.addr_spec
    # reserializes from parsed so parsed_header_from.addr_spec may not
    # be identical to the original?
    parsed_header_from : Optional[Address] = None

    class Status(IntEnum):
        # major problem with headers e.g. MissingHeaderBodySeparatorDefect
        NONE = 0
        # all headers can be parsed rfc5322 optional-field
        # https://www.rfc-editor.org/rfc/rfc5322#section-3.6.8
        BASIC = 1
        # exactly 1 well-formed From, Date, Message-ID
        # headers do not contain non-printing ascii, utf8 <=> SMTPUTF8
        # suggested default for ingress
        MEDIUM = 2
        # no defects reported by email parser
        # suggested default for submission
        HIGH = 3
    status : Status

    err : Optional[str] = None

    def __init__(self, received_header_count : Optional[int] = None):
        self.status = self.Status.HIGH
        if received_header_count is not None:
            self.received_header_count = received_header_count

    def to_json(self, w : WhichJson):
        if w not in [WhichJson.DB_ATTEMPT,
                     WhichJson.REST_CREATE,
                     WhichJson.REST_UPDATE]:
            return None
        out : Dict[str, Any] = {'status': int(self.status)}
        if self.received_header_count is not None:
            out['received_header_count'] = self.received_header_count
        if self.err:
            out['err'] = self.err
        return out

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

    def match(self, yaml : dict):
        if (r := yaml.get('max_received_headers', None)) is not None:
            if self.received_header_count is None:
                return MatcherResult.PRECONDITION_UNMET
            if self.received_header_count <= r:
                return MatcherResult.NO_MATCH
        if (v := yaml.get('validity_threshold', None)) is not None:
            if self.status is None:
                return MatcherResult.PRECONDITION_UNMET
            if self.status >= MessageValidationFilterResult.Status[v]:
                return MatcherResult.NO_MATCH
        return MatcherResult.MATCH

    def _maybe_set_from(self, header : AddressHeader):
        # parser reports header from in a group too
        # for the time being, only report the address if there was
        # exactly 1, the other cases are uncommon
        if len(header.groups) > 1:
            return
        if len(header.groups) == 1 and len(header.groups[0].addresses) > 1:
            return
        if len(header.addresses) != 1:
            return
        self.parsed_header_from = header.addresses[0]

# returns true if d contains an instance of any class in dc
def _has_defect(defects : List[MessageDefect], defect_classes : List[Type]):
    for defect in defects:
        for defect_class in defect_classes:
            if isinstance(defect, defect_class):
                return True
    return False

# returns true if any element of d is not an instance of any class in dc
def _has_other_defect(defects : List[MessageDefect],
                      defect_classes : List[Type]):
    for defect in defects:
        for defect_class in defect_classes:
            if isinstance(defect, defect_class):
                break
        else:
            return True
    return False

def _get_all(message, header_name):
    headers = message.get_all(header_name)
    if not headers:
        return []
    return headers

class MessageValidationFilter(Filter):
    """classifies message format (rfc822/mime) problems

    This filter parses rfc822/mime from tx.body and classifies the
    message into a small number of "validity level" buckets which it
    populates in tx.filter_output for consumption by an upstream
    policy filter.

    The underlying python email.parser decorates components of the
    message with _defects_. This is a bit of an experiment to see if
    it is feasible to enumerate known false positive defects from the
    python parser (i.e. incorrectly reporting a defect on a valid message) and
    fail-closed.
    """
    max_header_bytes : int
    max_mime_tree_depth : int
    content_manager : ContentManager

    def __init__(self, max_header_bytes = 1048576,
                 max_mime_tree_depth = 20):
        self.max_header_bytes = max_header_bytes
        self.max_mime_tree_depth = max_mime_tree_depth

        # use custom content manager with catchall so get_content()
        # doesn't throw on unknown mime type
        self.content_manager = ContentManager()
        self.content_manager.add_get_handler('', get_non_text_content)

    # returns true if any element of the mime tree rooted at part
    # contains defects
    def _check_mime_tree_defects(self, part, accepted_header_defects, depth):
        for k,v in part.items():
            if _has_other_defect(v.defects, accepted_header_defects):
                logging.debug(v.defects)
                return True

        if not part.is_multipart():
            c = part.get_content(content_manager=self.content_manager)

        # email.parser doesn't decode cte until you call get_content()
        # so if e.g. there are invalid chars in the base64, you won't
        # get the defect until after get_content()
        if part.defects:
            return True
        if part.is_multipart() and (depth + 1) > self.max_mime_tree_depth:
            return True

        for subpart in part.iter_parts():
            if self._check_mime_tree_defects(
                    subpart, accepted_header_defects, depth + 1):
                return True

        return False

    def on_update(self, tx_delta : TransactionMetadata):
        tx = self.downstream_tx

        assert tx is not None
        # TODO maybe don't need to do this e.g. in the upstream chain
        # if we already did it in the downstream chain. OTOH maybe we
        # screwed up adding a received: header and this might catch
        # that.
        if not isinstance(tx_delta.body, Blob) or not tx_delta.body.finalized():
            return FilterResult()

        result = self._check(tx_delta.body)
        tx.add_filter_output(self.fullname(), result)
        return FilterResult()

    def _check(self, body_blob : Blob) -> MessageValidationFilterResult:
        assert self.downstream_tx is not None
        assert self.downstream_tx.mail_from is not None

        result = MessageValidationFilterResult()
        smtputf8 = get_esmtp_param(
            self.downstream_tx.mail_from.esmtp, 'smtputf8') is not None

        with TemporaryFile('w+b') as file:
            b = body_blob.pread(0)
            assert b is not None
            # If we can't find the end of the headers in the first 1M,
            # the message is probably garbage. Don't risk some cpu/mem
            # complexity blowup trying to parse it.
            end_of_headers = b.find(b'\r\n\r\n', 0, self.max_header_bytes)
            if end_of_headers == -1:
                result.add_error(
                    MessageValidationFilterResult.Status.BASIC,
                    'couldn\'t find end of rfc822 headers in %d' %
                    self.max_header_bytes)
                return result
            file.write(b)
            file.flush()
            file.seek(0)

            # NOTE BytesParser uses
            # TextIOWrapper(fp, encoding='ascii', errors='surrogateescape')
            # under the hood. Using a text file seems to somehow end up with
            # unicode escapes \u4e16\u754c in the content of 8bit text parts?
            policy=email.policy.SMTPUTF8 if smtputf8 else email.policy.SMTP
            parser = BytesParser(policy=policy)
            parsed = parser.parse(file)

            full_headers = b[0:end_of_headers + 4]

        if _has_defect(parsed.defects, [MissingHeaderBodySeparatorDefect]):
            result.add_error(MessageValidationFilterResult.Status.BASIC,
                             'MissingHeaderBodySeparatorDefect')
            return result

        result.received_header_count = len(_get_all(parsed, 'received'))

        # email.parser seems to accept any octet value outside of
        # structured headers. Rank the message BASIC if the overall
        # headers aren't valid utf8 or printable ascii per the
        # smtputf8 esmtp capability. I don't know how prevalent this
        # still is in the wild. It may be that some users want a "lax
        # utf8" mode that accepts utf8 in most places in the headers.
        try:
            decoded = full_headers.decode(
                'utf-8' if smtputf8 else 'ascii', errors='strict')
            for ch in decoded:
                if ord(ch) < 0x20 and ch not in string.printable:
                    raise NonPrintableDefect
        except Exception as ex:
            result.add_error(MessageValidationFilterResult.Status.MEDIUM,
                             'invalid text encoding in headers')
            return result

        # parser emits defects for some valid smtputf8 cases
        # e.g. address headers
        # https://github.com/python/cpython/issues/81074
        accepted_defects = [NonASCIILocalPartDefect,
                            UndecodableBytesDefect] if smtputf8 else []
        if result.status >= MessageValidationFilterResult.Status.MEDIUM:
            for header_name in ['from', 'date', 'message-id']:
                headers = _get_all(parsed, header_name)
                if len(headers) != 1:
                    result.add_error(
                        MessageValidationFilterResult.Status.MEDIUM,
                        'missing/multiple ' + header_name)
                    break
                else:
                    header = headers[0]
                    if header_name == 'from':
                        result._maybe_set_from(header)
                    if _has_other_defect(header.defects, accepted_defects):
                        result.add_error(
                            MessageValidationFilterResult.Status.MEDIUM,
                            'invalid ' + header_name + str(header.defects))
                        break

        if result.status >= MessageValidationFilterResult.Status.HIGH:
            if self._check_mime_tree_defects(parsed, accepted_defects, 0):
                result.add_error(
                    MessageValidationFilterResult.Status.HIGH, 'mime tree')
                return result
        return result
