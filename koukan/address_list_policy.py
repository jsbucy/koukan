# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Any, List, Optional, Tuple

from email import _header_value_parser

import logging

from koukan.address import domain_from_address
from koukan.response import Response
from koukan.recipient_router_filter import Destination, RoutingPolicy

from koukan.matcher_result import MatcherResult

def _match(domains, prefixes, delimiter, addr):
    try:
        spec = _header_value_parser.get_addr_spec(addr)
    except:
        return False
    if len(spec) != 2 or spec[0].defects:
        return False
    domain = spec[0].domain.lower()
    local_part = spec[0].local_part.lower()
    if domain is None or local_part is None:
        return False

    if domains and domain not in domains:
        return False

    # catchall
    if prefixes == []:
        return True

    for prefix in prefixes:
        if (local_part == prefix or
            (delimiter is not None and
             local_part.startswith(prefix + delimiter))):
            return True
    return False

def match_address_list(yaml, tx):
    which_addr = yaml.get('which_addr', 'mail_from')
    addr = None
    if which_addr == 'mail_from':
        # TODO save parsed env-from Address somewhere
        if tx.mail_from is None:
            return MatcherResult.PRECONDITION_UNMET
        addr = tx.mail_from.mailbox
    elif which_addr == 'header_from':
        out = tx.get_filter_output('koukan.message_validation_filter')
        # cf MessageValidationFilterOutput this is reserialized from
        # the parsed headers
        if out is None or out.parsed_header_from is None:
            return MatcherResult.PRECONDITION_UNMET
        addr = out.parsed_header_from.addr_spec
    else:
        assert False, 'unknown which_addr ' + which_addr

    return MatcherResult.from_bool(_match(
        yaml.get('domains', []),
        yaml.get('prefixes', []),
        yaml.get('delimiter', None),
        addr))

class AddressListPolicy(RoutingPolicy):
    domains : List[str]
    prefixes : List[str]
    delimiter : Optional[str]
    dest : Optional[Destination] = None

    # dest == None -> reject
    def __init__(self, domains : List[str],
                 delimiter: Optional[str],
                 prefixes : List[str],
                 dest: Optional[Destination] = None):
        self.prefixes = prefixes
        self.delimiter = delimiter
        self.domains = domains
        self.dest = dest

    def _dest(self) -> Tuple[Optional[Destination], Optional[Response]]:
        if self.dest is not None:
            return self.dest, None
        else:
            return None, Response(
            550, '5.1.1 mailbox does not exist (AddressListPolicy)')

    # called on the first recipient in the transaction
    # -> (Endpoint, host, resp)
    def endpoint_for_rcpt(
            self, rcpt) -> Tuple[Optional[Destination], Optional[Response]]:
        if not _match(self.domains, self.prefixes, self.delimiter, rcpt):
            return None, None
        return self._dest()
