# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Any, List, Optional, Tuple

from email import _header_value_parser

import logging

from koukan.address import domain_from_address
from koukan.response import Response
from koukan.recipient_router_filter import Destination, RoutingPolicy

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
        try:
            spec = _header_value_parser.get_addr_spec(rcpt)
        except:
            return None, None
        if len(spec) != 2 or spec[0].defects:
            return None, None
        domain = spec[0].domain.lower()
        local_part = spec[0].local_part.lower()
        if domain is None or local_part is None:
            return None, None

        if self.domains and domain not in self.domains:
            return None, None

        # catchall
        if self.prefixes == []:
            return self._dest()

        for prefix in self.prefixes:
            if (local_part == prefix or
                (self.delimiter is not None and
                 local_part.startswith(prefix + self.delimiter))):
                return self._dest()

        return None, None
