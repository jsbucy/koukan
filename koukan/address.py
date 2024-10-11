# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from email import _header_value_parser


def domain_from_address(addr):
    spec = _header_value_parser.get_addr_spec(addr)
    if len(spec) != 2 or spec[0].defects:
        return None
    return spec[0].domain
