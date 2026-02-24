# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0

from ipaddress import ip_address, ip_network

from koukan.matcher_result import MatcherResult
from koukan.filter import TransactionMetadata

# Kitchen sink for trivial matchers

def match_network_address(yaml : dict, tx : TransactionMetadata):
    assert tx.mail_from is None or tx.remote_host is not None
    if tx.remote_host is None:
        return MatcherResult.PRECONDITION_UNMET
    ip = ip_address(tx.remote_host.host)
    cidr = ip_network(yaml['cidr'])
    return MatcherResult.from_bool(ip in cidr)

def match_tls(yaml : dict, tx : TransactionMetadata):
    assert tx.mail_from is None or tx.smtp_meta is not None
    if tx.smtp_meta is None:
        return MatcherResult.PRECONDITION_UNMET
    return MatcherResult.from_bool(tx.smtp_meta.get('tls', False))

def match_smtp_auth(yaml : dict, tx : TransactionMetadata):
    assert tx.mail_from is None or tx.smtp_meta is not None
    if tx.smtp_meta is None:
        return MatcherResult.PRECONDITION_UNMET
    return MatcherResult.from_bool(tx.smtp_meta.get('auth', False))
