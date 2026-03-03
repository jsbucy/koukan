# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional
from ipaddress import ip_address, ip_network

from koukan.matcher_result import MatcherResult
from koukan.filter import TransactionMetadata

# Kitchen sink for trivial matchers

def match_network_address(
        yaml : dict, tx : TransactionMetadata, rcpt_num : Optional[int]):
    assert tx.mail_from is None or tx.remote_host is not None
    if tx.remote_host is None:
        return MatcherResult.PRECONDITION_UNMET
    ip = ip_address(tx.remote_host.host)
    cidr = ip_network(yaml['cidr'])
    return MatcherResult.from_bool(ip in cidr)

def match_smtp_tls(
        yaml : dict, tx : TransactionMetadata, rcpt_num : Optional[int]):
    assert tx.mail_from is None or tx.smtp_meta is not None
    if tx.smtp_meta is None:
        return MatcherResult.PRECONDITION_UNMET
    return MatcherResult.from_bool(tx.smtp_meta.get('tls', False))

def match_smtp_auth(
        yaml : dict, tx : TransactionMetadata, rcpt_num : Optional[int]):
    assert tx.mail_from is None or tx.smtp_meta is not None
    if tx.smtp_meta is None:
        return MatcherResult.PRECONDITION_UNMET
    return MatcherResult.from_bool(tx.smtp_meta.get('auth', False))

def match_num_rcpts(
        yaml : dict, tx : TransactionMetadata, rcpt_num : Optional[int]):
    rcpts = 0
    assert rcpt_num is not None
    assert tx.rcpt_to[rcpt_num] is not None
    for i,resp in enumerate(tx.rcpt_response):
        if i >= rcpt_num:
            break
        if resp is not None and resp.ok():
            rcpts += 1
            if rcpts >= yaml['max_rcpts']:
                return MatcherResult.MATCH
    return MatcherResult.NO_MATCH
