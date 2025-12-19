# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional, Tuple
from enum import IntEnum

import dns.resolver
import logging

from koukan.filter import TransactionMetadata
from koukan.filter_chain import FilterResult, Filter
from koukan.response import Response

from koukan.dns_wrapper import NotFoundExceptions, Resolver, ServFailExceptions

class RemoteHostFilterResult:
    class Status(IntEnum):
        OK = 0
        DNS_TEMP = 1
        NOT_FOUND = 2

    status : Status
    remote_hostname : Optional[str] = None
    fcrdns : Optional[bool] = None

    def __init__(self, status : Status,
                 remote_hostname : Optional[str] = None,
                 fcrdns : Optional[bool] = None):
        self.status = status
        self.remote_hostname = remote_hostname
        self.fcrdns = fcrdns

class RemoteHostFilter(Filter):
    def __init__(self, resolver : Optional[Resolver] = None):
        self.resolver = resolver if resolver else Resolver()

    def on_update(self, tx_delta : TransactionMetadata):
        assert self.downstream_tx is not None
        if tx_delta.mail_from is not None:
            assert self.downstream_tx.mail_response is None
            self.downstream_tx.mail_response = self._resolve()
        return FilterResult()

    def _resolve(self) -> Optional[Response]:
        tx = self.downstream_tx
        assert tx is not None
        if (tx.remote_host is None or
            not tx.remote_host.host):
            return None
        ans = None
        try:
            ans = self.resolver.resolve_address(tx.remote_host.host)
        except ServFailExceptions:
            tx.add_filter_output(self.fullname(), RemoteHostFilterResult(
                RemoteHostFilterResult.Status.DNS_TEMP))
            return Response(450, 'RemoteHostFilter ptr err')
        except NotFoundExceptions:
            pass

        remote_hostname = None
        fcrdns = False
        if not(ans) or not ans[0].target:
            remote_hostname = ''
            fcrdns = False
            tx.add_filter_output(self.fullname(), RemoteHostFilterResult(
                RemoteHostFilterResult.Status.NOT_FOUND,
                remote_hostname, fcrdns))
            return None
        remote_hostname = str(ans[0].target)

        all_failed = True
        for rrtype in ['a', 'aaaa']:
            ans = None
            try:
                ans = self.resolver.resolve(remote_hostname, rrtype)
                all_failed = False
            except ServFailExceptions:
                pass
            except NotFoundExceptions:
                all_failed = False

            if ans is None:
                continue
            for a in ans:
                logging.debug('RemoteHostFilter._resolve %s %s %s',
                              rrtype, str(a), tx.remote_host.host)
                if str(a) == tx.remote_host.host:
                    fcrdns = True
                    break
            if fcrdns:
                break
        else:
            fcrdns = False

        if all_failed:
            tx.add_filter_output(self.fullname(), RemoteHostFilterResult(
                RemoteHostFilterResult.Status.DNS_TEMP, remote_hostname,
                fcrdns))
            return Response(450, 'RemoteHostFilter fwd err')

        tx.add_filter_output(self.fullname(), RemoteHostFilterResult(
            RemoteHostFilterResult.Status.OK, remote_hostname, fcrdns))

        logging.debug('RemoteHostFilter._resolve() '
                      'remote_hostname=%s fcrdns=%s',
                      remote_hostname, fcrdns)

        return None
