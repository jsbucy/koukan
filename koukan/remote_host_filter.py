# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional, Tuple
from enum import IntEnum

import dns.resolver
import logging

from koukan.filter import TransactionMetadata
from koukan.filter_chain import FilterResult, Filter
from koukan.response import Response

from koukan.matcher_result import MatcherResult
from koukan.filter_output import FilterOutput

from koukan.dns_wrapper import NotFoundExceptions, Resolver, ServFailExceptions
from koukan.rest_schema import WhichJson

class RemoteHostFilterOutput(FilterOutput):
    # to_json() emits these as int so only extend this enum conservatively
    class Status(IntEnum):
        OK = 0
        DNS_TEMP = 1
        NOT_FOUND = 2

    status : Status
    remote_hostname : Optional[str] = None
    # if tx.remote_host is None, the filter won't populate
    # RemoteHostFilterOutput at all (PRECONDITION_MISSING), any dns
    # error -> fcrdns == False
    fcrdns : bool
    ehlo_alignment : bool

    def __init__(self, status : Status,
                 remote_hostname : Optional[str] = None,
                 fcrdns : bool = False,
                 ehlo_alignment = False):
        self.status = status
        self.remote_hostname = remote_hostname
        self.fcrdns = fcrdns
        self.ehlo_alignment = ehlo_alignment

    def to_json(self, w : WhichJson):
        if w not in [WhichJson.DB_ATTEMPT,
                     WhichJson.REST_CREATE,
                     WhichJson.REST_UPDATE]:
            return None
        out = {'status': int(self.status),
               'fcrdns': self.fcrdns,
               'ehlo_alignment': self.ehlo_alignment}
        return out

    def match(self, yaml : dict):
        if (expected_fcrdns := yaml.get('fcrdns', None)) is not None:
            if self.fcrdns is None:
                return MatcherResult.PRECONDITION_UNMET
            if bool(self.fcrdns) != expected_fcrdns:
                return MatcherResult.NO_MATCH
        if (expected_ehlo_alignment :=
            yaml.get('ehlo_alignment', None)) is not None:
            if self.ehlo_alignment != expected_ehlo_alignment:
                return MatcherResult.NO_MATCH
        return MatcherResult.MATCH

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
        if tx.remote_host is None or not tx.remote_host.host:
            return None
        ans = None
        try:
            ans = self.resolver.resolve_address(tx.remote_host.host)
        except ServFailExceptions:
            tx.add_filter_output(self.fullname(), RemoteHostFilterOutput(
                RemoteHostFilterOutput.Status.DNS_TEMP))
            return Response(450, 'RemoteHostFilter ptr err')
        except NotFoundExceptions:
            pass

        remote_hostname = None
        fcrdns = False
        if not(ans) or not ans[0].target:
            remote_hostname = ''
            fcrdns = False
            tx.add_filter_output(self.fullname(), RemoteHostFilterOutput(
                RemoteHostFilterOutput.Status.NOT_FOUND,
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
            tx.add_filter_output(self.fullname(), RemoteHostFilterOutput(
                RemoteHostFilterOutput.Status.DNS_TEMP, remote_hostname,
                fcrdns))
            return Response(450, 'RemoteHostFilter fwd err')

        ehlo_alignment = False
        if fcrdns:
            if (tx.smtp_meta is not None and
                ((ehlo := tx.smtp_meta.get('ehlo_host', None)) is not None) and
                # XXX should this drop trailing dot from remote_hostname?
                ehlo == remote_hostname.rstrip('.')):
                ehlo_alignment = True

        tx.add_filter_output(self.fullname(), RemoteHostFilterOutput(
            RemoteHostFilterOutput.Status.OK, remote_hostname, fcrdns,
            ehlo_alignment))

        logging.debug('RemoteHostFilter._resolve() '
                      'remote_hostname=%s fcrdns=%s',
                      remote_hostname, fcrdns)

        return None
