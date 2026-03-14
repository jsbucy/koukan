# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0

from typing import Any, Dict, List, Optional, Tuple
from enum import IntEnum
import logging

from koukan.dns_wrapper import (
    Resolver,
    NoAnswer,
    NXDOMAIN,
    NotFoundExceptions,
    ServFailExceptions)
import ipaddress

from koukan.address import domain_from_address
from koukan.filter import HostPort, Resolution, TransactionMetadata
from koukan.rest_schema import WhichJson
from koukan.filter_chain import FilterResult, ProxyFilter
from koukan.response import Response
from koukan.filter_output import FilterOutput
from koukan.matcher_result import MatcherResult

class Result(IntEnum):
    OK = 0
    NX = 1
    TEMP = 2

class DnsResolutionFilterOutput(FilterOutput):
    mail_from_result : Optional[Result] = None
    rcpt_to_result : List[Optional[Result]]
    resolution_result : Optional[Result] = None

    def __init__(self):
        self.rcpt_to_result = []

    def copy(self):
        out = DnsResolutionFilterOutput()
        out.mail_from_result = self.mail_from_result
        out.rcpt_to_result = list(self.rcpt_to_result)
        out.resolution_result = self.resolution_result
        return out

    def to_json(self, which_js : WhichJson) -> Optional[dict]:
        if which_js not in [WhichJson.DB_ATTEMPT,
                            WhichJson.REST_CREATE,
                            WhichJson.REST_UPDATE]:
            return None

        out : Dict[str, Any] = {}
        if self.mail_from_result is not None:
            out['mail_from_result'] = int(self.mail_from_result)
        if self.rcpt_to_result:
            out['rcpt_to_result'] = self.rcpt_to_result
        if self.resolution_result is not None:
            out['resolution_result'] = int(self.resolution_result)
        return out

    def match(self, yaml : dict, rcpt_num : Optional[int]) -> MatcherResult:
        expected_mail = yaml.get('mail_result', None)
        if expected_mail is not None:
            if self.mail_from_result is None:
                return MatcherResult.PRECONDITION_UNMET
            elif self.mail_from_result != Result[expected_mail]:
                return MatcherResult.NO_MATCH

        expected_rcpt = yaml.get('rcpt_result', None)
        if expected_rcpt is not None:
            assert rcpt_num is not None
            rcpt_result = self.rcpt_to_result[rcpt_num]
            if rcpt_result != Result[expected_rcpt]:
                return MatcherResult.NO_MATCH

        expected_resolution = yaml.get('resolution_result', None)
        if expected_resolution is not None:
            if self.resolution_result is None:
                return MatcherResult.PRECONDITION_UNMET
            elif self.resolution_result != Result[expected_resolution]:
                return MatcherResult.NO_MATCH

        return MatcherResult.MATCH


# TODO: need more sophisticated timeout handling? cumulative timeout rather
# than per-lookup?
def resolve(resolver, hostport : HostPort) -> Optional[List[str]]:
    try:
        answers = resolver.resolve(hostport.host, 'MX')
        answers = sorted(answers, key=lambda x: x.preference)
        # null mx rfc7505
        if len(answers) == 1 and answers[0].preference == 0 and answers[0].exchange.labels == (b'',):
            return None
        mxen = [ mx.exchange for mx in answers]
    except ServFailExceptions as e:
        return []
    except NoAnswer as e:  # name exists but not that rrtype
        mxen = [hostport.host]
    except NXDOMAIN as e:  # name doesn't exist at all
        return None

    seen = []
    # It seems like the ordering gets randomized somewhere upstream so
    # we don't need to?
    for mx in mxen:
        # TODO newer library has dns.resolver.resolve_name() does both
        # A and AAAA
        for rrtype in ['a', 'aaaa']:
            try:
                a = resolver.resolve(mx, rrtype)
            except ServFailExceptions + NotFoundExceptions:
                continue
            for aa in a:
                aaa = str(aa)
                if aaa in seen:
                    continue
                seen.append(aaa)
    return seen

class DnsResolutionFilter(ProxyFilter):
    static_resolution : Optional[Resolution] = None
    suffix : Optional[str] = None  # empty = match all
    literal : Optional[str] = None
    resolver : Resolver

    def __init__(self,
                 static_resolution : Optional[Resolution] = None,
                 suffix : Optional[str] = None,
                 literal : Optional[str] = None,
                 resolver : Optional[Resolver] = None):
        self.resolver = resolver if resolver else Resolver()
        self.suffix = suffix
        self.literal = literal
        self.static_resolution = static_resolution

    def _valid_ip(self, ip):
        try:
            ipaddress.ip_address(ip)
            return True
        except ValueError:
            return False

    def _needs_resolution_host(self, host : Optional[HostPort]):
        return host is not None and not self._valid_ip(host.host) and self._match(host.host)

    def _needs_resolution(self, res : Optional[Resolution]) -> bool:
        if res is None or not res.hosts:
            return False
        return any([self._needs_resolution_host(h) for h in res.hosts])

    def _match(self, h):
        if self.literal is not None and self.literal.lower() == h.lower():
            return True
        if self.suffix is not None and h.lower().endswith(self.suffix.lower()):
            return True
        return False


    def _resolve(self, res : Resolution) -> Tuple[Result,List[HostPort]]:
        hosts_out : List[HostPort] = []
        if not res.hosts:
            return Result.TEMP, hosts_out
        for h in res.hosts:
            if not self._needs_resolution_host(h):
                hosts_out.append(h)
                continue
            if self.static_resolution is not None:
                assert self.static_resolution.hosts is not None
                hp_out = self.static_resolution.hosts
            else:
                dns_hosts = resolve(self.resolver, h)
                if dns_hosts is None:
                    return Result.NX, []
                if not dns_hosts:
                    return Result.TEMP, []
                hp_out = [ HostPort(hh, h.port) for hh in dns_hosts ]

            # A router policy could end up returning multiple hosts
            # that resolve to overlapping sets of IPs so drop any
            # duplicates here.
            for hp in hp_out:
                if hp in hosts_out:
                    logging.info('DnsResolutionFilter._resolver dropping '
                                 'duplicate host %s', hp)
                    continue
                hosts_out.append(hp)
        return Result.OK, hosts_out

    def on_update(self, tx_delta : TransactionMetadata):
        assert self.downstream_tx is not None
        assert self.upstream_tx is not None

        downstream_resolution = tx_delta.resolution
        if (downstream_resolution is not None and
            self._needs_resolution(downstream_resolution)):
            tx_delta.resolution = None
        else:
            downstream_resolution = None
        self.upstream_tx.merge_from(tx_delta)

        if (downstream_resolution is None and
            tx_delta.mail_from is None and
            not tx_delta.rcpt_to):
            return FilterResult()

        out = self.downstream_tx.get_filter_output(self.fullname())
        if out is None:
            out = DnsResolutionFilterOutput()
        else:
            out = out.copy()
        self.downstream_tx.add_filter_output(self.fullname(), out)
        self.upstream_tx.add_filter_output(self.fullname(), out)

        if tx_delta.mail_from:
            mail_from_domain = domain_from_address(tx_delta.mail_from.mailbox)
            if mail_from_domain:
                out.mail_from_result, hosts = self._resolve(
                    Resolution([HostPort(mail_from_domain, 0)]))
                logging.debug('%s %s', mail_from_domain, out.mail_from_result)

        if tx_delta.rcpt_to:
            off = tx_delta.rcpt_to_list_offset
            assert off is not None
            out.rcpt_to_result.extend(
                [None] * (len(self.downstream_tx.rcpt_to) - len(out.rcpt_to_result)))
            for i,rcpt in enumerate(tx_delta.rcpt_to):
                assert rcpt is not None
                rcpt_domain = domain_from_address(rcpt.mailbox)
                out.rcpt_to_result[off + i], hosts = self._resolve(
                    Resolution([HostPort(rcpt_domain, 0)]))

        if downstream_resolution:
            assert self.upstream_tx.resolution is None
            out.resolution_result, hosts = self._resolve(downstream_resolution)
            logging.debug('%s %s', out.resolution_result, hosts)
            if out.resolution_result == Result.OK:
                assert hosts
                self.upstream_tx.resolution = Resolution(hosts)


        # NOTE _resolve() passes through verbatim hosts that
        # didn't _match() so this won't fail unless there were
        # none of those

        return FilterResult()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d %(message)s')

    import sys
    resolver = Resolver()
    for host in sys.argv[1:]:
        logging.debug(resolve(resolver, HostPort(host, port=25)))
