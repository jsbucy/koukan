# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import List, Optional
import logging

from dns.resolver import NoNameservers
from koukan.dns_wrapper import Resolver, NotFoundExceptions, ServFailExceptions
import ipaddress

from koukan.filter import HostPort, Resolution, SyncFilter, TransactionMetadata
from koukan.response import Response

# TODO: need more sophisticated timeout handling? cumulative timeout rather
# than per-lookup?
def resolve(resolver, hostport : HostPort):
    try:
        answers = resolver.resolve(hostport.host, 'MX')
        answers = sorted(answers, key=lambda x: x.preference)
        mxen = [ mx.exchange for mx in answers]
    except ServFailExceptions:
        return []
    except NotFoundExceptions:
        mxen = [hostport.host]

    # TODO null mx rfc7505
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

class DnsResolutionFilter(SyncFilter):
    upstream : SyncFilter
    static_resolution : Optional[Resolution] = None
    resolution : Optional[Resolution] = None
    first = True
    suffix : Optional[str] = None  # empty = match all
    literal : Optional[str] = None
    resolver : Resolver

    def __init__(self, upstream : SyncFilter,
                 static_resolution : Optional[Resolution] = None,
                 suffix : Optional[str] = None,
                 literal : Optional[str] = None,
                 resolver : Optional[Resolver] = None):
        self.resolver = resolver if resolver else Resolver()
        self.upstream = upstream
        self.suffix = suffix
        self.literal = literal
        self.upstream = upstream
        self.static_resolution = static_resolution

    def _valid_ip(self, ip):
        try:
            ipaddress.ip_address(ip)
            return True
        except ValueError:
            return False

    def _needs_resolution_host(self, host : HostPort):
        return not self._valid_ip(host.host) and self._match(host.host)

    def _needs_resolution(self, res : Optional[Resolution]) -> bool:
        if res is None:
            return False
        return any([self._needs_resolution_host(h) for h in res.hosts])

    def _match(self, h):
        if self.literal is not None and self.literal.lower() == h.lower():
            return True
        if self.suffix is not None and h.lower().endswith(self.suffix.lower()):
            return True
        return False


    def _resolve(self, res : Resolution) -> List[HostPort]:
        hosts_out = []
        for h in res.hosts:
            if not self._needs_resolution_host(h):
                hosts_out.append(h)
                continue
            if self.static_resolution is not None:
                hp_out = self.static_resolution.hosts
            else:
                dns_hosts = resolve(self.resolver, h)
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
        return hosts_out

    def on_update(self,
                  tx : TransactionMetadata, tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:
        if (self.resolution is None and
            not self._needs_resolution(tx_delta.resolution)):
            return self.upstream.on_update(tx, tx_delta)

        resolution = None
        if self.resolution is None:
            resolution = Resolution(self._resolve(tx_delta.resolution))
            # NOTE _resolve() passes through verbatim hosts that
            # didn't _match() so this won't fail unless there were
            # none of those
            if not resolution.hosts:
                err = TransactionMetadata()
                tx.fill_inflight_responses(
                    Response(450, 'DnsResolverFilter empty result'), err)
                tx.merge_from(err)
                return err

            self.resolution = resolution

        downstream_tx = tx.copy()
        downstream_tx.resolution = self.resolution
        downstream_delta = tx_delta.copy()
        downstream_delta.resolution = resolution
        return self.upstream.on_update(downstream_tx, downstream_delta)


if __name__ == '__main__':
    import sys
    resolver = Resolver()
    for host in sys.argv[1:]:
        for a in resolve(resolver, HostPort(host, port=25)):
            print(a)
