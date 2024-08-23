from typing import Optional
import logging

import dns.resolver
import ipaddress

from filter import HostPort, Resolution, SyncFilter, TransactionMetadata

# TODO: need more sophisticated timeout handling? cumulative timeout rather
# than per-lookup?
def resolve(hostport : HostPort, lifetime=30):
    try:
        answers = dns.resolver.resolve(hostport.host, 'MX', lifetime=lifetime)
        answers = sorted(answers, key=lambda x: x.preference)
        mxen = [ mx.exchange for mx in answers]
    # XXX other exceptions: NXDOMAIN, NoNameservers
    except dns.resolver.NoAnswer:
        mxen = [host]

    # TODO null mx rfc7505

    seen = []
    # It seems like the ordering gets randomized somewhere upstream so
    # we don't need to?
    for mx in mxen:
        # TODO newer library has dns.resolver.resolve_name() does both
        # A and AAAA
        for rrtype in ['a', 'aaaa']:
            try:
                a = dns.resolver.resolve(mx, rrtype, lifetime=lifetime)
            except dns.resolver.NoAnswer:
                continue
            for aa in a:
                aaa = str(aa)
                if aaa in seen: continue
                seen.append(aaa)
                #yield(HostPort(aaa, hostport.port))
    return seen

class DnsResolutionFilter(SyncFilter):
    upstream : SyncFilter
    resolution : Optional[Resolution] = None

    def __init__(self, upstream : SyncFilter):
        self.upstream = upstream

    def _valid_ip(self, ip):
        try:
            ipaddress.ip_address(ip)
            return True
        except ValueError:
            return False

    # TODO there might be some situation where this needs to be
    # selectorized to a set of domains e.g. to disable mx for internal hosts
    # TODO also for internal, may want to accept multiple hosts here
    # us-west1, us-west2, etc.
    def on_update(self,
                  tx : TransactionMetadata, tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:
        resolution = None
        if (self.resolution is None and
            tx_delta.resolution is not None and
            len(tx_delta.resolution.hosts) == 1 and
            not self._valid_ip(tx_delta.resolution.hosts[0].host)):
            host = tx_delta.resolution.hosts[0]
            hosts = resolve(host)
            resolution = Resolution()
            resolution.hosts = [HostPort(h, host.port) for h in hosts]
            self.resolution = resolution

        if self.resolution is None:
            return self.upstream(tx, tx_delta)

        downstream_tx = tx.copy()
        downstream_tx.resolution = self.resolution
        downstream_delta = tx_delta.copy()
        downstream_delta.resolution = resolution
        return self.upstream.on_update(downstream_tx, downstream_delta)


class StaticResolutionFilter(SyncFilter):
    upstream : SyncFilter
    resolution : Resolution
    first = True
    overwrite = False  # if false, only set if unset downstream
    suffix : Optional[str] = None  # empty = match all
    literal : Optional[str] = None

    def __init__(self, resolution : Resolution,
                 upstream : SyncFilter,
                 suffix : Optional[str] = None,
                 literal : Optional[str] = None,
                 overwrite = False):
        self.suffix = suffix
        self.literal = literal
        self.upstream = upstream
        self.resolution = resolution
        self.overwrite = overwrite

    def _valid_ip(self, ip):
        try:
            ipaddress.ip_address(ip)
            return True
        except ValueError:
            return False

    def _match(self, h):
        if self.literal is not None and self.literal.lower() == h.lower():
            return True
        if self.suffix is not None and h.lower().endswith(self.suffix.lower()):
            return True
        return False

    def on_update(self,
                  tx : TransactionMetadata, tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:
        if (not self.overwrite and tx.resolution is not None) or (
                tx.resolution is None or not tx.resolution.hosts or
                not self._match(tx.resolution.hosts[0].host)):
            logging.debug('static resolution no overwrite %s', tx.resolution)
            return self.upstream.on_update(tx, tx_delta)

        logging.debug('static resolution %s -> %s',
                      tx.resolution, self.resolution)

        downstream_tx = tx.copy()
        downstream_tx.resolution = self.resolution
        downstream_delta = tx_delta.copy()
        if self.first:
            downstream_delta.resolution = self.resolution
            self.first = False
        return self.upstream.on_update(downstream_tx, downstream_delta)


if __name__ == '__main__':
    import sys
    for host in sys.argv[1:]:
        for a in resolve(HostPort(host, port=25)):
            print(a)
