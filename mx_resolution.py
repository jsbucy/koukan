from typing import List, Optional
import logging

from dns_wrapper import Resolver, NotFoundExceptions
import ipaddress

from filter import HostPort, Resolution, SyncFilter, TransactionMetadata

# TODO: need more sophisticated timeout handling? cumulative timeout rather
# than per-lookup?
def resolve(resolver, hostport : HostPort):
    try:
        answers = resolver.resolve(hostport.host, 'MX')
        answers = sorted(answers, key=lambda x: x.preference)
        mxen = [ mx.exchange for mx in answers]
    except NotFoundExceptions:  # XXX ServFailExceptions? (and below)
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
                a = resolver.resolve(mx, rrtype)
            except NotFoundExceptions:
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

    def _needs_resolution(self, res) -> bool:
        if res is None:
            return False
        return any([not self._valid_ip(h.host) and self._match(h.host)
                    for h in res.hosts])

    def _match(self, h):
        if self.literal is not None and self.literal.lower() == h.lower():
            return True
        if self.suffix is not None and h.lower().endswith(self.suffix.lower()):
            return True
        return False


    def _resolve(self, res : Resolution) -> List[HostPort]:
        hosts_out = []
        for h in res.hosts:
            if not self._match(h.host):
                hosts_out.append(h)
                continue
            if self.static_resolution is not None:
                hosts_out.extend(self.static_resolution.hosts)
                continue
            dns_hosts = resolve(self.resolver, h)
            hosts_out.extend([HostPort(hh, h.port) for hh in dns_hosts])
        return hosts_out

    def on_update(self,
                  tx : TransactionMetadata, tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:
        logging.debug('mx resolution tx %s', tx)
        logging.debug('mx resolution delta %s', tx_delta)
        resolution = None
        if (self.resolution is None and
            self._needs_resolution(tx_delta.resolution)):
            resolution = Resolution(self._resolve(tx_delta.resolution))
            self.resolution = resolution

        if self.resolution is None:
            return self.upstream.on_update(tx, tx_delta)

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
