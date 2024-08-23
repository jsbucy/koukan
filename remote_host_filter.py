from typing import Optional, Tuple
import dns.resolver
import logging

from filter import (
    SyncFilter,
    TransactionMetadata )
from response import Response

_NotFoundExceptions = (
    dns.resolver.NoAnswer,  # name exists but not that rrtype
    dns.resolver.NXDOMAIN   # name doesn't exist at all
)
_ServFailExceptions = (
    dns.resolver.NoNameservers,   # All nameservers failed to answer the query.
    dns.resolver.LifetimeTimeout  # timed out
)

# dns.exception.DNSException is the base class of dns exceptions, many
# of those are effectively "invalid argument" i.e. a bug in this
# code/unexpected

# TODO merge with dns_wrapper
class Resolver:
    def __init__(self):
        pass
    def resolve_address(self, addr):
        return dns.resolver.resolve_address(addr)
    def resolve(self, host, rrtype):
        return dns.resolver.resolve(host, rrtype)

class RemoteHostFilter(SyncFilter):
    upstream : SyncFilter
    upstream_tx : Optional[TransactionMetadata] = None

    def __init__(self, upstream : SyncFilter,
                 resolver=None):
        self.upstream = upstream
        self.resolver = resolver if resolver else Resolver()

    def on_update(self, tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:
        if self.upstream_tx is None:
            self.upstream_tx = tx.copy()
        else:
            assert self.upstream_tx.merge_from(tx_delta) is not None

        downstream_delta = tx_delta.copy()

        upstream_delta = None
        if tx_delta.mail_from is not None:
            err,res = self._resolve(downstream_delta)
            if err is None and res is not None:
                assert downstream_delta.merge_from(res) is not None
                assert self.upstream_tx.merge_from(res) is not None
            else:
                upstream_delta = err

        if upstream_delta is None or upstream_delta.mail_response is None:
            upstream_delta = self.upstream.on_update(
                self.upstream_tx, downstream_delta)
        assert tx.merge_from(upstream_delta) is not None
        return upstream_delta

    def _resolve(self, downstream_delta : TransactionMetadata
                 ) -> Tuple[Optional[TransactionMetadata],  # err
                            # added fields to send upstream
                            Optional[TransactionMetadata]]:
        if (self.upstream_tx.remote_host is None or
            not self.upstream_tx.remote_host.host):
            return None, None

        ans = None
        try:
            ans = self.resolver.resolve_address(
                self.upstream_tx.remote_host.host)
        except _ServFailExceptions:
            return TransactionMetadata(
                mail_response = Response(450, 'RemoteHostFilter ptr err')), None
        except _NotFoundExceptions:
            pass

        res = TransactionMetadata()
        if not(ans) or not ans[0].target:
            res.remote_hostname = ''
            res.fcrdns = False
            return None, res
        res.remote_hostname = str(ans[0].target)

        all_failed = True
        for rrtype in ['a', 'aaaa']:
            ans = None
            try:
                ans = self.resolver.resolve(
                    res.remote_hostname, rrtype)
                all_failed = False
            except _ServFailExceptions:
                pass
            except _NotFoundExceptions:
                all_failed = False

            if ans is None:
                continue
            for a in ans:
                logging.debug('RemoteHostFilter._resolve %s %s %s',
                              rrtype, str(a), downstream_delta.remote_host.host)
                if str(a) == downstream_delta.remote_host.host:
                    res.fcrdns = True
                    break
            if res.fcrdns:
                break
        else:
            res.fcrdns = False

        if all_failed:
            return TransactionMetadata(
                mail_response = Response(450, 'RemoteHostFilter fwd err')), None

        logging.debug('RemoteHostFilter._resolve() '
                      'remote_hostname=%s fcrdns=%s',
                      res.remote_hostname, res.fcrdns)
        return None, res

    def abort(self):
        pass
