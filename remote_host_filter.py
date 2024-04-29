from typing import Optional
import dns.resolver
import logging

from filter import Filter, TransactionMetadata

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

class RemoteHostFilter(Filter):
    next : Optional[Filter]
    def __init__(self, next : Optional[Filter] = None):
        self.next = next

    def on_update(self, tx : TransactionMetadata,
                  timeout : Optional[float] = None):
        if tx.mail_from is not None:
            self._resolve(tx)
        if self.next:
            self.next.on_update(tx)

    def _resolve(self, tx : TransactionMetadata):
        if tx.remote_host is None or not tx.remote_host.host:
            return

        try:
            ans = dns.resolver.resolve_address(tx.remote_host.host)
        except _ServFailExceptions:
            tx.mail_response = Response(450, 'RemoteHostFilter ptr err')
            return
        except _NotFoundExceptions:
            pass

        if ans is None or not ans[0].target:
            tx.remote_hostname = ''
            tx.fcrdns = False
            return
        tx.remote_hostname = str(ans[0].target)

        all_failed = True
        for rrtype in ['a', 'aaaa']:
            try:
                ans = dns.resolver.resolve(tx.remote_hostname, rrtype)
                all_failed = False
            except _ServFailExceptions:
                pass
            except _NotFoundExceptions:
                all_failed = False

            if ans is None:
                continue
            for a in ans:
                logging.debug('%s %s', str(a), tx.remote_host.host)
                if str(a) == tx.remote_host.host:
                    tx.fcrdns = True
                    break
            if tx.fcrdns:
                break
        else:
            tx.fcrdns = False

        if all_failed:
            tx.mail_response = Response(450, 'RemoteHostFilter fwd err')


    def abort(self):
        pass
