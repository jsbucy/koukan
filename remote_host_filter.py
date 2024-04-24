from typing import Optional
import dns.resolver

from filter import Filter, TransactionMetadata

class RemoteHostFilter(Filter):
    next : Optional[Filter]
    def __init__(self, next : Optional[Filter] = None):
        self.next = next

    def on_update(self, tx : TransactionMetadata,
                  timeout : Optional[float] = None):
        self._resolve(tx)
        if self.next:
            self.next.on_update(tx)

    def _resolve(self, tx : TransactionMetadata):
        if tx.remote_hostname is not None or tx.remote_host is None or (
                not tx.remote_host.host):
            return

        try:
            ans = dns.resolver.resolve_address(tx.remote_host.host)
            if ans is None or not ans[0].target:
                tx.remote_hostname = ''
                tx.fcrdns = False
                return

            tx.remote_hostname = str(ans[0].target)
            for rrtype in ['a', 'aaaa']:
                ans = dns.resolver.resolve(tx.remote_hostname, rrtype)
                for a in ans:
                    if str(a) == tx.remote_host.host:
                        tx.fcrdns = True
                        break
                else:
                    continue
                break
            else:
                tx.fcrdns = False

        except dns.resolver.NoAnswer:
            pass  # name exists but not that rrtype
        except dns.resolver.NXDOMAIN:
            pass  # name doesn't exist at all

        # dns.resolver.NoNameservers
        #   return Response(4xx)
        # fallthrough dns.exception.DNSException ?
        #   unexpected/internal error



    def abort(self):
        pass
