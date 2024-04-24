import dns.resolver

from filter import HostPort

# This is a producer so we defer doing resolutions until the caller
# wants the answer. e.g. if a lower-priority mx is in a different
# domain and is slow/broken, we won't even resolve it unless all of the
# higher priority ones fail.
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
                yield(HostPort(aaa, hostport.port))
    return seen

if __name__ == '__main__':
    import sys
    for host in sys.argv[1:]:
        for a in resolve(HostPort(host, port=25)):
            print(a.host)
