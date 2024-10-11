# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import dns.resolver
import logging

NotFoundExceptions = (
    dns.resolver.NoAnswer,  # name exists but not that rrtype
    dns.resolver.NXDOMAIN   # name doesn't exist at all
)
ServFailExceptions = (
    dns.resolver.NoNameservers,   # All nameservers failed to answer the query.
    dns.resolver.LifetimeTimeout  # timed out
)

# dns.exception.DNSException is the base class of dns exceptions, many
# of those are effectively "invalid argument" i.e. a bug in this
# code/unexpected

class Resolver:
    def __init__(self):
        pass
    def resolve_address(self, addr):
        return dns.resolver.resolve_address(addr)
    def resolve(self, host, rrtype):
        return dns.resolver.resolve(host, rrtype)
