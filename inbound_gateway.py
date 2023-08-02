import sys

from smtp_endpoint import SmtpEndpoint
from router import Router
from address_policy import AddressPolicy, PlusAddr
from local_domain_policy import LocalDomainPolicy
from smtp_service import service

port = int(sys.argv[1])

gsuite = lambda _: SmtpEndpoint('localhost', 2025, False) #'aspmx.l.google.com', 25, resolve_mx=True)

local_addrs = AddressPolicy(
    [ PrefixAddr('bucy', delimter='+', gsuite),
      PrefixAddr('pysmtpgw-discuss', delimter='-', local),
      PrefixAddr('restmtp-discuss', delimter='-', local),
     ] )
local_addr_router = lambda _: Router(local_addrs)
local_domains = LocalDomainPolicy(['sandbox.gloop.org'], local_addr_router)
local_domain_router = lambda: Router(local_domains)

#archive = FileSink()
#mirror = Mirror(archive, router)

service(local_domain_router, hostname="localhost", port=port)
