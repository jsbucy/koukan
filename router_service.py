
import rest_service
import gunicorn_main

from address_policy import AddressPolicy, PrefixAddr
from blobs import BlobStorage, BlobDerefEndpoint
from local_domain_policy import LocalDomainPolicy
from dest_domain_policy import DestDomainPolicy
from router import Router
from rest_endpoint import RestEndpoint
from dkim_endpoint import DkimEndpoint

from mx_resolution_endpoint import MxResolutionEndpoint

import sys

rest_port = int(sys.argv[1])
gateway_port = int(sys.argv[2])
cert = sys.argv[3]
key = sys.argv[4]
dkim_key = sys.argv[5]
dkim_domain = sys.argv[6].encode('ascii')
dkim_selector = sys.argv[7].encode('ascii')

gw_base_url = 'http://localhost:%d/' % gateway_port


blobs = BlobStorage()


### inbound

# AddressPolicy passes the rcpt to the endpoint factory
outbound_host = lambda _: RestEndpoint(
    gw_base_url, http_host='outbound',
    static_remote_host=('127.0.0.1', 3025))  #'aspmx.l.google.com')

local_addrs = AddressPolicy(
    [ PrefixAddr('u', delimiter='+', endpoint_factory=outbound_host),
      PrefixAddr('v', delimiter='+', endpoint_factory=outbound_host),
      ])
local_addr_router = lambda: Router(local_addrs)
local_domains = LocalDomainPolicy({'d': local_addr_router})
local_domain_router = lambda: Router(local_domains)

### outbound
outbound_mx = lambda: RestEndpoint(
    gw_base_url, http_host='outbound', static_remote_host=('127.0.0.1', 3025))
mx_resolution = lambda: MxResolutionEndpoint(outbound_mx)
next = mx_resolution

if dkim_key:
    print('enabled dkim signing', dkim_key)
    next = lambda: DkimEndpoint(dkim_domain, dkim_selector, dkim_key,
                                mx_resolution())

dest_domain_policy = DestDomainPolicy(next)
dest_domain_router = lambda: Router(dest_domain_policy)

blob_deref = lambda next: BlobDerefEndpoint(blobs, next)


def endpoints(host):
    if host == 'inbound-gw':
        return blob_deref(local_domain_router())
    elif host == 'outbound-gw':
        return blob_deref(dest_domain_router())
    else:
        return None

# top-level: http host -> endpoint

gunicorn_main.run('localhost', rest_port, cert, key,
                  rest_service.create_app(endpoints, blobs))
