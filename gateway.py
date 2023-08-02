import sys

from rest_endpoint import RestEndpoint
from smtp_endpoint import SmtpEndpoint

from blobs import BlobStorage, BlobDerefEndpoint
from smtp_service import service
import rest_service
import gunicorn_main

rest_port = int(sys.argv[1])
router_port = int(sys.argv[2])
mx_port = int(sys.argv[3])
msa_port = int(sys.argv[4])
cert = sys.argv[5]
key = sys.argv[6]
auth_secrets = sys.argv[7]
ehlo_host = sys.argv[8]

### smtp -> rest

router_base_url = 'http://localhost:%d/' % router_port

inbound_gw_factory = (
    lambda: RestEndpoint(router_base_url, http_host='inbound-gw'))


service(inbound_gw_factory, port=mx_port,
        cert=cert, key=key,
        msa=False,
        max_rcpt=1)

outbound_gw_factory = (
    lambda: RestEndpoint(router_base_url, http_host='outbound-gw'))

service(outbound_gw_factory, port=msa_port,
        cert=cert, key=key,
        msa=True,
        auth_secrets_path=auth_secrets,
        max_rcpt=1)


### rest -> smtp

blobs = BlobStorage()

def endpoints(host):
    if host == 'outbound':
        return BlobDerefEndpoint(blobs, SmtpEndpoint(ehlo_hostname=ehlo_host))
    else:
        return None

gunicorn_main.run('localhost', rest_port, cert, key,
                  rest_service.create_app(endpoints, blobs))
