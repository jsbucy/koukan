import sys

from smtp_endpoint import SmtpEndpoint
from spf_endpoint import SpfEndpoint
from router import Router
from dest_domain_policy import DestDomainPolicy
from smtp_service import service

from dkim_endpoint import DkimEndpoint

port = int(sys.argv[1])

policy = DestDomainPolicy(
    lambda domain: SmtpEndpoint(domain, port=2025, resolve_mx=False))
router = lambda: Router(policy)

dkim = lambda: DkimEndpoint(
    b'sandbox.gloop.org', b'tachygraph20230720', '/tmp/dk.key',
    next=router())

spf = lambda: SpfEndpoint(['google.com'], ['127.0.0.1'], endpoint_factory=dkim)

service(spf, port=port,
        #cert="cert.pem", key="key.pem",
        #auth_secrets_path=sys.argv[2]
        )
