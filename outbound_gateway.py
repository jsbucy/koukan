import sys

from smtp_endpoint import SmtpEndpoint
from spf_endpoint import SpfEndpoint
from router import Router
from dest_domain_policy import DestDomainPolicy
from smtp_service import service

port = int(sys.argv[1])

policy = DestDomainPolicy(
    lambda domain: SmtpEndpoint(domain, port=25, resolve_mx=True))
router = lambda: Router(policy)
spf = lambda: SpfEndpoint(['google.com'], ['127.0.0.1'], router)

service(spf, port=port)
