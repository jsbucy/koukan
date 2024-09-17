import spf

from koukan.response import Response

from typing import List

# for using spf as an IP allowlist: return an error greeting for IPs that
# fail spf for a given domain

# maybe we need a metadata/env object that's shared up and down the
# chain so this can set an ip allowlist field and then the router can
# have code like
# add_rcpt():
#   if rcpt is not local and not auth and not ip allowlist
#     return (550, 'relaying denied')

# actually, these should chain like Envoy filters, just return
# Continue and the machinery will go on to the next endpoint in the
# chain until it gets a response
# None ~ Continue
# resp ~ StopIteration
class SpfEndpoint:
    domains : List[str]
    allowlist : List[str]
    def __init__(self, domains, allowlist, endpoint_factory):
        self.domains = domains
        self.allowlist = allowlist
        self.endpoint_factory = endpoint_factory

    def check(self, host):
        for h in self.allowlist:
            if host == h:
                return True
        for domain in self.domains:
            (result, code, desc) = spf.check(i=host, s=domain, h='')
            print(domain, result)
            if result.lower() == 'pass':
                return True
        return False

    def on_connect(self, remote_host, local_host):
        if not self.check(remote_host[0]):
            return Response(550, 'relaying denied')
        self.next = self.endpoint_factory()
        return self.next.on_connect(remote_host, local_host)

    def on_ehlo(self, hostname):
        return self.next.on_ehlo(hostname)

    def start_transaction(self, reverse_path, esmtp_options=None,
                          forward_path = None):
        return self.next.start_transaction(
            reverse_path, esmtp_options, forward_path)

    def add_rcpt(self, forward_path, esmtp_options=None):
        return self.next.add_rcpt(forward_path, esmtp_options)

    def append_data(self, last : bool, chunk_id : int, d : bytes = None):
        return self.next.append_data(last, chunk_id, d)

    def append_data_chunk(self, chunk_id, offset,
                          d : bytes, last : bool):
        return self.next.append_data_chunk(chunk_id, offset, d, last)

    def get_transaction_status(self):
        return self.next.get_transaction_status()
