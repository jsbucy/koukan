
from response import Response, Esmtp

from typing import Callable, Dict, Optional, Tuple

import smtp_endpoint
import email.utils

class RoutingPolicy:
    # called on the first recipient in the transaction

    # this calls Endpoint.on_connect() before return, returned
    # Endpoint is ready for start_transaction()
    # -> Endpoint, dest host, Response
    def endpoint_for_rcpt(self, rcpt):
        pass


class Router:
    final_status : Optional[Response] = None
    endpoint = None
    received_ascii : bytes = None

    def __init__(self, policy : Callable[[str], Tuple["Endpoint", Response]]):
        self.policy = policy
        self.ehlo = "fixme.ehlo"

    def start(self,
              local_host, remote_host,
              mail_from, transaction_esmtp,
              rcpt_to, rcpt_esmtp):

        received_host = remote_host[0] if remote_host else ""
        received = 'Received: from %s ([%s]);\r\n\t%s\r\n' % (
            self.ehlo, received_host,
            email.utils.format_datetime(email.utils.localtime()))
        self.received_ascii = received.encode('ascii')

        self.endpoint, next_hop, resp = self.policy.endpoint_for_rcpt(rcpt_to)
        if resp and resp.err(): return resp
        return self.endpoint.start(
            None, next_hop,
            mail_from, transaction_esmtp,
            rcpt_to, rcpt_esmtp)

    def append_data(self, last : bool, d=None, blob_id=None):
        if self.received_ascii:
            if d is not None:
                d = self.received_ascii + d
            else:
                resp = self.endpoint.append_data(
                    last=False, d=received_ascii)
                if resp.err():
                    return resp, None
            self.received_ascii = None

        return self.endpoint.append_data(last, d, blob_id)


    def get_status(self):
        if self.final_status is None:
            self.final_status = self.endpoint.get_status()
        return self.final_status
