
from typing import Any, Callable, List, Optional, Tuple

from response import Response, Esmtp
import mx_resolution

from blob import Blob

import logging

class MxResolutionEndpoint:
    def __init__(self, transaction_factory : Callable[[], Any]):  # Endpoint
        self.transaction_factory = transaction_factory
        self.next = None

    # forward_path : [ (rcpt, esmtp) ]
    # -> (resp, rcpt_status)
    def start(
            self,
            local_host, remote_host_port,
            mail_from, transaction_esmtp=None, rcpt_to = None, rcpt_esmtp=None
            ) -> Response:

        (remote_host, remote_port) = remote_host_port
        logging.info('MxResoultionEndpoint.start %s', remote_host)
        for host in mx_resolution.resolve(remote_host):
            logging.info('MxResoultionEndpoint.start mx %s', host)
            self.next = self.transaction_factory()
            resp = self.next.start(
                local_host, (host, remote_port), mail_from, transaction_esmtp,
                rcpt_to, rcpt_esmtp)
            if not resp.temp():
                return resp
        return Response(400, 'MxResolutionEndpoint.start all MXes failed')

    def append_data(self, last : bool, blob : Blob) -> Response:
        return self.next.append_data(last, blob)
