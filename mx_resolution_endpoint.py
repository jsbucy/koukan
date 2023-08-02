

from typing import Any, Callable, List, Optional, Tuple

from response import Response, Esmtp
import mx_resolution

class MxResolutionEndpoint:
    def __init__(self, next : Callable[[], "Endpoint"]):
        self.next = next

    # forward_path : [ (rcpt, esmtp) ]
    # -> (resp, rcpt_status)
    def start(
            self,
            local_host, remote_host,
            mail_from, transaction_esmtp=None, rcpt_to = None, rcpt_esmtp=None
            ) -> Tuple[Response,List[Tuple[str, Any]]]:

        for host in mx_resolution.resolve(remote_host[0]):
            self.endpoint = self.next()
            resp = self.endpoint.start(
                local_host, host, mail_from, transaction_esmtp,
                rcpt_to, rcpt_esmtp)
            if not resp.temp():
                return resp
            return Response(400, 'MxResolutionEndpoint.start all MXes failed')

    def append_data(self, last : bool, d : bytes = None, blob_id = None
                    ) -> Response:
        return self.endpoint.append_data(last, d, blob_id)

    def get_status(self) -> Response:
        return self.endpoint.get_status()
