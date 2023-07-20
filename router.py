
from response import Response, Esmtp

from typing import Dict, Optional

import smtp_endpoint
import email.utils

class Router:
    final_status : Optional[Response] = None
    next_chunk = None

    reverse_path : str
    transaction_esmtp_options : {}
    rcpt0 : str = None
    endpoint = None

    def __init__(self, policy):
        self.policy = policy

    def on_connect(self, remote_host, local_host):
        self.remote_host = remote_host
        self.local_host = local_host
        return Response(220, 'router ready')

    def on_ehlo(self, hostname):
        self.ehlo = hostname
        return Response(220, 'router at your service'), Esmtp()

    def setup_endpoint(self, rcpt):
        if self.endpoint:
            return self.policy.check_rcpt(self.rcpt0, rcpt)

        self.endpoint, resp = self.policy.endpoint_for_rcpt(rcpt)
        if resp.err():
            return resp
        self.rcpt0 = rcpt
        resp = self.endpoint.on_connect(None, None)
        if resp.err(): return resp
        # XXX esmtp
        resp, esmtp = self.endpoint.on_ehlo('router')
        return resp

    def start_transaction(self,
                          reverse_path, esmtp_options=None,
                          forward_path = None):
        self.reverse_path = reverse_path
        self.reverse_path_esmtp = esmtp_options
        self.transaction_esmtp_options = esmtp_options
        if not forward_path:
            return Response(250, "mail ok"), None

        # 1. find the first address that the routing policy accepts
        # 2. filter all of the remaining recipients that the policy will
        # simultaneously deliver to into valid_rcpt

        rcpt_status = []
        valid_rcpt = []
        rcpt_offset = []  # offset of valid_rcpt in forward_path
        for i,rcpt in enumerate(forward_path):
            resp = self.setup_endpoint(rcpt, forward_path)
            rcpt_status.append(resp)
            if resp.ok():
              rcpt_offset.append(i)
              valid_rcpt.append(rcpt)
        if not self.endpoint:
            return rcpt_status[0], rcpt_status
        upstream_resp, upstream_rcpt_status = self.endpoint.start_transaction(
            reverse_path, esmtp_options, valid_rcpt)
        if upstream_rcpt_status:
            assert(len(upstream_rcpt_status) == len(valid_rcpt) ==
                   len(rcpt_offset))
            for i,offset in enumerate(rcpt_offset):
              rcpt_status[offset] = upstream_rcpt_status[i]
        return upstream_resp, rcpt_status

    def add_rcpt(self, forward_path, esmtp_options=None):
        start = not self.endpoint
        resp = self.setup_endpoint(forward_path)
        if resp.err(): return resp
        if start:
            resp, _ = self.endpoint.start_transaction(
                self.reverse_path, self.reverse_path_esmtp, None)
            if resp.err():
                return resp
        resp = self.endpoint.add_rcpt(forward_path, esmtp_options)
        if resp.ok():
            self.received = False
        return resp

    # -> resp
    def append_data(self, last : bool, chunk_id = None, d : bytes = None):
        print('Router.append_data', last, chunk_id)

        if not self.endpoint:
            return Response(500, "router no rcpt"), None

        # probably don't need return-path for inbound-gw, def. not for relay

        if not self.received:
            received = 'Received: from %s ([%s]);\r\n\t%s\r\n' % (
                self.ehlo, self.remote_host,
                email.utils.format_datetime(email.utils.localtime()))
            received_ascii = received.encode('ascii')
            resp = self.endpoint.append_data(
                last=False, chunk_id=0, d=received_ascii)
            if resp.err():
                return resp
            self.received = True

        # we inserted an extra append_data() at the beginning for received
        return self.endpoint.append_data(last, chunk_id + 1, d)

    # -> (resp, len)
    def append_data_chunk(self, chunk_id, offset,
                          d : bytes, last: bool):
        if not self.endpoint:
            return Response(500, "router no rcpt")
        # we inserted an extra append_data() at the beginning for received
        resp,len = self.endpoint.append_data_chunk(
            chunk_id=(chunk_id + 1), offset=offset, d=d, last=last)
        print(resp, len)
        return resp, len

    # -> resp
    def get_transaction_status(self):
        print('Router.get_transaction_status')

        if not self.endpoint:
            return Response(500, "router no rcpt")

        if self.final_status is None:
            self.final_status = self.endpoint.get_transaction_status()
            self.endpoint = None
        return self.final_status


