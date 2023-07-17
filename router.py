
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
            if not self.policy.check_rcpt(self.rcpt0, rcpt):
                return Response(
                    452, 'too many recipients (router policy check_rcpt)')
            return Response()

        self.endpoint = self.policy.endpoint_for_rcpt(rcpt)
        if not self.endpoint:
            return Response(550, 'router unknown address')
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

    # -> (resp, chunk_id)
    def append_data(self, last : bool, chunk_id = None):
        print('Router.append_data', last, chunk_id)

        # probably don't need return-path for inbound-gw, def. not for relay

        if not self.endpoint:
            return Response(500, "router no rcpt"), None

        if not self.received or not chunk_id:
            resp, id = self.endpoint.append_data(
                last=(False if chunk_id else last), chunk_id=None)

        if not self.received:
            received = 'Received: from %s ([%s]);\r\n\t%s\r\n' % (
                self.ehlo, self.remote_host,
                email.utils.format_datetime(email.utils.localtime()))
            received_ascii = received.encode('ascii')
            # something downstream may have added something else ahead of us
            resp, self.data_offset = self.endpoint.append_data_chunk(
                id, offset=0, d=received_ascii, last=(chunk_id is not None))
            if resp.err():
                return resp, None
            self.received = True
        if chunk_id:
            resp, id = self.endpoint.append_data(last, chunk_id)
        return resp, id

    # -> (resp, len)
    def append_data_chunk(self, chunk_id, offset,
                          d : bytes, last: bool):
        if not self.endpoint:
            return Response(500, "router no rcpt")
        # XXX this manipulation of len/offset only works because the
        # received header is added first, I think this doesn't work
        # correctly if stuff gets added later
        resp,len = self.endpoint.append_data_chunk(  
          chunk_id, offset + self.data_offset, d, last)
        print(resp, len)
        return resp, len - self.data_offset

    # -> resp
    def get_transaction_status(self):
        print('Router.get_transaction_status')

        if not self.endpoint:
            return Response(500, "router no rcpt")

        if self.final_status is None:
            self.final_status = self.endpoint.get_transaction_status()
            self.endpoint = None
        return self.final_status


