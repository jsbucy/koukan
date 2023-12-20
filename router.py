from typing import Any, Callable, Dict, Optional, Tuple, TypeAlias
from abc import ABC, abstractmethod

import logging

from response import Response, Esmtp

from blob import Blob, InlineBlob

from filter import Filter, HostPort, TransactionMetadata

import smtp_endpoint
import email.utils

class RoutingPolicy(ABC):
    # called on the first recipient in the transaction

    # -> rest endpoint base url, remote_host, Response
    # returns one of endpoint and possibly dest_host or response which
    # is probably a not-found error

    @abstractmethod
    def endpoint_for_rcpt(self, rcpt) -> Tuple[
            str, HostPort, Optional[Response]]:
        raise NotImplementedError


class Router(Filter):
    endpoint: Filter
    received_ascii : bytes = None
    policy : RoutingPolicy

    def __init__(self, policy : RoutingPolicy, next : Filter):
        self.policy = policy
        self.endpoint = next
        self.ehlo = "fixme.ehlo"

    def start(self,
              transaction_metadata : TransactionMetadata,
              mail_from, transaction_esmtp,
              rcpt_to, rcpt_esmtp):
        logging.debug('Router.start %s %s', mail_from, rcpt_to)

        received_host = ""
        if transaction_metadata.remote_host and transaction_metadata.remote_host.host:
            received_host = transaction_metadata.remote_host.host
        received = 'Received: from %s ([%s]);\r\n\t%s\r\n' % (
            self.ehlo, received_host,
            email.utils.format_datetime(email.utils.localtime()))
        self.received_ascii = received.encode('ascii')

        rest_endpoint, next_hop, resp = self.policy.endpoint_for_rcpt(rcpt_to)
        if resp and resp.err():
            return resp
        upstream_tx = TransactionMetadata()
        upstream_tx.rest_endpoint = rest_endpoint
        upstream_tx.remote_host = next_hop
        return self.endpoint.start(
            upstream_tx,
            mail_from, transaction_esmtp,
            rcpt_to, rcpt_esmtp)

    def append_data(self, last : bool, blob : Blob):
        if self.received_ascii:
            resp = self.endpoint.append_data(
                last=False, blob=InlineBlob(self.received_ascii))
            if resp.err():
                return resp
            self.received_ascii = None
        return self.endpoint.append_data(last, blob)

    def abort(self):
        if self.endpoint: self.endpoint.abort()
