from typing import Any, Callable, Dict, List, Optional, Tuple, TypeAlias
from abc import ABC, abstractmethod
import logging

from response import Response, Esmtp
from filter import Filter, HostPort, Mailbox, TransactionMetadata


class RoutingPolicy(ABC):
    # called on the first recipient in the transaction

    # -> rest endpoint base url, remote_host, Response
    # returns one of endpoint and possibly dest_host or response which
    # is probably a not-found error
    @abstractmethod
    def endpoint_for_rcpt(self, rcpt) -> Tuple[
            Optional[str], Optional[HostPort], Optional[Response]]:
        raise NotImplementedError


class RecipientRouterFilter(Filter):
    endpoint: Filter
    policy : RoutingPolicy
    upstream_tx : Optional[TransactionMetadata] = None

    def __init__(self, policy : RoutingPolicy, next : Filter):
        self.policy = policy
        self.endpoint = next
        self.rcpt_to = []

    def _route(self, tx : TransactionMetadata):
        rest_endpoint, next_hop, resp = self.policy.endpoint_for_rcpt(
            tx.rcpt_to[0].mailbox)
        # TODO validate that other mailboxes route to the same place
        # else -> internal error?
        if resp and resp.err():
            if tx.mail_from:
                tx.mail_response = Response(
                    250, 'MAIL ok (RecipientRouterFilter')
            tx.rcpt_response = [resp]
            return
        self.upstream_tx = TransactionMetadata()
        self.upstream_tx.rest_endpoint = rest_endpoint
        self.upstream_tx.remote_host = next_hop

    def on_update(self, tx : TransactionMetadata):
        logging.debug('Router.start %s %s', tx.mail_from, tx.rcpt_to)

        # xxx "buffer mail" return 250 if mail no rcpt

        if self.upstream_tx is None and tx.rcpt_to:
            self._route(tx)
            if tx.rcpt_response:
                return
        else:
            self.upstream_tx = TransactionMetadata()

        self.upstream_tx.mail_from = tx.mail_from
        self.upstream_tx.rcpt_to = tx.rcpt_to
        self.upstream_tx.body_blob = tx.body_blob

        self.endpoint.on_update(self.upstream_tx)
        tx.mail_response = self.upstream_tx.mail_response
        tx.rcpt_response = self.upstream_tx.rcpt_response
        tx.data_response = self.upstream_tx.data_response


    def abort(self):
        if self.endpoint: self.endpoint.abort()
