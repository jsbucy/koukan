from typing import Any, Callable, Dict, List, Optional, Tuple, TypeAlias
from abc import ABC, abstractmethod
import logging

from response import Response, Esmtp
from filter import (
    HostPort,
    Mailbox,
    SyncFilter,
    TransactionMetadata )

class RoutingPolicy(ABC):
    # called on the first recipient in the transaction

    # -> rest endpoint base url, remote_host, Response
    # returns one of endpoint and possibly dest_host or response which
    # is probably a not-found error
    @abstractmethod
    def endpoint_for_rcpt(self, rcpt) -> Tuple[
            Optional[str], Optional[HostPort], Optional[Response]]:
        raise NotImplementedError


class RecipientRouterFilter(SyncFilter):
    upstream: SyncFilter
    policy : RoutingPolicy
    upstream_tx : Optional[TransactionMetadata] = None

    def __init__(self, policy : RoutingPolicy, upstream : SyncFilter):
        self.policy = policy
        self.upstream = upstream

    def _route(self, downstream_delta : TransactionMetadata
               ) -> Optional[TransactionMetadata]:
        logging.debug('RecipientRouterFilter._route() %s %s',
                      self.upstream_tx.mail_from, self.upstream_tx.rcpt_to)
        mailbox = self.upstream_tx.rcpt_to[0]
        assert mailbox is not None
        rest_endpoint, next_hop, resp = self.policy.endpoint_for_rcpt(
            mailbox.mailbox)
        # TODO if we ever have multi-rcpt in the output chain, this
        # should validate that other mailboxes route to the same place
        if resp and resp.err():
            upstream_delta = TransactionMetadata()
            if self.upstream_tx.mail_from:
                upstream_delta.mail_response = Response(
                    250, 'MAIL ok (RecipientRouterFilter')
            upstream_delta.rcpt_response = [resp]
            return upstream_delta

        downstream_delta.rest_endpoint = rest_endpoint
        self.upstream_tx.rest_endpoint = rest_endpoint
        downstream_delta.remote_host = self.upstream_tx.remote_host = next_hop


    def on_update(self, tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:
        if self.upstream_tx is None:
            self.upstream_tx = tx.copy()
        else:
            assert self.upstream_tx.merge_from(tx_delta) is not None

        downstream_delta = tx_delta.copy()

        upstream_delta = None
        if len(tx.rcpt_to) == len(tx_delta.rcpt_to) == 1:
            upstream_delta = self._route(downstream_delta)

        if upstream_delta is None:
            upstream_delta = self.upstream.on_update(
                self.upstream_tx, downstream_delta)
        assert tx.merge_from(upstream_delta) is not None
        return upstream_delta
