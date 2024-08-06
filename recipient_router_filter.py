from typing import Any, Callable, Dict, List, Optional, Tuple, TypeAlias
from abc import ABC, abstractmethod
import logging

from response import Response, Esmtp
from filter import (
    HostPort,
    Mailbox,
    SyncFilter,
    TransactionMetadata )

class Destination:
    rest_endpoint : Optional[str] = None
    remote_host : Optional[HostPort] = None
    options : dict

    def __init__(self, rest_endpoint : Optional[str] = None,
                 remote_host : Optional[HostPort] = None,
                 options : Optional[dict] = None):
        self.rest_endpoint = rest_endpoint
        self.remote_host = remote_host
        self.options = options if options else {}

class RoutingPolicy(ABC):
    # called on the first recipient in the transaction

    # returns either dest or resp (err)
    @abstractmethod
    def endpoint_for_rcpt(self, rcpt) -> Tuple[
            Optional[Destination], Optional[Response]]:
        raise NotImplementedError

class RecipientRouterFilter(SyncFilter):
    upstream: SyncFilter
    policy : RoutingPolicy
    dest_delta : Optional[TransactionMetadata] = None

    def __init__(self, policy : RoutingPolicy, upstream : SyncFilter):
        self.policy = policy
        self.upstream = upstream

    def _route(self, tx : TransactionMetadata
               ) -> TransactionMetadata:
        logging.debug('RecipientRouterFilter._route() %s', tx)
        mailbox = tx.rcpt_to[0]
        assert mailbox is not None
        dest, resp = self.policy.endpoint_for_rcpt(mailbox.mailbox)
        # TODO if we ever have multi-rcpt in the output chain, this
        # should validate that other mailboxes route to the same place
        dest_delta = TransactionMetadata()
        if resp and resp.err():
            if tx.mail_from and not tx.mail_response:
                dest_delta.mail_response = Response(
                    250, 'MAIL ok (RecipientRouterFilter')
            dest_delta.rcpt_response = [resp]
            return dest_delta

        dest_delta.rest_endpoint = dest.rest_endpoint
        dest_delta.remote_host = dest.remote_host
        dest_delta.options = dest.options
        logging.debug('RecipientRouterFilter._route() dest_delta %s', dest_delta)
        return dest_delta

    def on_update(self, tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:
        routed = False
        if self.dest_delta is None and tx.rcpt_to:
            self.dest_delta = self._route(tx)
            if self.dest_delta.rcpt_response:  # i.e. err
                tx.merge_from(self.dest_delta)
                return self.dest_delta
            routed = True

        if self.dest_delta is None:
            if self.upstream is None:
                return TransactionMetadata()
            return self.upstream.on_update(tx, tx_delta)

        # cf "filter chain" doc 2024/8/6, we can't add internal fields
        # to the downstream tx because it will cause a delta/conflict
        # when we do the next db read in the OutputHandler
        downstream_tx = tx.copy()
        downstream_delta = tx_delta.copy()
        downstream_tx.merge_from(self.dest_delta)
        if routed:
            downstream_delta.merge_from(self.dest_delta)
        upstream_delta = self.upstream.on_update(
            downstream_tx, downstream_delta)
        assert upstream_delta is not None
        tx.merge_from(upstream_delta)
        return upstream_delta
