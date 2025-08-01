# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeAlias
from abc import ABC, abstractmethod
import logging

from koukan.response import Response, Esmtp
from koukan.filter import (
    HostPort,
    Mailbox,
    Resolution,
    SyncFilter,
    TransactionMetadata )

class Destination:
    rest_endpoint : Optional[str] = None
    http_host : Optional[str] = None
    remote_host : Optional[List[HostPort]] = None
    options : dict

    def __init__(self, rest_endpoint : Optional[str] = None,
                 http_host : Optional[str] = None,
                 remote_host : Optional[List[HostPort]] = None,
                 options : Optional[dict] = None):
        self.rest_endpoint = rest_endpoint
        self.http_host = http_host
        self.remote_host = remote_host
        self.options = options if options else {}


class RoutingPolicy(ABC):
    # called on the first recipient in the transaction

    # Returns either a Destination or an error Response.
    # The error response is really to say "we were explicitly
    # configured to reject this address" vs "address syntax error"
    # Possibly there should be input validation near the beginning of
    # the chain to emit "501 5.1.3 Bad destination system address" in
    # that case.
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
               ) -> Optional[TransactionMetadata]:
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
        elif dest is None:
            return None

        dest_delta.rest_endpoint = dest.rest_endpoint
        if dest.remote_host is not None:
            dest_delta.resolution = Resolution(dest.remote_host)
        if dest.http_host is not None:
            dest_delta.upstream_http_host = dest.http_host
        dest_delta.options = dest.options
        logging.debug('RecipientRouterFilter._route() dest_delta %s',
                      dest_delta)
        return dest_delta

    def on_update(self, tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:
        routed = False
        if (tx.rest_endpoint is None and tx.options is None and
                self.dest_delta is None and tx_delta.rcpt_to):
            self.dest_delta = self._route(tx)
            # i.e. err
            if self.dest_delta is not None and self.dest_delta.rcpt_response:
                assert tx.merge_from(self.dest_delta) is not None
                return self.dest_delta
            routed = True

        if self.dest_delta is None:
            if self.upstream is None:
                return TransactionMetadata()
            return self.upstream.on_update(tx, tx_delta)
        # noop/heartbeat update after previous failure
        if self.dest_delta is not None and self.dest_delta.rcpt_response:
            return TransactionMetadata()

        # cf "filter chain" doc 2024/8/6, we can't add internal fields
        # to the downstream tx because it will cause a delta/conflict
        # when we do the next db read in the OutputHandler
        downstream_tx = tx.copy()
        downstream_delta = tx_delta.copy()
        assert downstream_tx.merge_from(self.dest_delta) is not None
        if routed:
            assert downstream_delta.merge_from(self.dest_delta) is not None
        upstream_delta = self.upstream.on_update(
            downstream_tx, downstream_delta)
        assert upstream_delta is not None
        assert tx.merge_from(upstream_delta) is not None
        return upstream_delta
