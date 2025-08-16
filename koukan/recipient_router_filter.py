# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import (
    Awaitable,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple )
from abc import ABC, abstractmethod
import logging

from koukan.response import Response, Esmtp
from koukan.filter import (
    HostPort,
    Mailbox,
    Resolution,
    TransactionMetadata )
from koukan.filter_chain import CoroutineProxyFilter, FilterResult

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


class RecipientRouterFilter(CoroutineProxyFilter):
    policy : RoutingPolicy
    # upstream_tx.rcpt_to[i] == downstream_tx[upstream_rcpt[i]]
    upstream_rcpt : List[int]

    def __init__(self, policy : RoutingPolicy):
        self.policy = policy
        self.upstream_rcpt = []

    def _route(self, mailbox) -> Optional[Response]:
        tx = self.downstream_tx
        logging.debug('RecipientRouterFilter._route() %s', tx)
        assert mailbox is not None
        dest, resp = self.policy.endpoint_for_rcpt(mailbox.mailbox)
        logging.debug(dest)
        logging.debug(resp)

        if resp and resp.err():
            # xxx this can potentially keep going?
            # if tx.mail_from and tx.mail_response is None:
            #     tx.mail_response = Response(
            #         250, 'MAIL ok (RecipientRouterFilter)')
            return resp
        elif dest is None:
            return None

        # XXX err if any of this doesn't match previous.  Eventually
        # this becomes per-rcpt and the terminal/sink filter can sort
        # that out.
        self.upstream_tx.rest_endpoint = dest.rest_endpoint
        if dest.remote_host is not None:
            self.upstream_tx.resolution = Resolution(dest.remote_host)
        if dest.http_host is not None:
            self.upstream_tx.upstream_http_host = dest.http_host
        self.upstream_tx.options = dest.options
        return None

    async def on_update(
            self, tx_delta : TransactionMetadata,
            upstream : Callable[[], Awaitable[TransactionMetadata]]):
        rcpt_to = tx_delta.rcpt_to
        tx_delta.rcpt_to = []

        # xxx drop body if no good rcpt?

        for i,rcpt in enumerate(rcpt_to):
            off = i + tx_delta.rcpt_to_list_offset
            # this may be chained multiple times; noop if a previous
            # instance already routed
            resp = None
            if (self.downstream_tx.rest_endpoint is None and
                self.downstream_tx.options is None):
                resp = self._route(rcpt)
            assert resp is None or resp.err()
            self.downstream_tx.rcpt_response.append(resp)
            if resp is None:
                tx_delta.rcpt_to.append(rcpt)
                self.upstream_rcpt.append(i)

        self.upstream_tx.merge_from(tx_delta)
        upstream_delta = await upstream()
        upstream_rcpt_response = upstream_delta.rcpt_response
        upstream_delta.rcpt_response = None
        self.downstream_tx.merge_from(upstream_delta)
        logging.debug(self.upstream_tx)
        for i,resp in enumerate(upstream_rcpt_response):
            self.downstream_tx.rcpt_response[self.upstream_rcpt[i]] = resp
