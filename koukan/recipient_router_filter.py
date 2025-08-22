# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import (
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
from koukan.filter_chain import ProxyFilter, FilterResult

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


class RecipientRouterFilter(ProxyFilter):
    policy : RoutingPolicy
    dry_run : bool

    def __init__(self, policy : RoutingPolicy, dry_run = False):
        self.policy = policy
        self.dry_run = dry_run

    def _route(self, mailbox) -> Tuple[Optional[Response], bool]:
        tx = self.downstream_tx
        assert tx is not None
        assert self.upstream_tx is not None

        logging.debug('RecipientRouterFilter._route() %s', tx)
        assert mailbox is not None
        dest, resp = self.policy.endpoint_for_rcpt(mailbox.mailbox)

        if resp and resp.err():
            return resp, True
        elif dest is None:
            return None, False

        # for the exploder downstream chain, this is configured with
        # dry_run=True to skip setting routing results into the tx; we
        # just want it to reject invalid rcpts
        if self.dry_run:
            return None, True

        # in practice, in any output chain other than exploder
        # downstream, there will never be more that one rcpt but
        # multiple should work as long as they all have the same
        # routing results
        e = self.upstream_tx.rest_endpoint
        assert e is None or e == dest.rest_endpoint
        self.upstream_tx.rest_endpoint = dest.rest_endpoint

        res = None
        if dest.remote_host is not None:
            res = Resolution(dest.remote_host)
        up_res = self.upstream_tx.resolution
        assert up_res is None or up_res == res
        self.upstream_tx.resolution = res

        hh = self.upstream_tx.upstream_http_host
        assert hh is None or hh == dest.http_host
        self.upstream_tx.upstream_http_host = dest.http_host

        opt = self.upstream_tx.options
        assert opt is None or opt == dest.options
        self.upstream_tx.options = dest.options

        return None, True

    def on_update(self, tx_delta : TransactionMetadata) -> FilterResult:
        tx_delta.rcpt_to = []
        assert self.downstream_tx is not None
        assert self.upstream_tx is not None
        self.upstream_tx.merge_from(tx_delta)

        for i,rcpt in enumerate(self.downstream_tx.rcpt_to):
            assert rcpt is not None
            if (i < len(self.downstream_tx.rcpt_response) and
                self.downstream_tx.rcpt_response[i] is not None):
                continue
            # this may be chained multiple times; noop if a previous
            # instance already routed
            resp = None
            if not rcpt.routed:
                resp, rcpt.routed = self._route(rcpt)
            assert resp is None or resp.err()
            self.downstream_tx.rcpt_response.append(resp)

        return FilterResult()
