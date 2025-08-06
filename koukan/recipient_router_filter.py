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
    TransactionMetadata )
from koukan.filter_chain import Filter

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


class RecipientRouterFilter(Filter):
    policy : RoutingPolicy
    done = False
    err = False

    def __init__(self, policy : RoutingPolicy):
        self.policy = policy

    def _route(self):
        tx = self.downstream
        logging.debug('RecipientRouterFilter._route() %s', tx)
        mailbox = tx.rcpt_to[0]
        assert mailbox is not None
        dest, resp = self.policy.endpoint_for_rcpt(mailbox.mailbox)
        logging.debug('%s %s', dest, resp)

        # TODO if we ever have multi-rcpt in the output chain, this
        # should validate that other mailboxes route to the same place
        if resp and resp.err():
            if tx.mail_from and tx.mail_response is None:
                tx.mail_response = Response(
                    250, 'MAIL ok (RecipientRouterFilter)')
            tx.rcpt_response = [resp]
            self.err = True
            return
        elif dest is None:
            return

        tx.rest_endpoint = dest.rest_endpoint
        if dest.remote_host is not None:
            tx.resolution = Resolution(dest.remote_host)
        if dest.http_host is not None:
            tx.upstream_http_host = dest.http_host
        tx.options = dest.options

    async def on_update(self, tx_delta : TransactionMetadata, upstream):
        logging.debug(self.downstream)
        if self.err:
            return
        if (self.downstream.rest_endpoint is None and
            self.downstream.options is None and
            not self.done and
            tx_delta.rcpt_to):
            self._route()
            self.done = True
            logging.debug(self.downstream)
            if self.err:
                return
        await upstream()
