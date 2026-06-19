# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import (
    Any,
    Awaitable,
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
from koukan.sender import Sender

class Destination:
    rest_endpoint : Optional[str] = None
    rest_upstream_sender : Optional[Sender] = None
    remote_host : Optional[List[HostPort]] = None
    options : dict

    def __init__(self, rest_endpoint : Optional[str] = None,
                 rest_upstream_sender : Optional[Sender] = None,
                 remote_host : Optional[List[HostPort]] = None,
                 options : Optional[dict] = None):
        self.rest_endpoint = rest_endpoint
        self.rest_upstream_sender = rest_upstream_sender
        self.remote_host = remote_host
        self.options = options if options else {}


class RoutingPolicy(ABC):
    # called on the first recipient in the transaction

    # Returns either a Destination or an error Response.

    # The error response is really to say "we were explicitly
    # configured to reject this address" vs "address syntax error"
    # Explicitly reject invalid envelope addresses at the beginning of
    # each chain with policy_action matcher invalid_mail_from/rcpt_to.
    @abstractmethod
    def endpoint_for_rcpt(self, rcpt : Mailbox) -> Tuple[
            Optional[Destination], Optional[Response]]:
        raise NotImplementedError


class RecipientRouterFilter(CoroutineProxyFilter):
    policy : Optional[RoutingPolicy] = None
    static_dest : Optional[Destination] = None
    dry_run : bool
    downstream_mail : Optional[Mailbox] = None

    # upstream_tx.rcpt_to[i] == downstream_tx[rcpt_offset[i]]
    # upstream -> downstream
    rcpt_offset : List[int]

    def __init__(self, policy : Optional[RoutingPolicy], dry_run = False,
                 static_dest : Optional[Destination] = None):
        self.policy = policy
        self.dry_run = dry_run
        self.static_dest = static_dest
        self.rcpt_offset = []

    def _route(self, mailbox) -> Tuple[Optional[Response], bool]:
        tx = self.downstream_tx
        assert tx is not None
        assert self.upstream_tx is not None

        logging.debug('RecipientRouterFilter._route() %s', tx)
        assert mailbox is not None
        if self.policy is not None:
            dest, resp = self.policy.endpoint_for_rcpt(mailbox.mailbox)
        else:
            dest = self.static_dest
            resp = None if dest is not None else Response(
                550, '5.1.1 mailbox does not exist '
                '(RecipientRouterFilter null policy)')

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

        self.upstream_tx.rest_upstream_sender = dest.rest_upstream_sender

        opt = self.upstream_tx.options
        assert opt is None or opt == dest.options
        self.upstream_tx.options = dest.options

        return None, True

    async def on_update(
            self, tx_delta : TransactionMetadata,
            upstream : Callable[[], Awaitable[TransactionMetadata]]
    ) -> None:
        assert self.downstream_tx is not None
        assert self.upstream_tx is not None

        if (tx_delta.mail_from is not None) and (tx_delta.mail_response is None) and not tx_delta.rcpt_to:
            logging.debug('save mail')
            self.downstream_mail = tx_delta.mail_from
            tx_delta.mail_from = None
            self.upstream_tx.merge_from(tx_delta)
            assert self.downstream_tx.merge_from(await upstream()) is not None
            self.downstream_tx.mail_response = Response(
                250, 'mail ok (rcpt router noop)')
            return

        buffered_mail = False
        if self.downstream_mail is not None and tx_delta.rcpt_to:
            buffered_mail = True
            logging.debug('restore mail')
            tx_delta.mail_from = self.downstream_mail
            self.downstream_mail = None

        tx_delta.rcpt_to = []

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
            if resp is None:
                self.rcpt_offset.append(i)
                self.upstream_tx.rcpt_to.append(rcpt)

        logging.debug(self.downstream_tx)

        # xxx only if some rcpt succeeded
        upstream_delta = await upstream()

        logging.debug(self.upstream_tx)
        logging.debug(upstream_delta)
        for i in range(0, len(self.upstream_tx.rcpt_response)):
            if self.downstream_tx.rcpt_response[self.rcpt_offset[i]] is not None:
                continue
            # if we previously sent a noop mail_response and then got a
            # mail error from upstream, report that in rcpt_response.
            if buffered_mail and (mail_resp := upstream_delta.mail_response) is not None and mail_resp.err():
                self.downstream_tx.rcpt_response[self.rcpt_offset[i]] = mail_resp
            else:
                self.downstream_tx.rcpt_response[self.rcpt_offset[i]] = self.upstream_tx.rcpt_response[i]


            upstream_delta.rcpt_response = []
        assert self.downstream_tx.merge_from(upstream_delta) is not None
        logging.debug(self.downstream_tx)
