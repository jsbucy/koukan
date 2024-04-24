from typing import Any, Callable, Dict, List, Optional, Tuple, TypeAlias
from abc import ABC, abstractmethod
import logging
from datetime import datetime

import email.utils

from response import Response, Esmtp
from blob import Blob, InlineBlob, CompositeBlob
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
    inject_time : Optional[datetime] = None
    received_hostname : Optional[str] = None

    smtp_meta : Optional[dict] = None
    mail_from : Optional[Mailbox] = None
    rcpt_to : List[Mailbox]

    def __init__(self, policy : RoutingPolicy, next : Filter,
                 received_hostname : Optional[str] = None,
                 inject_time = None):
        self.policy = policy
        self.endpoint = next
        self.inject_time = inject_time
        self.received_hostname = received_hostname
        self.rcpt_to = []

    def _route(self, tx : TransactionMetadata):
        rest_endpoint, next_hop, resp = self.policy.endpoint_for_rcpt(
            tx.rcpt_to[0].mailbox)
        # TODO validate that other mailboxes route to the same place
        # else -> internal error?
        if resp and resp.err():
            tx.rcpt_response = [resp]
            return
        self.upstream_tx = TransactionMetadata()
        self.upstream_tx.rest_endpoint = rest_endpoint
        self.upstream_tx.remote_host = next_hop

# Received: from a48-180.smtp-out.amazonses.com
#  (a48-180.smtp-out.amazonses.com. [54.240.48.180])
#  by mx.google.com with ESMTPS id iu13-20020ad45ccd000000b0068ca87d31f1si2255553qvb.592.2024.02.07.14.47.24
#  for <alice@example.com>
#  (version=TLS1_2 cipher=ECDHE-ECDSA-AES128-GCM-SHA256 bits=128/128);
#  Wed, 07 Feb 2024 14:47:24 -0800 (PST)

    def _format_received(self, tx : TransactionMetadata) -> str:
        received_host = ''
        if tx.remote_host and tx.remote_host.host:
            received_host = '[' + tx.remote_host.host + ']'
            if tx.remote_hostname and tx.fcrdns:
                received_host = tx.remote_hostname + ' ' + received_host

        datetime = (self.inject_time if self.inject_time
                    else email.utils.localtime())

        ehlo = None
        esmtp = ''
        if self.smtp_meta is not None:
            ehlo = self.smtp_meta.get('ehlo_host', None)
            if self.mail_from.esmtp and 'SMTPUTF8' in self.mail_from.esmtp:
                esmtp = 'UTF8SMTP'
            elif self.smtp_meta.get('esmtp', False):
                esmtp = 'ESMTP'
            else:
                esmtp = 'SMTP'
            if self.smtp_meta.get('tls', False):
                esmtp += 'S'
            if self.smtp_meta.get('auth', False):
                esmtp += 'A'
            esmtp = '\r\n\twith ' + esmtp

        if ehlo is None:
            # ick do rest clients need to provide this?
            ehlo = '[' + received_host + ']'

        by_host = ''
        if self.received_hostname:
            by_host = '\r\n\tby ' + self.received_hostname

        for_mailbox = ''
        if len(self.rcpt_to) == 1:
            for_mailbox = '\r\n\tfor ' + self.rcpt_to[0].mailbox

        received = 'Received: from %s (%s)%s%s%s;\r\n\t%s\r\n' % (
            ehlo, received_host, by_host, esmtp, for_mailbox,
            email.utils.format_datetime(datetime))
        return received

    def on_update(self, tx : TransactionMetadata):
        logging.debug('Router.start %s %s', tx.mail_from, tx.rcpt_to)

        if tx.smtp_meta:
            self.smtp_meta = tx.smtp_meta
        if tx.mail_from:
            self.mail_from = tx.mail_from
        if tx.rcpt_to:
            self.rcpt_to.extend(tx.rcpt_to)

        if self.upstream_tx is None and tx.rcpt_to:
            self._route(tx)
            if tx.rcpt_response:
                return
        else:
            self.upstream_tx = TransactionMetadata()

        self.upstream_tx.mail_from = tx.mail_from
        self.upstream_tx.rcpt_to = tx.rcpt_to

        # TODO in this case, since the received header that's being
        # prepended onto the body doesn't depend on the body contents,
        # we can trickle out the body as it comes through rather than
        # effectively buffering it all like this. However something
        # else in the chain is likely to do that anyway so it's
        # probably moot.
        if tx.body_blob and tx.body_blob.len() == tx.body_blob.content_length():
            upstream_body = CompositeBlob()
            received = InlineBlob(self._format_received(tx).encode('ascii'))
            upstream_body.append(received, 0, received.len())
            upstream_body.append(tx.body_blob, 0, tx.body_blob.len(), True)
            self.upstream_tx.body_blob = upstream_body

        self.endpoint.on_update(self.upstream_tx)
        tx.mail_response = self.upstream_tx.mail_response
        tx.rcpt_response = self.upstream_tx.rcpt_response
        tx.data_response = self.upstream_tx.data_response


    def abort(self):
        if self.endpoint: self.endpoint.abort()
