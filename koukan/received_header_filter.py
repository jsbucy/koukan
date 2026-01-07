# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import List, Optional
from datetime import datetime
import email.utils
import copy
import logging
from email.parser import BytesHeaderParser
from email import policy

from koukan.blob import Blob, InlineBlob, CompositeBlob
from koukan.filter import (
    HostPort,
    Mailbox,
    TransactionMetadata,
    get_esmtp_param )
from koukan.filter_chain import FilterResult, ProxyFilter
from koukan.response import Response

from koukan.remote_host_filter import RemoteHostFilter, RemoteHostFilterResult

class ReceivedHeaderFilter(ProxyFilter):
    inject_time : Optional[datetime] = None
    received_hostname : Optional[str] = None  # from yaml

    def __init__(self, received_hostname : Optional[str] = None,
                 inject_time = None):
        self.inject_time = inject_time
        self.received_hostname = received_hostname

# Received: from a48-180.smtp-out.amazonses.com
#  (a48-180.smtp-out.amazonses.com. [54.240.48.180])
#  by mx.google.com with ESMTPS id iu13-20020ad45ccd000000b0068ca87d31f1si2255553qvb.592.2024.02.07.14.47.24
#  for <alice@example.com>
#  (version=TLS1_2 cipher=ECDHE-ECDSA-AES128-GCM-SHA256 bits=128/128);
#  Wed, 07 Feb 2024 14:47:24 -0800 (PST)

    def _format_received(self) -> str:
        tx = self.downstream_tx
        assert tx is not None
        received_host = None
        received_host_literal = None

        rh = tx.get_filter_output(RemoteHostFilter.fullname())
        remote_hostname = fcrdns = None
        if (rh is not None) and (isinstance(rh, RemoteHostFilterResult)):
            fcrdns = rh.fcrdns
            remote_hostname = rh.remote_hostname
        logging.debug('%s %s', fcrdns, remote_hostname)

        if tx.remote_host and tx.remote_host.host:
            received_host_literal = '[' + tx.remote_host.host + ']'
            if remote_hostname and fcrdns:
                received_host = (remote_hostname + ' ' +
                                 received_host_literal)
            else:
                received_host = received_host_literal

        clauses = []
        ehlo = None
        with_protocol = None
        if tx.smtp_meta is not None:
            ehlo = tx.smtp_meta.get('ehlo_host', None)
            assert tx.mail_from is not None
            if tx.mail_from.esmtp and get_esmtp_param(
                    tx.mail_from.esmtp, 'smtputf8') is not None:
                with_protocol = 'UTF8SMTP'
            elif tx.smtp_meta.get('esmtp', False):
                with_protocol = 'ESMTP'
            else:
                with_protocol = 'SMTP'
            if tx.smtp_meta.get('tls', False):
                with_protocol += 'S'
            if tx.smtp_meta.get('auth', False):
                with_protocol += 'A'
            with_protocol = 'with ' + with_protocol
        else:
            # TODO other paths besides rest end up here
            # i.e. internally generated messages/notification/dsn
            if remote_hostname and fcrdns:
                ehlo = remote_hostname
            elif received_host_literal:
                ehlo = received_host_literal
            with_protocol = 'with X-RESTMTP'

        # TODO yaml option to skip the whole 'from' stanza for submission
        if ehlo and received_host:
            clauses.append('from ' + ehlo + ' (' + received_host + ')')

        if self.received_hostname:
            clauses.append('by ' + self.received_hostname)

        if with_protocol:
            clauses.append(with_protocol)

        # TODO id?

        if len(tx.rcpt_to) == 1 and tx.rcpt_to[0] is not None:
            clauses.append('for ' + tx.rcpt_to[0].mailbox)

        # TODO tls info rfc8314

        datetime = (self.inject_time if self.inject_time
                    else email.utils.localtime())

        received = 'Received: %s;\r\n\t%s\r\n' % (
            '\r\n\t'.join(clauses),
            email.utils.format_datetime(datetime))
        return received

    def on_update(self, tx_delta : TransactionMetadata) -> FilterResult:
        assert self.downstream_tx is not None
        assert self.upstream_tx is not None

        body = tx_delta.maybe_body_blob()
        tx_delta.body = None
        if body is not None and not body.finalized():
            body = None
        else:
            assert self.upstream_tx.body is None

        self.upstream_tx.merge_from(tx_delta)

        if body is None:
            return FilterResult()

        # TODO in this case, since the received header that's being
        # prepended onto the body doesn't depend on the body contents,
        # we could trickle out the body as it comes through rather than
        # effectively buffering it all like this. However something
        # else in the chain is likely to do that anyway so it's
        # probably moot.

        upstream_body = CompositeBlob()
        received = InlineBlob(self._format_received().encode('ascii'))
        upstream_body.append(received, 0, received.len())
        upstream_body.append(body, 0, body.len(), True)
        self.upstream_tx.body = upstream_body

        return FilterResult()
