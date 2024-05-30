from typing import List, Optional
from datetime import datetime
import email.utils
import copy
import logging
from email.parser import BytesHeaderParser
from email import policy

from blob import Blob, InlineBlob, CompositeBlob
from filter import (
    HostPort,
    Mailbox,
    SyncFilter,
    TransactionMetadata )
from response import Response

class ReceivedHeaderFilter(SyncFilter):
    upstream : Optional[SyncFilter]
    inject_time : Optional[datetime] = None
    received_hostname : Optional[str] = None
    max_received_headers : int
    body_blob : Optional[Blob] = None
    data_err : Optional[Response] = None

    def __init__(self, upstream : Optional[SyncFilter] = None,
                 received_hostname : Optional[str] = None,
                 inject_time = None,
                 max_received_headers = 30):
        self.upstream = upstream
        self.inject_time = inject_time
        self.rcpt_to = []
        self.received_hostname = received_hostname
        self.max_received_headers = max_received_headers

# Received: from a48-180.smtp-out.amazonses.com
#  (a48-180.smtp-out.amazonses.com. [54.240.48.180])
#  by mx.google.com with ESMTPS id iu13-20020ad45ccd000000b0068ca87d31f1si2255553qvb.592.2024.02.07.14.47.24
#  for <alice@example.com>
#  (version=TLS1_2 cipher=ECDHE-ECDSA-AES128-GCM-SHA256 bits=128/128);
#  Wed, 07 Feb 2024 14:47:24 -0800 (PST)

    def _format_received(self, tx : TransactionMetadata) -> str:
        received_host = None
        received_host_literal = None
        if tx.remote_host and tx.remote_host.host:
            received_host_literal = '[' + tx.remote_host.host + ']'

            if tx.remote_hostname and tx.fcrdns:
                received_host = (tx.remote_hostname + ' ' +
                                 received_host_literal)
            else:
                received_host = received_host_literal

        clauses = []
        ehlo = None
        with_protocol = None
        if tx.smtp_meta is not None:
            ehlo = tx.smtp_meta.get('ehlo_host', None)

            if tx.mail_from.esmtp and 'SMTPUTF8' in tx.mail_from.esmtp:
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
            if tx.remote_hostname and tx.fcrdns:
                ehlo = tx.remote_hostname
            elif received_host_literal:
                ehlo = received_host_literal
            with_protocol = 'with X-RESTMTP'

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

    def _check_max_received_headers(self, body_blob : Blob):
        body = body_blob.read(0, int(pow(2, 16)))
        parser = BytesHeaderParser(policy=policy.SMTP)
        parsed = parser.parsebytes(body)
        received_count = 0
        for (k,v) in parsed.items():
            if k.lower() == 'received':
                received_count += 1
                if received_count > self.max_received_headers:
                    return Response(550, '5.4.6 message has too many received: '
                                    'headers and is likely looping')
        return None

    def on_update(self, tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:
        if tx.body_blob is None:
            return self.upstream.on_update(tx, tx_delta)

        # TODO in this case, since the received header that's being
        # prepended onto the body doesn't depend on the body contents,
        # we could trickle out the body as it comes through rather than
        # effectively buffering it all like this. However something
        # else in the chain is likely to do that anyway so it's
        # probably moot.

        blob_complete = (tx.body_blob.len() == tx.body_blob.content_length())
        data_resp = None
        if blob_complete and self.body_blob is None and self.data_err is None:
            self.data_err = self._check_max_received_headers(tx.body_blob)

            if self.data_err is None:
                self.body_blob = CompositeBlob()
                received = InlineBlob(self._format_received(tx).encode('ascii'))
                self.body_blob.append(received, 0, received.len())
                self.body_blob.append(tx.body_blob, 0, tx.body_blob.len(), True)

        assert self.body_blob is None or self.data_err is None
        downstream_tx = tx.copy()
        downstream_delta = tx_delta.copy()
        downstream_tx.body_blob = self.body_blob
        downstream_delta.body_blob = self.body_blob

        if not bool(downstream_delta):
            return TransactionMetadata()

        # we continue upstream even if we already know we're going to
        # fail the body to get authoritative responses for mail/rcpt
        upstream_delta = self.upstream.on_update(
            downstream_tx, downstream_delta)
        if self.data_err is not None:
            assert upstream_delta.data_response is None
            upstream_delta.data_response = self.data_err
        tx.merge_from(upstream_delta)
        return upstream_delta
