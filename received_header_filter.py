from typing import List, Optional
from datetime import datetime
import email.utils
import copy
import logging
from email.parser import BytesHeaderParser
from email import policy

from blob import Blob, InlineBlob, CompositeBlob
from filter import Filter, HostPort, Mailbox, TransactionMetadata
from response import Response

class ReceivedHeaderFilter(Filter):
    next : Optional[Filter]
    inject_time : Optional[datetime] = None
    smtp_meta : Optional[dict] = None
    mail_from : Optional[Mailbox] = None
    rcpt_to : List[Mailbox]
    received_hostname : Optional[str] = None
    remote_host : Optional[HostPort] = None
    remote_hostname : Optional[str] = None
    fcrdns : Optional[bool] = None
    max_received_headers : int

    def __init__(self, next : Optional[Filter] = None,
                 received_hostname : Optional[str] = None,
                 inject_time = None,
                 max_received_headers = 30):
        self.next = next
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
        if self.remote_host and self.remote_host.host:
            received_host_literal = '[' + self.remote_host.host + ']'

            if self.remote_hostname and self.fcrdns:
                received_host = (self.remote_hostname + ' ' +
                                 received_host_literal)
            else:
                received_host = received_host_literal

        clauses = []
        ehlo = None
        with_protocol = None
        if self.smtp_meta is not None:
            ehlo = self.smtp_meta.get('ehlo_host', None)

            if self.mail_from.esmtp and 'SMTPUTF8' in self.mail_from.esmtp:
                with_protocol = 'UTF8SMTP'
            elif self.smtp_meta.get('esmtp', False):
                with_protocol = 'ESMTP'
            else:
                with_protocol = 'SMTP'
            if self.smtp_meta.get('tls', False):
                with_protocol += 'S'
            if self.smtp_meta.get('auth', False):
                with_protocol += 'A'
            with_protocol = 'with ' + with_protocol
        else:
            if self.remote_hostname and self.fcrdns:
                ehlo = self.remote_hostname
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

        if len(self.rcpt_to) == 1:
            clauses.append('for ' + self.rcpt_to[0].mailbox)

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
                  timeout : Optional[float] = None):
        if tx.smtp_meta:
            self.smtp_meta = tx.smtp_meta
        if tx.mail_from:
            self.mail_from = tx.mail_from
        if tx.rcpt_to:
            self.rcpt_to.extend(tx.rcpt_to)
        if tx.remote_host:
            self.remote_host = tx.remote_host
        if tx.remote_hostname:
            self.remote_hostname = tx.remote_hostname
        if tx.fcrdns:
            self.fcrdns = tx.fcrdns

        # TODO in this case, since the received header that's being
        # prepended onto the body doesn't depend on the body contents,
        # we can trickle out the body as it comes through rather than
        # effectively buffering it all like this. However something
        # else in the chain is likely to do that anyway so it's
        # probably moot.
        upstream_tx = tx
        if tx.body_blob and tx.body_blob.len() == tx.body_blob.content_length():
            resp = self._check_max_received_headers(tx.body_blob)
            if resp:
                tx.data_response = resp
                return

            upstream_body = CompositeBlob()
            received = InlineBlob(self._format_received(tx).encode('ascii'))
            upstream_body.append(received, 0, received.len())
            upstream_body.append(tx.body_blob, 0, tx.body_blob.len(), True)
            upstream_tx = copy.copy(tx)
            upstream_tx.body_blob = upstream_body

        if self.next:
            self.next.on_update(upstream_tx)
            if upstream_tx != tx:
                tx.mail_response = upstream_tx.mail_response
                tx.rcpt_response = upstream_tx.rcpt_response
                tx.data_response = upstream_tx.data_response


    def abort(self):
        pass
