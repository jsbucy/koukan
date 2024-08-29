# reference https://datatracker.ietf.org/doc/html/rfc3464
from typing import Optional

import datetime

from email.policy import default
from email import policy
from email.mime.text import MIMEText
from email.mime.message import MIMEMessage
from email.headerregistry import ContentTypeHeader, DateHeader
from email.message import EmailMessage, MIMEPart, Message
from email.utils import format_datetime

from email.headerregistry import Address

from email.parser import HeaderParser
from email import policy

from math import pow
import time
import secrets
import logging

from response import Response

# I couldn't see how to get BytesHeaderParser to give me back either the
# input headers without the body and without reserializing or the byte
# offset of the end of the headers.
def read_headers(blob) -> Optional[str]:
    body = blob.read(0, int(pow(2, 16)))
    off = body.find(b'\r\n\r\n')
    if off == -1:
        # if we can't find CRLFCRLF in the first 65k, it's probably garbage
        return ''
    body = body[0:off+4]
    # xxx errors
    return body.decode('utf-8')

def _cleanup_part(part : Message):
    # the docs say MIMEPart skips adding the MIME-Version header but it seems
    # like buried in the flow e.g. set_charset() ends up adding it
    if 'mime-version' in part:
        del part['mime-version']

# domain for msgid
# header-from for mailer-daemon
def generate_dsn(mailer_daemon_mailbox, return_path, orig_to,
                 # one would expect bytes for this
                 # cf headers.set_content() below
                 orig_headers : str,
                 received_date : int,
                 now : int,
                 response: Response) -> bytes:
    dsn = EmailMessage(policy = policy.SMTP)
    dsn.make_mixed()

    parser = HeaderParser(policy=policy.SMTP)
    parsed = parser.parsestr(orig_headers)
    msgid = parsed.get('message-id', None)

    # top-level headers
    dsn['from'] = Address(display_name='Mail Delivery Subsystem',
                          addr_spec=mailer_daemon_mailbox)

    dsn.add_header('to', return_path)
    now_date = datetime.datetime.utcfromtimestamp(now)
    dsn['date'] = now_date

    mailer_daemon_domain = mailer_daemon_mailbox[
        mailer_daemon_mailbox.rfind('@') + 1 : ]
    dsn['message-id'] = secrets.token_hex(16) + '@' + mailer_daemon_domain

    dsn.add_header('subject', 'Delivery Status Notification (failure)')

    # so even if you don't understand dsn, it will still thread with
    # the original
    if msgid:
        dsn.add_header('in-reply-to', msgid)
        dsn.add_header('references', msgid)

    # 1st: part: text/plain human-readable
    readable = MIMEPart()
    readable.set_type('text/plain')
    readable.set_content("delivery failed")
    _cleanup_part(readable)
    dsn.attach(readable)

    # 2nd part: message/delivery-status
    #   with 1 per-message fields and 1 or more per-recipient fields
    per_recipient_fields = MIMEPart(policy = policy.SMTP)
    per_recipient_fields.add_header('Final-Recipient', 'rfc822;' + orig_to)
    per_recipient_fields.add_header('Action', 'failed')
    per_recipient_fields['Last-Attempt-Date'] = format_datetime(now_date)

    # TODO finer-grained errors
    if response.temp():
        per_recipient_fields['Status'] = '4.0.0'
    elif response.perm():
        per_recipient_fields['Status'] = '5.0.0'

    per_recipient_fields['Diagnostic-Code'] = 'smtp;' + str(response)

    # TODO populate this from tx remote_host?
    # Remote-MTA: dns; tachygraph.gloop.org. (1.2.3.4,
    #   the server for the domain sandbox.gloop.org.)
    # per_recipient_fields['Remote-MTA']

    per_message_fields = MIMEMessage(per_recipient_fields, policy = policy.SMTP)
    per_message_fields['Reporting-MTA'] = 'dns;' + mailer_daemon_domain
    if received_date:
        per_message_fields['Arrival-Date'] = format_datetime(
            datetime.datetime.utcfromtimestamp(received_date))

    del per_message_fields['mime-version']
    del per_message_fields['content-transfer-encoding']
    del per_message_fields['content-type']

    status_msg = MIMEMessage(per_message_fields, 'delivery-status')

    _cleanup_part(status_msg)
    dsn.attach(status_msg)


    # 3rd part: orig msg/headers message/rfc822 or text/rfc822-headers
    headers = MIMEPart()
    dsn.policy = policy.SMTP
    # if content is str, maintype -> text
    # if content is bytes, seems to always get base64 whether it needs it or not
    headers.set_content(orig_headers, subtype='rfc822-headers')
    headers.set_param('charset', 'us-ascii')
    _cleanup_part(headers)

    dsn.attach(headers)

    dsn.set_type('multipart/report')
    dsn.set_param('report_type', 'delivery-status')

    # BytesGenerator if we want to serialize to a file/buffer but we
    # expect these to be small enough that bytes is probably fine
    return bytes(dsn)
