from typing import Callable, Dict, List, Optional, Union
import logging

from email.message import EmailMessage, MIMEPart
from email.parser import BytesParser
from email.headerregistry import (
    Address,
    AddressHeader,
    BaseHeader,
    ContentDispositionHeader,
    ContentTypeHeader,
    DateHeader,
    MessageIDHeader,
    ParameterizedMIMEHeader )
import email.policy
import email.contentmanager

from blob import (
    Blob,
    WritableBlob )

class ParsedMessage:
    json : dict
    blobs : List[Blob]

    def __init__(self):
        self.json = {}
        self.blobs = []

class MessageParser:
    BlobFactory = Callable[[],WritableBlob]
    HeaderParser = Callable[[BaseHeader], Union[str,dict]]
    _header_parsers : Dict[str, HeaderParser]
    out : ParsedMessage
    blob_factory : BlobFactory
    max_inline : int
    max_depth : int

    def __init__(self, blob_factory : BlobFactory,
                 max_inline=1048576,
                 # limit on mime tree recursion depth, anything more
                 # than this will be parsed into a single part
                 max_recursion_depth = 10):
        self.blob_factory = blob_factory
        self.inline = 0
        self.max_inline = max_inline
        self.max_depth = 10
        self._header_parsers = {
            'from': self._parse_address_header,
            'to': self._parse_address_header,
            'cc': self._parse_address_header,
            'bcc': self._parse_address_header,
            'date': self._parse_date_header,
            'message-id': self._parse_messageid_header,
            'content-disposition': self._parse_parameter_header,
            'content-type': self._parse_parameter_header,

            # TODO default HeaderRegistery doesn't map in-reply-to to
            # MessageIDHeader
            #'in-reply-to': self._parse_messageid_header,

            # TODO MessageIDHeader only holds one but we will want to
            # parse References: at some point
            #'references': self._parse_messageid_header
        }

        self.out = ParsedMessage()

    def _parse_address_header(self, header : BaseHeader):
        assert isinstance(header, AddressHeader)
        out = []
        for addr in header.addresses:
            out.append({'display_name': addr.display_name,
                        'address': addr.addr_spec})
        return out

    def _parse_date_header(self, header : BaseHeader):
        assert isinstance(header, DateHeader)
        return {
            'unix_secs': int(header.datetime.timestamp()),
            'tz_offset': int(header.datetime.utcoffset().total_seconds())
        }

    def _parse_messageid_header(self, header : BaseHeader):
        assert isinstance(header, MessageIDHeader)
        msgid = str(header)
        msgid = msgid.removeprefix('<')
        msgid = msgid.removesuffix('>')
        return msgid

    def _parse_parameter_header(self, header : BaseHeader):
        assert isinstance(header, ParameterizedMIMEHeader)
        params = { k:v for k,v in header.params.items() }
        if isinstance(header, ContentTypeHeader):
            return [header.content_type, params]
        elif isinstance(header, ContentDispositionHeader):
            return [header.content_disposition, params]
        elif isinstance(header, ContentTransferEncodingHeader):
            return [header.cte, params]
        else:
            return _parse_generic_header(header)

    def _parse_generic_header(self, header : BaseHeader):
        return str(header)

    def parse_headers(self, message : MIMEPart, out):
        headers_out = []
        out['headers'] = headers_out
        for name,value in message.items():
            name = name.lower()
            if parser := self._header_parsers.get(name, None):
                out_i = parser(value)
            else:
                out_i = self._parse_generic_header(value)
            headers_out.append([name.lower(), out_i])

    def _parse_text_headers(self, part : MIMEPart, out):
        parser = BytesParser(policy=email.policy.SMTP)
        # this probably gets the line endings converted to LF but it
        # should be ok here
        parsed = parser.parsebytes(part.get_content().encode('ascii'))
        assert isinstance(parsed, MIMEPart)
        self.parse_headers(parsed, out)

    def _parse_part(self, part : MIMEPart, out):
        if part.get_content_maintype() == 'text' and (
                self.inline + len(part.get_content()) < self.max_inline):
            utf8_len = len(part.get_content().encode('utf-8'))
            if self.inline + utf8_len < self.max_inline:
                out['content'] = part.get_content()
                self.inline += utf8_len
                return

        blob = self.blob_factory()
        content : bytes
        if part.get_content_maintype() == 'text':
            content_str = part.get_content()
            content = content_str.encode('utf-8')
        else:
            content = part.get_content()

        blob.append_data(0, content, len(content))
        out['blob_rest_id'] = blob.rest_id()
        self.out.blobs.append(blob)

    def _parse_mime_tree(self, part : MIMEPart,
                         depth, parse_headers : bool,
                         out : dict):
        self.parse_headers(part, out)

        out['content_type'] = part.get_content_type()
        # disposition/filename

        # TODO also parse headers if content_type ==
        # 'text/rfc822-headers' for DSN, need to reparse the content
        # since email.parser probably doesn't do it?
        is_message = (part.get_content_maintype() == 'message')
        is_text_headers = (part.get_content_type() == 'text/rfc822-headers')
        # assert not is_message or (
        #   part.is_multipart() and len(part.iter_parts()) == 1)

        if is_text_headers:
            out_i = {}
            out['parts'] = [out_i]
            self._parse_text_headers(part, out_i)
            return
        elif not part.is_multipart() or depth >= self.max_depth:
            self._parse_part(part, out)
            return

        out['parts'] = []
        for part_i in part.iter_parts():
            out_i = {}
            out['parts'].append(out_i)
            assert isinstance(part_i, MIMEPart)
            self._parse_mime_tree(part_i, depth + 1, is_message, out_i)

    def _flatten_part(self, part_json) -> dict:
        out = {}
        out['content_type'] = part_json['content_type']

        # content_id or filename
        for k,v in part_json['headers']:
            if (k == 'content-disposition' and len(v) == 2 and
                v[0] == 'attachment'):
                if 'filename' in v[1]:
                    out['filename'] = v[1]['filename']
                break
            elif k == 'content-id':
                out['content_id'] = v
                break

        for c in ['content', 'blob_rest_id']:
            if c in part_json:
                out[c] = part_json[c]

        return out

    def _flatten(self):
        next = self.out.json['parts']
        text = []
        related = None
        attach = []

        # S/MIME https://www.rfc-editor.org/rfc/rfc1847.html
        if next['content_type'] == 'multipart/signed':
            next = next['parts'][0]

        if next['content_type'] == 'multipart/mixed':
            attach = next['parts'][1:]
            next = next['parts'][0]

        if next['content_type'] == 'multipart/related':
            related = next['parts'][1:]
            if next['parts'][0]['content_type'] == 'text/html':
                text.append(next['parts'][0])
                next = None
            else:
                next = next['parts'][0]

        if next and next['content_type'] == 'multipart/alternative':
            if next['parts'][0]['content_type'] == 'text/plain':
                text.append(next['parts'][0])
                next = next['parts'][1]
            else:
                next = None

        if (next and related is None and
            next['content_type'] == 'multipart/related'):
            related = next['parts'][1:]
            next = next['parts'][0]

        if next and next['content_type'] in ['text/plain', 'text/html']:
            text.append(next)

        for k,v in [('text_body', text),
                    ('related_attachments', related),
                    ('file_attachments', attach)]:
            if v:
                self.out.json[k] = [ self._flatten_part(p) for p in v ]


    def parse(self, raw_file  # binary file
              ) -> Optional[ParsedMessage]:
        parser = BytesParser(policy=email.policy.SMTP)
        parsed = parser.parse(raw_file)

        parts = {}
        self.out.json['parts'] = parts
        assert isinstance(parsed, MIMEPart)
        self._parse_mime_tree(parsed, 0, True, parts)

        self._flatten()

        return self.out

