from typing import Callable, Dict, List, Optional, Union
import logging

from email.message import EmailMessage
from email.parser import BytesParser
from email.headerregistry import (
    Address,
    AddressHeader,
    BaseHeader,
    DateHeader,
    MessageIDHeader )
import email.policy

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
            'in-reply-to': self._parse_messageid_header,
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

    def _parse_generic_header(self, header : BaseHeader):
        return str(header)

    def parse_headers(self, message : EmailMessage, out):
        headers_out = []
        out['headers'] = headers_out
        for name,value in message.items():
            name = name.lower()
            logging.debug('%s: %s', name, value)
            if parser := self._header_parsers.get(name, None):
                out_i = parser(value)
            else:
                out_i = self._parse_generic_header(value)
            headers_out.append([name.lower(), out_i])

    def _parse_part(self, part, out):
        logging.debug(part.get_content_type())
        logging.debug(part.get_content())

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
            content = part.get_content().encode('utf-8')
        else:
            content = part.get_content()
        blob.append_data(0, content, len(content))
        out['blob_id'] = len(self.out.blobs)
        self.out.blobs.append(blob)

    def _parse_mime_tree(self, part, depth, parse_headers, out : dict):
        if parse_headers:
            self.parse_headers(part, out)

        out['content_type'] = part.get_content_type()
        # disposition/filename

        # TODO also parse headers if content_type ==
        # 'text/rfc822-headers' for DSN, need to reparse the content
        # since email.parser probably doesn't do it?
        is_message = (part.get_content_maintype() == 'message')
        # assert not is_message or (
        #   part.is_multipart() and len(part.iter_parts()) == 1)

        if part.is_multipart() and depth < self.max_depth:
            out['parts'] = []
            for part_i in part.iter_parts():
                out_i = {}
                out['parts'].append(out_i)
                self._parse_mime_tree(part_i, depth + 1, is_message, out_i)
        else:
            self._parse_part(part, out)


    def parse(self, raw_file  # binary file
              ) -> Optional[ParsedMessage]:
        parser = BytesParser(policy=email.policy.SMTP)
        parsed = parser.parse(raw_file)

        parts = {}
        self.out.json['parts'] = parts
        self._parse_mime_tree(parsed, 0, True, parts)

        logging.debug(self.out.json)
        logging.debug(self.out.blobs)

        return self.out

        # generally expecting one of

        # multipart/mixed
        #   multipart/related
        #     multipart/alternative
        #       text/plain
        #       text/html
        #     inline parts
        #   attachments

        # or

        # multipart/mixed
        #   multipart/alternative
        #     text/plain
        #     multipart/related
        #       text/html
        #       inline parts
        #   attachments

        # but there's stuff like e.g. s/mime that the whole thing will
        # be wrapped in multipart/signed ?

        # don't recurse into message/* -> single blob

    def add_blob_ids(self, blob_ids : List[str]):
        logging.debug(self.out.blobs)
        logging.debug(blob_ids)
