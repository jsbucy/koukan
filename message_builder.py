from typing import Callable, List, Optional, Tuple

import logging
import datetime
import io

from email.message import EmailMessage, MIMEPart
from email.generator import BytesGenerator
from email.headerregistry import Address
from email import policy

from blob import Blob

BlobFactory = Callable[[str],Blob]

class MessageBuilder:
    blob_factory : Optional[BlobFactory]

    def __init__(self, json, blob_factory : Optional[BlobFactory] = None):
        self.json = json
        self.header_json = self.json.get('headers', {})

        self.blob_factory = blob_factory

        self._header_adders = {
            'from': MessageBuilder._add_address_header,
            'to': MessageBuilder._add_address_header,
            'cc': MessageBuilder._add_address_header,
            'bcc': MessageBuilder._add_address_header,
            'date': MessageBuilder._add_date_header,
            'message-id': MessageBuilder._add_messageid_header,
            'in-reply-to': MessageBuilder._add_messageid_header,
            'references': MessageBuilder._add_messageid_header,
            # any other header (e.g. subject) will be added verbatim
        }


    # replace part 'content_uri' with 'blob_rest_id' per uri_to_id
    # and return a list of these
    @staticmethod
    def get_blobs(json, uri_to_id : Callable[[str], str]) -> List[str]:
        reuse = []

        for multipart in [
                'text_body', 'related_attachments', 'file_attachments']:
            parts = json.get(multipart, [])
            for part in parts:
                if not part.get('content_uri', None):
                    continue
                blob_rest_id = uri_to_id(part['content_uri'])
                part['blob_rest_id'] = blob_rest_id
                del part['content_uri']
                reuse.append(blob_rest_id)
        return reuse

    def _add_part(self, part_json, multipart,
                  existing_part=None,
                  inline=None):
        maintype, subtype = part_json['content_type'].split('/')
        part = existing_part if existing_part is not None else MIMEPart()

        # TODO use str if maintype == 'text'
        content : Optional[bytes]
        if 'content' in part_json:
            content = part_json['content'].encode('utf-8')
        elif 'blob_rest_id' in part_json:
            blob = self.blob_factory(part_json['blob_rest_id'])
            content = blob.read(0)
        else:
            raise ValueError()

        part.set_content(content,
                         maintype=maintype, subtype=subtype)
        if maintype == 'text':
            # for now text/* must be utf8 or ascii
            content.decode('utf-8')  # fail if not utf8
            part.set_param('charset', 'utf-8')

        if inline:
            part.add_header('content-disposition', 'inline')
        elif 'filename' in part_json:
            part.add_header('content-disposition', 'attachment',
                            filename=part_json['filename'])
        if 'content_id' in part_json:
            part.add_header('content-id', part_json['content_id'])

        if existing_part is None:
            multipart.attach(part)

    def _add_address_header(self, field, field_json, builder):
        addrs = []
        for addr_json in field_json:
            if not (addr := addr_json.get('address', None)):
                raise ValueError('no address')
            addrs.append(Address(display_name=addr_json.get('display_name', ''),
                                 addr_spec=addr))
        if not addrs:
            return
        builder[field] = addrs

    def _add_messageid_header(self, field, field_json, builder):
        msgids = []
        for msgid in field_json:
            msgids.append('<' + msgid + '>')

        builder[field] = ' '.join(msgids)

    def _add_date_header(self, field, field_json, builder):
        if tz_json := field_json.get('tz_offset', None):
            tz = datetime.timezone(datetime.timedelta(seconds=tz_json))
        else:
            tz = datetime.timezone.utc  # tz=None -> system timezone?

        builder[field] = datetime.datetime.fromtimestamp(
            field_json['unix_secs'], tz=tz)

    def _add_headers(self, builder):
        for k,v in self.header_json:
            if adder := self._header_adders.get(k, None):
                adder(self, k, v, builder)
            else:
                builder[k] = v

    def build(self, out):
        builder = EmailMessage()

        self._add_headers(builder)

        text_body = self.json.get('text_body', [])
        if text_body:
            self._add_part(text_body[0], None, builder)

        related = self.json.get('related_attachments', [])
        if related:
            builder.make_related()
            for part in related:
                self._add_part(part, builder, inline=True)

        extra_text = text_body[1:]
        if extra_text:
            builder.make_alternative()
            for part in extra_text:
                self._add_part(part, builder)

        mixed = self.json.get('file_attachments', [])
        if mixed:
            builder.make_mixed()
            for part in mixed:
                self._add_part(part, builder)

        gen = BytesGenerator(out, policy=policy.SMTP)
        gen.flatten(builder)


    def build_headers_for_notification(self) -> bytes:
        builder = EmailMessage()
        self._add_headers(builder)
        out = io.BytesIO()
        gen = BytesGenerator(out, policy=policy.SMTP)
        gen.flatten(builder)
        out.seek(0)
        return out.read()
