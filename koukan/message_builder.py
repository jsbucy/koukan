# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Callable, Dict, List, Optional, Set, Tuple

import logging
import datetime
import io

from email.message import EmailMessage, MIMEPart
from email.generator import BytesGenerator
from email.headerregistry import Address
from email import policy

from koukan.blob import Blob, InlineBlob

from koukan.rest_schema import parse_blob_uri
from koukan.storage_schema import BlobSpec

class MessageBuilderSpec:
    json : dict
    blob_specs : List[BlobSpec]
    blobs : List[Blob]
    body_blob : Optional[Blob] = None
    # part['content']['create_id']  XXX rename
    ids : Optional[Set[str]] = None

    def __init__(self, json, blobs : Optional[List[Blob]] = None,
                 blob_specs : Optional[List[BlobSpec]] = None):
        self.json = json
        self.blobs = blobs if blobs else []

        self.blob_specs = blob_specs if blob_specs else []

    def check_ids(self):
        self.ids = set()
        if root_part := self.json.get('parts', None):
            self._check_ids(root_part)
        for multipart in [
                'text_body', 'related_attachments', 'file_attachments']:
            parts = self.json.get(multipart, [])
            for part in parts:
                self._check_ids(part)

    def _check_ids(self, part):
        for p in part.get('parts', []):
            self._check_ids(p)
        if content := part.get('content', {}):
            if 'create_id' in part['content']:
                self.ids.add(part['content']['create_id'])

    def parse_blob_specs(self):
        create_blob_id = 0
        for multipart in [
                'text_body', 'related_attachments', 'file_attachments']:
            parts = self.json.get(multipart, [])
            for part in parts:
                self._add_part_blob(part, create_blob_id)
                create_blob_id += 1

    def _add_part_blob(self, part, create_blob_id : int):
        # if not present -> invalid spec
        content = part.get('content')
        if 'reuse_uri' in content:
            reuse_uri = parse_blob_uri(content['reuse_uri'])
            assert reuse_uri
            blob_spec = BlobSpec(reuse_uri=reuse_uri)
        elif 'create_id' in content:
            blob_spec = BlobSpec(create_id=content['create_id'])
        elif 'inline' in content:
            blob_spec = BlobSpec(
                blob = InlineBlob(content['inline'].encode('utf-8'), last=True),
                create_id='inline%d' % create_blob_id)
        else:
            raise ValueError('bad MessageBuilder entity content')
        # cf MessageBuilder._add_part(), reusing 'create_id' for both
        # create and reuse, maybe should be separate tag '_internal_blob_id'
        if blob_spec.create_id:
            blob_id = blob_spec.create_id
        else:
            blob_id = blob_spec.reuse_uri.blob
        part['content'] = {'create_id': blob_id}
        self.blob_specs.append(blob_spec)

    def finalized(self):
        return (len(self.ids) == len(self.blobs) and
                not any([not b.finalized() for b in self.blobs]))

    def __repr__(self):
        return '%s %s' % (self.json, self.blobs)

class MessageBuilder:
    blobs : Dict[str, Blob]
    def __init__(self, json, blobs : Dict[str, Blob]):
        self.json = json
        self.blobs = blobs
        self.header_json = self.json.get('headers', {})
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

    def _add_part(self, part_json, multipart,
                  existing_part=None,
                  inline=None):
        maintype, subtype = part_json['content_type'].split('/')
        part = existing_part if existing_part is not None else MIMEPart()

        # TODO use str if maintype == 'text'
        # content : Optional[bytes]
        content_json = part_json['content']
        if 'inline' in content_json:
            content = content_json['inline'].encode('utf-8')
        elif 'create_id' in content_json:
            blob = self.blobs[content_json['create_id']]
            content = blob.pread(0)
        else:
            raise ValueError()

        part.set_content(content, maintype=maintype, subtype=subtype)
        if maintype == 'text':
            # for now text/* must be utf8 or ascii
            assert content is not None
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
        for header in self.header_json:
            if len(header) != 2:
                logging.debug('_add_headers bad header %s', header)
                continue
            k,v = header
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
