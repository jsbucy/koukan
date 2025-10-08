# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Callable, Dict, List, Optional, Set, Sequence, Tuple, Union

import logging
import datetime
import io
import copy

from email.message import EmailMessage, MIMEPart
from email.generator import BytesGenerator
from email.headerregistry import Address
from email import policy

from koukan.blob import Blob, InlineBlob

from koukan.rest_schema import parse_blob_uri
from koukan.storage_schema import BlobSpec
#from koukan.filter import WhichJson

class MessageBuilderSpec:
    json : dict
    blobs : Dict[str, Union[Blob, BlobSpec, None]]
    body_blob : Union[Blob, BlobSpec, None] = None
    uri : Optional[str] = None

    def __init__(self, json,
                 blobs : Optional[Dict[str, Union[Blob, BlobSpec]]] = None):
        self.json = json
        # XXX Mapping?
        self.blobs = {bi:bs for bi,bs in blobs.items() } if blobs else {}

    def set_blobs(self, blobs : Sequence[Blob]):
        for blob in blobs:
            bid = blob.rest_id()
            assert bid is not None
            self.blobs[bid] = blob

    def clone(self):
        out = copy.copy(self)
        out.blobs = copy.copy(self.blobs)
        return out

    # walks mime part tree in json and populates placeholder entries in blobs
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
            if (blob_id := part['content'].get('create_id', None)) and blob_id not in self.blobs:

                self.blobs[blob_id] = None

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
        blob_id = None
        if blob_spec.create_id:
            blob_id = blob_spec.create_id
        elif blob_spec.reuse_uri is not None:
            blob_id = blob_spec.reuse_uri.blob
        if blob_id is None:
            raise ValueError()
        part['content'] = {'create_id': blob_id}
        assert blob_id not in self.blobs
        self.blobs[blob_id] = blob_spec

    def finalized(self):
        logging.debug('finalized %s', self.blobs)
        for blob_id, blob in self.blobs.items():
            logging.debug('%s %s', blob_id, blob)
            if isinstance(blob, Blob):
                if not blob.finalized():
                    return False
            elif isinstance(blob, BlobSpec):
                if not blob.finalized:
                    return False
            elif blob is None:
                return False
            else:
                assert False, blob
        return True

    def delta(self, rhs, which_json
              ) -> Optional[bool]:
        logging.debug(self)
        logging.debug(rhs)
        if not isinstance(rhs, MessageBuilderSpec):
            return None
        # xxx rhs can be None if REST_READ
        # which_json != WhichJson.REST_READ and
        # if which_json == WhichJson.REST_READ:
        #     if rhs.json != {} and rhs.json != self.json:
        #         return None
        if rhs.json != {} and self.json != rhs.json:
            return None
        out = False
        if not self.blobs:
            if rhs.blobs:
                out = True
        else:
            if self.blobs.keys() != rhs.blobs.keys():
                return None
            for blob_id,blob in self.blobs.items():
                rblob = rhs.blobs[blob_id]
                if rblob is None:
                    return None
                if blob is None and rblob is not None:
                    out = True
                    continue
                bdelta = blob.delta(rblob, which_json)
                if bdelta is None:
                    return None
                out |= bdelta
        return out


    def __repr__(self):
        return '%s %s' % (self.json, self.blobs)

class MessageBuilder:
    blobs : Dict[str, Blob]
    def __init__(self, json, blobs : Dict[str, Any]):
        self.json = json
        def _blob(b):
            assert isinstance(b, Blob)
            return b
        self.blobs = { bid : _blob(blob) for bid,blob in blobs.items() }
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
        if (content_json := part_json.get('content', None)) is None:
            raise ValueError('no part content')
        if inline_content := content_json.get('inline', None):
            content = inline_content.encode('utf-8')
        elif create_id := content_json.get('create_id', None):
            if not (blob := self.blobs.get(create_id, None)):
                raise ValueError('invalid blob id')
            content = blob.pread(0)
        else:
            raise ValueError('invalid part content')

        part.set_content(content, maintype=maintype, subtype=subtype)
        if maintype == 'text':
            # for now text/* must be utf8 or ascii
            assert content is not None
            content.decode('utf-8')  # fail if not utf8
            part.set_param('charset', 'utf-8')

        if inline:
            part.add_header('content-disposition', 'inline')
        elif filename := part_json.get('filename', None):
            part.add_header('content-disposition', 'attachment',
                            filename=filename)
        if content_id := part_json.get('content_id', None):
            part.add_header('content-id', content_id)

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
        if date := field_json.get('unix_secs', None):
            builder[field] = datetime.datetime.fromtimestamp(date, tz=tz)
        else:
            raise ValueError('invalid date')

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
