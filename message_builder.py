from typing import Callable, List, Optional, Tuple

from email.message import EmailMessage, MIMEPart
from email.generator import BytesGenerator

from blob import Blob

BlobFactory = Callable[[str],Blob]

class MessageBuilder:
    blob_factory : Optional[BlobFactory]
    def __init__(self, json, blob_factory : Optional[BlobFactory]):
        self.json = json
        self.blob_factory = blob_factory

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

    def build(self, out):
        builder = EmailMessage()

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

        gen = BytesGenerator(out, policy=builder.policy.clone(linesep='\r\n'))
        gen.flatten(builder)
