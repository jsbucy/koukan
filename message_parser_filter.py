from typing import Optional
from tempfile import TemporaryFile
from io import IOBase

from blob import (
    FileLikeBlob,
    InlineBlob )
from filter import (
    SyncFilter,
    TransactionMetadata )
from message_parser import (
    MessageParser,
    ParsedMessage )

class MessageParserFilter(SyncFilter):
    upstream : Optional[SyncFilter] = None

    def __init__(self, upstream : Optional[SyncFilter] = None):
        self.upstream = upstream

    def _blob_factory(self):
        file = TemporaryFile('w+b')
        assert isinstance(file, IOBase)
        return FileLikeBlob(file)

    def on_update(self, tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:
        if tx.body_blob is None or not tx.body_blob.finalized():
            if self.upstream:
                return self.upstream.on_update(tx, tx_delta)
            else:
                return TransactionMetadata()

        file = TemporaryFile('w+b')
        file.write(tx.body_blob.read(0))
        file.flush()
        file.seek(0)
        parser = MessageParser(self._blob_factory, max_inline=0)
        parsed = parser.parse(file)
        file.close()

        if parsed is not None:
            downstream_delta = tx_delta.copy()
            body_blob = downstream_delta.body_blob
            tx.body_blob = downstream_delta.body_blob = None
            if parsed.blobs:
                blob_delta = TransactionMetadata()
                blob_delta.parsed_blobs = parsed.blobs
                tx.merge_from(blob_delta)
                downstream_delta.merge_from(blob_delta)
                upstream_delta = self.upstream.on_update(tx, downstream_delta)
                assert upstream_delta is not None
                upstream_delta.merge_from(blob_delta)
                if upstream_delta.data_response:  # err
                    return upstream_delta
                downstream_delta = TransactionMetadata()
            else:
                upstream_delta = TransactionMetadata()

            # blob IDs -> json
            assert len(upstream_delta.parsed_blob_ids) == len(parsed.blobs)
            parser.add_blob_ids(upstream_delta.parsed_blob_ids)

            json_delta = TransactionMetadata()
            json_delta.parsed_json = parsed.json
            tx.merge_from(json_delta)
            downstream_delta.merge_from(json_delta)
            json_upstream_delta = self.upstream.on_update(tx, downstream_delta)
            upstream_delta.merge_from(json_upstream_delta)

        blob_delta = TransactionMetadata(body_blob=body_blob)
        tx.merge_from(blob_delta)
        blob_upstream_delta = self.upstream.on_update(tx, blob_delta)
        upstream_delta.merge_from(blob_upstream_delta)
        return upstream_delta
