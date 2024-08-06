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
    _blob_i = 0

    def __init__(self, upstream : Optional[SyncFilter] = None):
        self.upstream = upstream

    def _blob_factory(self):
        file = TemporaryFile('w+b')
        assert isinstance(file, IOBase)
        blob_id = str(self._blob_i)
        self._blob_i += 1
        return FileLikeBlob(file, blob_id)

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

        parsed_delta = TransactionMetadata()
        if parsed is not None:
            parsed_delta.parsed_blobs = parsed.blobs
            parsed_delta.parsed_json = parsed.json
            tx.merge_from(parsed_delta)
        upstream_delta = self.upstream.on_update(
            tx, tx_delta.merge(parsed_delta))
        assert upstream_delta is not None
        upstream_delta.merge_from(parsed_delta)
        return upstream_delta
