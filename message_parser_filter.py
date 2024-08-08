from typing import Optional
from tempfile import TemporaryFile
from io import IOBase

import logging

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
    parsed_delta : Optional[TransactionMetadata] = None
    parsed : bool = False

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
        logging.debug('MessageParserFilter options %s', tx.options)

        parsed = False
        if (not self.parsed and
            (tx.options and 'receive_parsing' in tx.options) and
            tx.body_blob is not None and tx.body_blob.finalized()):
            file = TemporaryFile('w+b')
            file.write(tx.body_blob.read(0))
            file.flush()
            file.seek(0)
            parser = MessageParser(
                self._blob_factory,
                max_inline=tx.options.get('receive_parsing_max_inline', 65536))
            parsed_message = parser.parse(file)
            file.close()
            parsed = self.parsed = True
            if parsed_message is not None:
                self.parsed_delta = TransactionMetadata()
                self.parsed_delta.parsed_blobs = parsed_message.blobs
                self.parsed_delta.parsed_json = parsed_message.json

        if self.parsed_delta is None:
            if self.upstream is None:
                return TransactionMetadata()
            return self.upstream.on_update(tx, tx_delta)

        # cf "filter chain" doc 2024/8/6, we can't add internal fields
        # to the downstream tx because it will cause a delta/conflict
        # when we do the next db read in the OutputHandler

        downstream_tx = tx.copy()
        downstream_tx.merge_from(self.parsed_delta)
        downstream_delta = tx_delta.copy()
        if parsed:
            downstream_delta.merge_from(self.parsed_delta)
        upstream_delta = self.upstream.on_update(
            downstream_tx, downstream_delta)
        assert upstream_delta is not None
        tx.merge_from(upstream_delta)
        return upstream_delta
