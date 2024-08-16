from typing import Optional
from tempfile import TemporaryFile
import logging
from io import IOBase

from filter import (
    SyncFilter,
    TransactionMetadata )
from message_builder import MessageBuilder
from storage import Storage
from blob import FileLikeBlob
from rest_schema import BlobUri

class MessageBuilderFilter(SyncFilter):
    upstream : Optional[SyncFilter] = None
    body_delta : Optional[TransactionMetadata] = None

    def __init__(self, storage : Storage, upstream):
        self.storage = storage
        self.upstream = upstream

    def _blob_factory(self, tx_rest_id : str, blob_id : str):
        logging.debug('message builder %s %s', tx_rest_id, blob_id)
        blob_reader = self.storage.get_blob_for_read(
            BlobUri(tx_id=tx_rest_id, blob=blob_id))
        if blob_reader is None:
            return None
        return blob_reader

    def on_update(self, tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata] :
        logging.debug('MessageBuilderFilter.on_update %s', tx_delta)
        if self.body_delta is None and tx_delta.message_builder is not None:
            builder = MessageBuilder(
                tx_delta.message_builder,
                lambda blob_id: self._blob_factory(tx.rest_id, blob_id))

            file = TemporaryFile('w+b')
            assert isinstance(file, IOBase)
            builder.build(file)
            file.flush()
            body_blob = FileLikeBlob(file, finalized=True)
            self.body_delta = TransactionMetadata(body_blob = body_blob)
            logging.debug('MessageBuilderFilter.on_update %d %s',
                          body_blob.len(), body_blob.content_length())

        downstream_tx = tx.copy()
        downstream_delta = tx_delta.copy()

        if downstream_tx.message_builder:
            downstream_tx.message_builder = None
        if downstream_delta.message_builder:
            downstream_delta.message_builder = None

        if self.body_delta is None:
            if self.upstream is None:
                return TransactionMetadata()
            return self.upstream.on_update(downstream_tx, downstream_delta)

        assert downstream_tx.merge_from(self.body_delta) is not None
        assert downstream_delta.merge_from(self.body_delta) is not None
        upstream_delta = self.upstream.on_update(
            downstream_tx, downstream_delta)
        assert tx.merge_from(upstream_delta) is not None
        return upstream_delta
