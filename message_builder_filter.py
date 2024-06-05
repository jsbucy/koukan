from typing import Optional
import copy
from tempfile import TemporaryFile
import logging

from filter import (
    SyncFilter,
    TransactionMetadata )
from message_builder import MessageBuilder
from storage import Storage
from blob import FileLikeBlob

class MessageBuilderFilter(SyncFilter):
    upstream_tx : Optional[TransactionMetadata] = None

    def __init__(self, storage : Storage, upstream):
        self.storage = storage
        self.upstream = upstream

    def _blob_factory(self, blob_id):
        blob_reader = self.storage.get_blob_reader()
        blob_reader.load(rest_id=blob_id)
        return blob_reader

    def on_update(self, tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata] :
        if self.upstream_tx is None:
            self.upstream_tx = tx.copy()
        else:
            assert self.upstream_tx.merge_from(tx_delta) is not None

        downstream_delta = tx_delta.copy()

        if tx_delta.message_builder is not None:
            builder = MessageBuilder(
                tx_delta.message_builder,
                lambda blob_id: self._blob_factory(blob_id))

            file = TemporaryFile('w+b')
            builder.build(file)
            file.flush()
            body_blob = FileLikeBlob(file)
            self.upstream_tx.body_blob = downstream_delta.body_blob = body_blob
            del self.upstream_tx.message_builder
            del downstream_delta.message_builder
            logging.debug('MessageBuilderFilter.on_update %d %d',
                      body_blob.len(), body_blob.content_length())

        upstream_delta = self.upstream.on_update(
            self.upstream_tx, downstream_delta)
        assert tx.merge_from(upstream_delta) is not None
        logging.debug('MessageBuilderFilter upstream_delta %s', upstream_delta)
        return upstream_delta
