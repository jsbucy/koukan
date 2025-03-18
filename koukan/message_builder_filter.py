# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional
from tempfile import TemporaryFile
import logging
from io import IOBase

from koukan.filter import (
    SyncFilter,
    TransactionMetadata )
from koukan.message_builder import MessageBuilder
from koukan.storage import Storage
from koukan.blob import Blob, FileLikeBlob
from koukan.rest_schema import BlobUri

class MessageBuilderFilter(SyncFilter):
    upstream : Optional[SyncFilter] = None
    body : Optional[Blob] = None

    def __init__(self, storage : Storage, upstream):
        self.storage = storage
        self.upstream = upstream

    # TODO probably OutputHandler should just load the blobs in the tx
    # the same as the body
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
        if self.body is None and tx.message_builder is None:
            return self.upstream.on_update(tx, tx_delta)

        built = False
        if tx_delta.message_builder is not None:
            builder = MessageBuilder(
                tx_delta.message_builder,
                lambda blob_id: self._blob_factory(tx.rest_id, blob_id))

            file = TemporaryFile('w+b')
            assert isinstance(file, IOBase)
            builder.build(file)
            file.flush()
            self.body = FileLikeBlob(file, finalized=True)
            logging.debug('MessageBuilderFilter.on_update %d %s',
                          self.body.len(), self.body.content_length())
            built = True

        downstream_tx = tx.copy()
        downstream_delta = tx_delta.copy()

        if downstream_tx.message_builder:
            downstream_tx.message_builder = None
        if downstream_delta.message_builder:
            downstream_delta.message_builder = None

        downstream_tx.body = self.body
        downstream_delta.body = self.body if built else None

        upstream_delta = self.upstream.on_update(
            downstream_tx, downstream_delta)
        assert tx.merge_from(upstream_delta) is not None
        return upstream_delta
