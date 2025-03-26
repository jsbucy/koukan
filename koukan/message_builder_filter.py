# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional
from tempfile import TemporaryFile
import logging
from io import IOBase

from koukan.filter import (
    SyncFilter,
    TransactionMetadata )
from koukan.message_builder import MessageBuilder, MessageBuilderSpec
from koukan.blob import Blob, FileLikeBlob
from koukan.rest_schema import BlobUri

class MessageBuilderFilter(SyncFilter):
    upstream : Optional[SyncFilter] = None
    body : Optional[Blob] = None

    def __init__(self, upstream):
        self.upstream = upstream

    def on_update(self, tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata] :
        logging.debug(tx.body)
        if ((tx.body is None) or (not isinstance(tx.body, MessageBuilderSpec))) and (self.body is None):
            return self.upstream.on_update(tx, tx_delta)

        built = False
        if tx_delta.body is not None and isinstance(tx_delta.body, MessageBuilderSpec):
            builder = MessageBuilder(
                tx_delta.body.json,
                # xxx always BlobCursor?
                { blob.rest_id(): blob for blob in tx_delta.body.blobs })

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

        downstream_tx.body = self.body
        downstream_delta.body = self.body if built else None

        upstream_delta = self.upstream.on_update(
            downstream_tx, downstream_delta)
        assert tx.merge_from(upstream_delta) is not None
        return upstream_delta
