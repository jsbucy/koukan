# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional
from tempfile import TemporaryFile
import logging
from io import IOBase

from koukan.filter import (
    SyncFilter,
    TransactionMetadata )
from koukan.response import Response
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
        body = tx.body
        if not isinstance(body, MessageBuilderSpec):
            return self.upstream.on_update(tx, tx_delta)
        if not body.finalized():
            # TODO fastfail on invalid tx.body.json here
            downstream_tx = tx.copy()
            downstream_delta = tx_delta.copy()
            downstream_tx.body = downstream_delta.body = None
            upstream_delta = self.upstream.on_update(
                downstream_tx, downstream_delta)
            tx.merge_from(upstream_delta)
            return upstream_delta


        builder = MessageBuilder(
            tx_delta.body.json,
            # xxx always BlobCursor?
            { blob.rest_id(): blob for blob in tx_delta.body.blobs })

        try:
            file = TemporaryFile('w+b')
            assert isinstance(file, IOBase)
            builder.build(file)
            file.flush()
            self.body = FileLikeBlob(file, finalized=True)
        except:
            # last-ditch handling in case the json tickled a bug

            # TODO I'm not confident the client will figure out
            # what the actual problem is from these error
            # responses. We need to do some validation of the
            # message builder spec closer to RestHandler so that
            # it can return an http 400 synchronously to the
            # original tx creation POST.
            logging.exception('unexpected exception in MessageBuilder')
            err = TransactionMetadata()
            msg = ('unexpected exception in MessageBuilder, '
                   'likely invalid message_builder json')
            tx.fill_inflight_responses(Response(450, msg), err)
            err.data_response = Response(550, msg)
            tx.merge_from(err)
            return err

        logging.debug('MessageBuilderFilter.on_update %d %s',
                      self.body.len(), self.body.content_length())

        downstream_tx = tx.copy()
        downstream_delta = tx_delta.copy()

        downstream_tx.body = downstream_delta.body = self.body

        upstream_delta = self.upstream.on_update(
            downstream_tx, downstream_delta)
        assert tx.merge_from(upstream_delta) is not None
        return upstream_delta
