# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional
from tempfile import TemporaryFile
import logging
from io import IOBase
from contextlib import nullcontext
from os import devnull

from koukan.filter import (
    SyncFilter,
    TransactionMetadata )
from koukan.response import Response
from koukan.message_builder import MessageBuilder, MessageBuilderSpec
from koukan.blob import Blob, FileLikeBlob, InlineBlob
from koukan.rest_schema import BlobUri

class MessageBuilderFilter(SyncFilter):
    upstream : Optional[SyncFilter] = None
    body : Optional[Blob] = None
    validation : Optional[bool]= None

    def __init__(self, upstream):
        self.upstream = upstream

    def on_update(self, tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata] :
        assert self.body is None
        assert self.validation is not False
        body = tx.body
        if not isinstance(body, MessageBuilderSpec):
            return self.upstream.on_update(tx, tx_delta)
        if not body.finalized() and body.json is None:
            downstream_tx = tx.copy()
            downstream_delta = tx_delta.copy()
            downstream_tx.body = downstream_delta.body = None
            upstream_delta = self.upstream.on_update(
                downstream_tx, downstream_delta)
            tx.merge_from(upstream_delta)
            return upstream_delta


        if body.finalized():
            blobs = { blob.rest_id(): blob for blob in tx_delta.body.blobs }
        elif self.validation is None:
            # do a dry run with placeholder blobs so we can fastfail
            # on invalid json before we possibly hang on the upstream
            blobs = { blob.rest_id(): InlineBlob(b'xyz', last=True)
                      for blob in tx_delta.body.blobs }
        builder = MessageBuilder(tx_delta.body.json, blobs)

        try:
            file = None
            if body.finalized():
                file = TemporaryFile('w+b')
                assert isinstance(file, IOBase)
            # FR for an internal-to-python sink file object
            # https://github.com/python/cpython/issues/73050
            with nullcontext(file) if file is not None else open(devnull, 'wb') as f:
                builder.build(f)
            if file is not None:
                file.flush()
                self.body = FileLikeBlob(file, finalized=True)
            else:
                self.validation = True
        except:
            self.validation = False
            # last-ditch handling in case the json tickled a bug
            # TODO add a validator/fixup filter in front of this to
            # catch these errors
            logging.exception('unexpected exception in MessageBuilder')
            err = TransactionMetadata()
            msg = ('unexpected exception in MessageBuilder, '
                   'likely invalid message_builder json')
            tx.fill_inflight_responses(Response(450, msg), err)
            err.data_response = Response(550, msg)
            tx.merge_from(err)
            return err

        if self.body:
            logging.debug('MessageBuilderFilter.on_update %d %s',
                          self.body.len(), self.body.content_length())

        downstream_tx = tx.copy()
        downstream_delta = tx_delta.copy()

        downstream_tx.body = downstream_delta.body = self.body

        upstream_delta = self.upstream.on_update(
            downstream_tx, downstream_delta)
        assert tx.merge_from(upstream_delta) is not None
        return upstream_delta
