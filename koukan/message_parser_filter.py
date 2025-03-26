# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional
from tempfile import TemporaryFile
from io import IOBase

import logging

from koukan.blob import (
    FileLikeBlob,
    InlineBlob )
from koukan.filter import (
    SyncFilter,
    TransactionMetadata )
from koukan.message_parser import (
    MessageParser,
    ParsedMessage )

from koukan.message_builder import MessageBuilderSpec

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
            tx.body is not None and tx.body.finalized()):
            parse_options = tx.options.get('receive_parsing', {})
            parse_options = parse_options if parse_options else {}
            file = TemporaryFile('w+b')
            file.write(tx.body.pread(0))
            file.flush()
            file.seek(0)
            parser = MessageParser(
                self._blob_factory,
                max_inline=parse_options.get('max_inline', 65536))
            parsed_message = parser.parse(file)
            file.close()
            parsed = self.parsed = True
            if parsed_message is not None:
                self.parsed_delta = TransactionMetadata()
                spec = MessageBuilderSpec(
                    parsed_message.json, parsed_message.blobs)
                spec.check_ids()
                spec.body_blob = tx.body
                self.parsed_delta.body = spec


            # XXX else: data err?

        if self.parsed_delta is None:
            if self.upstream is None:
                return TransactionMetadata()
            return self.upstream.on_update(tx, tx_delta)

        # cf "filter chain" doc 2024/8/6, we can't add internal fields
        # to the downstream tx because it will cause a delta/conflict
        # when we do the next db read in the OutputHandler

        downstream_tx = tx.copy()
        del downstream_tx.body
        assert downstream_tx.merge_from(self.parsed_delta) is not None

        downstream_delta = tx_delta.copy()

        if parsed:
            del downstream_delta.body
            assert downstream_delta.merge_from(self.parsed_delta) is not None
        logging.debug(self.parsed_delta)
        logging.debug(downstream_tx)
        upstream_delta = self.upstream.on_update(
            downstream_tx, downstream_delta)
        assert upstream_delta is not None
        tx.merge_from(upstream_delta)
        return upstream_delta
