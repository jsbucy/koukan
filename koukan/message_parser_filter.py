# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional
from tempfile import TemporaryFile
from io import IOBase

import logging

from koukan.blob import (
    Blob,
    FileLikeBlob,
    InlineBlob )
from koukan.filter import TransactionMetadata
from koukan.filter_chain import FilterResult, ProxyFilter
from koukan.message_parser import (
    MessageParser,
    ParsedMessage )

from koukan.message_builder import MessageBuilderSpec

class MessageParserFilter(ProxyFilter):
    _blob_i = 0
    parsed : bool = False

    def __init__(self):
        pass

    def _blob_factory(self):
        file = TemporaryFile('w+b')
        assert isinstance(file, IOBase)
        blob_id = str(self._blob_i)
        self._blob_i += 1
        return FileLikeBlob(file, blob_id)

    def on_update(self, tx_delta : TransactionMetadata):
        tx = self.downstream_tx
        assert tx is not None
        logging.debug('MessageParserFilter options %s', tx.options)

        body = tx_delta.maybe_body_blob()
        enabled = tx.options is not None and 'receive_parsing' in tx.options
        if enabled:
            tx_delta.body = None
        if not enabled or (body is not None and not body.finalized()):
            body = None

        assert self.upstream_tx is not None
        self.upstream_tx.merge_from(tx_delta)

        if body is None:
            return FilterResult()

        assert self.upstream_tx.body is None

        parse_options = None
        if tx.options is not None:
            parse_options = tx.options.get('receive_parsing', {})
        if parse_options is None:
            parse_options = {}
        file = TemporaryFile('w+b')
        b = body.pread(0)
        assert b is not None
        file.write(b)
        file.flush()
        file.seek(0)
        parser = MessageParser(
            self._blob_factory,
            max_inline=parse_options.get('max_inline', 65536))
        parsed_message = parser.parse(file)
        file.close()
        self.parsed = True
        if parsed_message is not None:
            spec = MessageBuilderSpec(
                parsed_message.json, parsed_message.blobs)
            spec.check_ids()
            assert isinstance(tx.body, Blob)
            spec.body_blob = tx.body
            self.upstream_tx.body = spec
        else:
            # TODO option to fail/set data err on parse error?
            self.upstream_body = body

        return FilterResult()
