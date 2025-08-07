# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional
from tempfile import TemporaryFile
from io import IOBase

import logging

from koukan.blob import (
    FileLikeBlob,
    InlineBlob )
from koukan.filter import TransactionMetadata
from koukan.filter_chain import ProxyFilter
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

    async def on_update(self, tx_delta : TransactionMetadata, upstream):
        tx = self.downstream
        logging.debug('MessageParserFilter options %s', tx.options)

        body = tx_delta.maybe_body_blob()
        enabled = tx.options is not None and 'receive_parsing' in tx.options
        if enabled:
            tx_delta.body = None
        if not enabled or (body is not None and not body.finalized()):
            body = None
        self.upstream.merge_from(tx_delta)

        if body is None:
            assert self.downstream.merge_from(await upstream()) is not None
            return

        assert self.upstream.body is None

        parse_options = tx.options.get('receive_parsing', {})
        file = TemporaryFile('w+b')
        file.write(body.pread(0))
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
            spec.body_blob = tx.body
            self.upstream.body = spec
        else:
            # TODO option to fail/set data err on parse error?
            self.upstream_body = body

        assert self.downstream.merge_from(await upstream()) is not None
