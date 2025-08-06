# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional
from tempfile import TemporaryFile
import logging
from io import IOBase
from contextlib import nullcontext
from os import devnull

from koukan.filter import (
    TransactionMetadata )
from koukan.filter_chain import ProxyFilter
from koukan.response import Response
from koukan.message_builder import MessageBuilder, MessageBuilderSpec
from koukan.blob import Blob, FileLikeBlob, InlineBlob
from koukan.rest_schema import BlobUri

class MessageBuilderFilter(ProxyFilter):
    body : Optional[Blob] = None
    validation : Optional[bool]= None

    def __init__(self):
        pass

    async def on_update(self, tx_delta : TransactionMetadata, upstream):
        assert self.body is None
        assert self.validation is not False

        body = None
        if isinstance(tx_delta.body, MessageBuilderSpec):
            body = tx_delta.body
            tx_delta.body = None  # xxx ok to mutate this delta?
        assert self.upstream.merge_from(tx_delta) is not None

        if body is None:
            upstream_delta = await upstream()
            assert self.downstream.merge_from(upstream_delta) is not None
            return

        if body.finalized():
            blobs = { blob.rest_id(): blob for blob in body.blobs }
        elif self.validation is None:
            # do a dry run with placeholder blobs so we can fastfail
            # on invalid json before we possibly hang on the upstream
            blobs = { blob.rest_id(): InlineBlob(b'xyz', last=True)
                      for blob in body.blobs }
        builder = MessageBuilder(body.json, blobs)

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
            self.downstream.fill_inflight_responses(
                Response(250, 'ok (MessageBuilderFilter no-op, '
                         'DATA will fail)'))
            self.downstream.data_response = Response(
                550, 'unexpected exception in MessageBuilder, '
                'likely invalid message_builder json')
            return

        if self.body:
            logging.debug('MessageBuilderFilter.on_update %d %s',
                          self.body.len(), self.body.content_length())

        self.upstream.body = self.body
        assert self.downstream.merge_from(await upstream()) is not None
