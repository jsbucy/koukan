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
from koukan.filter_chain import FilterResult, OneshotProxyFilter
from koukan.response import Response
from koukan.message_builder import MessageBuilder, MessageBuilderSpec
from koukan.blob import Blob, FileLikeBlob, InlineBlob
from koukan.rest_schema import BlobUri

class MessageBuilderFilter(OneshotProxyFilter):
    validation : Optional[bool] = None

    def __init__(self):
        pass

    def on_update(self, tx_delta : TransactionMetadata):
        body = tx_delta.body
        if isinstance(body, MessageBuilderSpec):
            tx_delta.body = None
            if (body is not None and
                self.validation is not None and
                not body.finalized()):
                body = None
        else:
            body = None
        self.upstream.merge_from(tx_delta)

        if body is None:
            return FilterResult()

        assert self.validation is not False
        assert self.upstream.body is None

        if body.finalized():
            blobs = { blob.rest_id(): blob for blob in body.blobs }
        elif self.validation is None:
            # do a dry run with placeholder blobs so we can fastfail
            # on invalid json before we possibly hang on the upstream
            blobs = { blob.rest_id(): InlineBlob(b'xyz', last=True)
                      for blob in body.blobs }
        builder = MessageBuilder(body.json, blobs)

        upstream_body = None
        data_err = None
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
                upstream_body = FileLikeBlob(file, finalized=True)
            else:
                self.validation = True
        except:
            self.validation = False
            # last-ditch handling in case the json tickled a bug
            # TODO add a validator/fixup filter in front of this to
            # catch these errors
            logging.exception('unexpected exception in MessageBuilder')
            data_err = Response(
                550, 'unexpected exception in MessageBuilder, '
                'likely invalid message_builder json')

        if upstream_body:
            logging.debug('MessageBuilderFilter.on_update %d %s',
                          upstream_body.len(), upstream_body.content_length())
            self.upstream.body = upstream_body

        delta = None
        if data_err is not None:
            delta = TransactionMetadata(data_response = data_err)
        return FilterResult(delta)
