from typing import Optional
import copy
from tempfile import TemporaryFile
import logging

from filter import Filter, TransactionMetadata
from message_builder import MessageBuilder
from storage import Storage
from blob import FileLikeBlob

class MessageBuilderFilter(Filter):
    def __init__(self, storage : Storage, upstream):
        self.storage = storage
        self.upstream = upstream

    def _blob_factory(self, blob_id):
        blob_reader = self.storage.get_blob_reader()
        blob_reader.load(rest_id=blob_id)
        return blob_reader

    def on_update(self, tx : TransactionMetadata):
        if tx.message_builder is None:
            return self.upstream.on_update(tx)

        builder = MessageBuilder(tx.message_builder,
                                 lambda blob_id: self._blob_factory(blob_id))

        file = TemporaryFile('w+b')
        builder.build(file)
        upstream_tx = copy.copy(tx)
        del upstream_tx.message_builder
        file.flush()

        body_blob = FileLikeBlob(file)
        upstream_tx.body_blob = body_blob
        logging.debug('MessageBuilderFilter.on_update %d %d',
                      body_blob.len(), body_blob.content_length())

        self.upstream.on_update(upstream_tx)
        tx.mail_response = upstream_tx.mail_response
        tx.rcpt_response = upstream_tx.rcpt_response
        tx.data_response = upstream_tx.data_response

    def abort(self):
        pass
