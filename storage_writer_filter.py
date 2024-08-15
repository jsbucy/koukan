from typing import Any, Callable, Dict, List, Optional, Tuple
import logging
import json
import secrets
import time
from functools import partial
from threading import Lock, Condition

from storage import Storage, TransactionCursor, BlobReader, BlobWriter
from storage_schema import InvalidActionException, VersionConflictException
from response import Response
from filter import (
    AsyncFilter,
    HostPort,
    Mailbox,
    SyncFilter,
    TransactionMetadata )
from blob import Blob, InlineBlob, WritableBlob
from deadline import Deadline
from message_builder import MessageBuilder

from rest_schema import BlobUri, parse_blob_uri

class StorageWriterFilter(AsyncFilter):
    storage : Storage
    tx_cursor : Optional[TransactionCursor] = None
    rest_id_factory : Optional[Callable[[], str]] = None
    rest_id : Optional[str] = None
    create_leased : bool = False

    mu : Lock
    cv : Condition

    body_blob_uri : bool = False

    def __init__(self, storage,
                 rest_id_factory : Optional[Callable[[], str]] = None,
                 rest_id : Optional[str] = None,
                 create_leased : bool = False):
        self.storage = storage
        self.rest_id_factory = rest_id_factory
        self.rest_id = rest_id
        self.mu = Lock()
        self.cv = Condition(self.mu)

    # AsyncFilter
    def wait(self, timeout) -> bool:
        return self.tx_cursor.wait(timeout)

    # AsyncFilter
    async def wait_async(self, timeout):
        return await self.tx_cursor.wait_async(timeout)

    def get_rest_id(self):
        with self.mu:
            self.cv.wait_for(lambda: self.rest_id is not None)
            return self.rest_id

    def version(self) -> Optional[int]:
        if self.tx_cursor is None:
            self.tx_cursor = self.storage.get_transaction_cursor(
                rest_id=self.rest_id)
        return self.tx_cursor.version

    def _create(self, tx : TransactionMetadata,
                reuse_blob_rest_id : Optional[List[str]] = None):
        assert tx.host is not None
        self.tx_cursor = self.storage.get_transaction_cursor()
        rest_id = self.rest_id_factory()
        self.tx_cursor.create(
            rest_id, tx, reuse_blob_rest_id=reuse_blob_rest_id,
            create_leased=self.create_leased)
        with self.mu:
            self.rest_id = rest_id
            self.cv.notify_all()

    def _load_tx(self):
        while True:
            try:
                self.tx_cursor.load()
                break
            except VersionConflictException:
                pass

    def _load(self):
        if self.tx_cursor is None:
            self.tx_cursor = self.storage.get_transaction_cursor(
                rest_id=self.rest_id)
        self._load_tx()

    # AsyncFilter
    def get(self) -> Optional[TransactionMetadata]:
        self._load()
        return self.tx_cursor.tx.copy()

    def _get_body_blob_uri(self, tx) -> Optional[BlobUri]:
        logging.debug('_get_body_blob_uri')
        if self.body_blob_uri:
            return None
        if not tx.body and (not tx.body_blob or not tx.body_blob.finalized()):
            return None
        if isinstance(tx.body_blob, BlobReader):
            self.body_blob_uri = True
            return BlobUri(tx_id='', blob=tx.body_blob.rest_id)  # XXX
        elif tx.body:
            self.body_blob_uri = True
            uri = parse_blob_uri(tx.body)
            # xxx parse error -> err Response
            assert uri and uri.tx_body
            # XXX move to Storage
            source_cursor = self.storage.get_transaction_cursor()
            source_cursor.load(rest_id=uri.tx_id)
            # should be able to eliminate tx table body_rest_id col
            # it's always blobrefs(tx_id, '__koukan_internal_tx_body')
            # etc
            uri.blob = tx.body = source_cursor.body_rest_id
            return uri
        return None

    # -> data err
    def _maybe_write_body_blob(self, tx) -> Optional[Response]:
        # XXX maybe RestHandler should deal with inline_body before it
        # gets here?
        logging.debug('_maybe_write_body_blob inline %s blob %s',
                      len(tx.inline_body) if tx.inline_body else None,
                      tx.body_blob)
        if tx.inline_body:
            # TODO need to worry about roundtripping
            # utf8 -> python str -> utf8 is ever lossy?
            body_utf8 = tx.inline_body.encode('utf-8')
            body_blob = InlineBlob(body_utf8, len(body_utf8))
            # xxx downstream_tx/delta in caller
            del tx.inline_body
        elif tx.body_blob and tx.body_blob.finalized():
            body_blob = tx.body_blob
        else:
            return

        # refs into tx
        blob_writer = self.get_blob_writer(
            create=True, blob_rest_id=self.rest_id_factory(), tx_body=True)
        if blob_writer is None:
            return Response(400, 'StorageWriterFilter: internal error')
        off = 0
        while True:
            CHUNK_SIZE = 1048576
            d = body_blob.read(0, CHUNK_SIZE)
            last = len(d) < CHUNK_SIZE
            content_length = off + len(d) if last else None
            logging.debug('_maybe_write_body_blob %s %d %s',
                          off, len(d), content_length)
            appended, length, content_length_out = blob_writer.append_data(
                off, d, content_length)
            if (not appended or length != (off + len(d)) or
                content_length_out != content_length):
                return Response(400, 'StorageWriterFilter: internal error')
            off = length
            if last:
                break

    def _get_message_builder_blobs(self, tx) -> List[BlobUri]:
        blobs = []
        def tx_blob_id(uri) -> Optional[str]:
            uri = parse_blob_uri(uri)
            if uri is None:
                return None
            # this is unlikely but ought to work? reuse another tx body
            # as a message_builder blob? fix w/BlobUri
            if uri.tx_body:
                return None
            blobs.append(uri)
            return uri.blob

        if not tx.message_builder:
            return []

        # TODO maybe this should this blob-ify larger inline content in
        # message_builder a la _maybe_write_body_blob() with inline_body?

        MessageBuilder.get_blobs(tx.message_builder, tx_blob_id)
        return blobs

    # AsyncFilter
    def update(self,
               tx : TransactionMetadata,
               tx_delta : TransactionMetadata
               ) -> Optional[TransactionMetadata]:
        logging.debug('StorageWriterFilter.update tx %s %s',
                      self.rest_id, tx)
        logging.debug('StorageWriterFilter.update tx_delta %s %s',
                      self.rest_id, tx_delta)

        reuse_blob_rest_id : Optional[List[BlobUri]] = None

        if tx_delta.cancelled:
            self.tx_cursor.write_envelope(
                tx_delta, final_attempt_reason='downstream cancelled')
            return TransactionMetadata()

        downstream_tx = tx.copy()
        downstream_delta = tx_delta.copy()
        if getattr(downstream_tx, 'rest_id', None) is not None:
            del downstream_tx.rest_id

        # TODO move to TransactionMetadata.from_json()?
        body_fields = 0
        for body_field in [
                'body', 'body_blob', 'inline_body', 'message_builder']:
            if getattr(tx, body_field):
                body_fields += 1
        if body_fields > 1:
            err_delta = TransactionMetadata(data_response=Response(
                550, 'internal error (StorageWriterFilter)'))
            tx.merge_from(err_delta)
            return err_delta

        reuse_blob_rest_id = self._get_message_builder_blobs(tx_delta)
        logging.debug('StorageWriterFilter.update reuse_blob_rest_id %s',
                      reuse_blob_rest_id)

        # finalized body_blob w/uri:
        # rest body reuse (body is uri)
        # exploder (body_blob is storage.BlobReader)

        if body_blob_uri := self._get_body_blob_uri(downstream_tx):
            logging.debug('body_blob_uri %s', body_blob_uri)
            assert not reuse_blob_rest_id
            reuse_blob_rest_id = [body_blob_uri]
            if not downstream_delta.body:
                downstream_delta.body = body_blob_uri.blob

        if downstream_tx.body_blob is not None:
            del downstream_tx.body_blob
        if downstream_delta.body_blob is not None:
            del downstream_delta.body_blob

        created = False
        if self.rest_id is None:
            created = True
            self._create(downstream_tx,
                         reuse_blob_rest_id=reuse_blob_rest_id)
            reuse_blob_rest_id = None
            tx.rest_id = self.rest_id

        # blobs w/o uri:
        # notification/dsn: InlineBlob w/dsn,
        # exploder currently sends downstream BlobReader verbatim but
        # could chain received header which would send us CompositeBlob
        if body_blob_uri is None:
            err = self._maybe_write_body_blob(tx_delta)
            if err:
                err_delta = TransactionMetadata(data_response=err)
                tx.merge_from(err_delta)
                return err_delta
            assert not downstream_delta.body

        # reuse_blob_rest_id is only
        # POST /tx/123/message_builder  ?
        # new body upload handled via get_blob_reader()
        if not created or reuse_blob_rest_id:
            if self.tx_cursor is None:
                self._load()

            while True:
                try:
                    self.tx_cursor.write_envelope(
                        downstream_delta, reuse_blob_rest_id=reuse_blob_rest_id)
                    break
                except VersionConflictException:
                    self.tx_cursor.load()
                    logging.debug('StorageWriterFilter.update conflict %s',
                                  self.tx_cursor.tx)

        logging.debug('StorageWriterFilter.update %s result %s '
                      'input tx %s',
                      self.rest_id, self.tx_cursor.tx, tx)

        # TODO even though this no longer does inflight waiting on the
        # upstream, it's possible one of the version conflict paths
        # yielded upstream responses?
        return TransactionMetadata()

    def get_blob_writer(self,
                        create : bool,
                        blob_rest_id : Optional[str] = None,
                        tx_body : Optional[bool] = None
                        ) -> Optional[WritableBlob]:
        assert tx_body or blob_rest_id

        if create:
            while True:
                try:
                    blob_uri = BlobUri(tx_id=self.rest_id,
                                       blob=blob_rest_id,
                                       tx_body=tx_body)
                    blob = self.storage.create_blob(blob_uri)
                    break
                except VersionConflictException:
                    pass
        else:
            blob = self.storage.get_blob_for_append(
                BlobUri(tx_id=self.rest_id, blob=blob_rest_id))

        return blob
