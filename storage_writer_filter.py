from typing import Any, Callable, Dict, List, Optional, Tuple
import logging
import json
import secrets
import time
from functools import partial
from threading import Lock, Condition

from storage import Storage, TransactionCursor, BlobCursor
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
    # none if still inflight, true if success, false if err/uncaught exception
    created : Optional[bool] = None

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
        self.create_leased = create_leased
        self.mu = Lock()
        self.cv = Condition(self.mu)

    # AsyncFilter
    def wait(self, version, timeout) -> bool:
        if self.tx_cursor.version != version:
            return True
        return self.tx_cursor.wait(timeout)

    # AsyncFilter
    async def wait_async(self, version, timeout):
        if self.tx_cursor.version != version:
            return True
        return await self.tx_cursor.wait_async(timeout)

    def release_transaction_cursor(self) -> Optional[TransactionCursor]:
        with self.mu:
            if not self.cv.wait_for(lambda: self.created is not None, 30):
                logging.warning(
                    'StorageWriterFilter.get_transaction_cursor timeout')
                return None
            elif not self.created:
                return None
            logging.debug('StorageWriterFilter.release_transaction_cursor')
            cursor = self.tx_cursor
            self.tx_cursor = None
            return cursor

    def version(self) -> Optional[int]:
        if self.tx_cursor is None:
            self.tx_cursor = self.storage.get_transaction_cursor(
                rest_id=self.rest_id)
        return self.tx_cursor.version

    def _create(self, tx : TransactionMetadata,
                reuse_blob_rest_id : Optional[List[str]] = None,
                message_builder_blobs_done=False):
        assert tx.host is not None
        self.tx_cursor = self.storage.get_transaction_cursor()
        rest_id = self.rest_id_factory()
        self.tx_cursor.create(
            rest_id, tx, reuse_blob_rest_id=reuse_blob_rest_id,
            create_leased=self.create_leased,
            message_builder_blobs_done=message_builder_blobs_done)
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
        # xxx races with release_transaction_cursor()
        with self.mu:
            self._load()
            tx = self.tx_cursor.tx.copy()
            tx.version = self.tx_cursor.version
            return tx

    def _get_body_blob_uri(self, tx) -> Optional[BlobUri]:
        logging.debug('_get_body_blob_uri')
        if self.body_blob_uri:
            return None
        if not tx.body and (not tx.body_blob or not tx.body_blob.finalized()):
            return None
        if isinstance(tx.body_blob, BlobCursor):
            self.body_blob_uri = True
            return tx.body_blob.blob_uri
        elif tx.body:
            self.body_blob_uri = True
            uri = parse_blob_uri(tx.body)
            logging.debug('_get_body_blob_uri %s %s', tx.body, uri)
            # xxx parse error -> err Response
            assert uri and uri.tx_body
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

    # -> blobs to reuse, blobs to create in tx
    def _get_message_builder_blobs(
            self, tx) -> Tuple[List[BlobUri], List[str]]:
        blobs = []
        create_blobs = []
        def tx_blob_id(uri) -> Optional[str]:
            blob_uri = parse_blob_uri(uri)
            # xxx not uri.startswith('/')
            # other limits? ascii printable, etc?
            if blob_uri is None:
                create_blobs.append(uri)
                return uri
            if blob_uri.tx_body:
                return None
            blobs.append(blob_uri)
            return blob_uri.blob

        if not tx.message_builder:
            return [], []

        # TODO maybe this should this blob-ify larger inline content in
        # message_builder a la _maybe_write_body_blob() with inline_body?

        MessageBuilder.get_blobs(tx.message_builder, tx_blob_id)

        return blobs, create_blobs

    # AsyncFilter
    def update(self,
               tx : TransactionMetadata,
               tx_delta : TransactionMetadata
               ) -> Optional[TransactionMetadata]:
        needs_create = self.rest_id is None
        try:
            upstream_delta = self._update(tx, tx_delta)
            if self.tx_cursor is not None:
                upstream_delta.version = tx.version = self.tx_cursor.version
            return upstream_delta
        finally:
            if needs_create and self.created is None:
                # i.e. uncaught exception
                with self.mu:
                    self.created = False
                    self.cv.notify_all()

    def _update(self,
               tx : TransactionMetadata,
               tx_delta : TransactionMetadata
               ) -> Optional[TransactionMetadata]:
        logging.debug('StorageWriterFilter.update tx %s %s',
                      self.rest_id, tx)
        logging.debug('StorageWriterFilter.update tx_delta %s %s',
                      self.rest_id, tx_delta)

        reuse_blob_rest_id : Optional[List[BlobUri]] = None

        if tx_delta.cancelled:
            while True:
                try:
                    self.tx_cursor.write_envelope(
                        tx_delta, final_attempt_reason='downstream cancelled')
                    break
                except VersionConflictException:
                    self.tx_cursor.load()
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

        # TODO move message builder blob creation into
        # cursor.create/write_env w/message builder?
        reuse_blob_rest_id, create_blobs = (
            self._get_message_builder_blobs(tx_delta))
        logging.debug('StorageWriterFilter.update reuse_blob_rest_id %s',
                      reuse_blob_rest_id)

        # finalized body_blob w/uri:
        # rest body reuse (body is uri)
        # exploder (body_blob is storage.BlobCursor)

        if body_blob_uri := self._get_body_blob_uri(downstream_tx):
            logging.debug('body_blob_uri %s', body_blob_uri)
            assert not reuse_blob_rest_id
            reuse_blob_rest_id = [body_blob_uri]
            # XXX
            if not downstream_delta.body:
                downstream_delta.body = 'b'

        if downstream_tx.body_blob is not None:
            del downstream_tx.body_blob
        if downstream_delta.body_blob is not None:
            del downstream_delta.body_blob

        created = False
        if self.rest_id is None:
            created = True
            self._create(downstream_tx,
                         reuse_blob_rest_id=reuse_blob_rest_id,
                         message_builder_blobs_done=not bool(create_blobs))
            reuse_blob_rest_id = None
            tx.rest_id = self.rest_id

        for blob in create_blobs:
            self.get_blob_writer(create=True, blob_rest_id = blob)

        # blobs w/o uri:
        # notification/dsn: InlineBlob w/dsn,
        # exploder currently sends downstream BlobCursor verbatim but
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

        if created:
            with self.mu:
                self.created = True
                self.cv.notify_all()

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
