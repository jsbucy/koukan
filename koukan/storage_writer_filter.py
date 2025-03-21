# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Callable, Dict, List, Optional, Tuple
import json
import logging
import secrets
from functools import partial
from threading import Lock, Condition

from koukan.backoff import backoff
from koukan.storage import Storage, TransactionCursor, BlobCursor
from koukan.storage_schema import BlobSpec, VersionConflictException
from koukan.response import Response
from koukan.filter import (
    AsyncFilter,
    HostPort,
    Mailbox,
    SyncFilter,
    TransactionMetadata )
from koukan.blob import Blob, InlineBlob, WritableBlob
from koukan.deadline import Deadline
from koukan.message_builder import MessageBuilder

from koukan.rest_schema import BlobUri, parse_blob_uri

class StorageWriterFilter(AsyncFilter):
    storage : Storage
    tx_cursor : Optional[TransactionCursor] = None
    rest_id_factory : Optional[Callable[[], str]] = None
    rest_id : Optional[str] = None
    create_leased : bool = False
    # leased cursor for cutthrough
    upstream_cursor : Optional[TransactionCursor] = None
    create_err : bool = False

    mu : Lock
    cv : Condition

    # xxx this seems dead but it's possible it's here to make the
    # exploder flow only ref the blob once?
    # body_blob_uri : bool = False

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
        # cursor can be None after first update() to create with the
        # cutthrough/handoff workflow
        if self.tx_cursor is None:
            self._load()
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
            if not self.cv.wait_for(
                    lambda: self.upstream_cursor is not None or
                    self.create_err, 30):
                logging.warning(
                    'StorageWriterFilter.get_transaction_cursor timeout')
                return None
            elif self.upstream_cursor is None:
                return None
            logging.debug('StorageWriterFilter.release_transaction_cursor')
            cursor = self.upstream_cursor
            self.upstream_cursor = None
            return cursor

    def version(self) -> Optional[int]:
        self._load()
        return self.tx_cursor.version

    def _create(self, tx : TransactionMetadata,
                blobs : Optional[List[BlobSpec]] = None):
        assert tx.host is not None
        self.tx_cursor = self.storage.get_transaction_cursor()
        rest_id = self.rest_id_factory()
        self.tx_cursor.create(
            rest_id, tx, blobs=blobs, create_leased=self.create_leased)
        with self.mu:
            self.rest_id = rest_id
            self.cv.notify_all()

    def _load(self):
        if self.tx_cursor is None:
            self.tx_cursor = self.storage.get_transaction_cursor(
                rest_id=self.rest_id)
        self.tx_cursor.load()

    # AsyncFilter
    def get(self) -> Optional[TransactionMetadata]:
        self._load()
        if self.tx_cursor.tx is None:
            return None
        tx = self.tx_cursor.tx.copy()
        tx.version = self.tx_cursor.version
        return tx

    # -> blobs to reuse, blobs to create in tx
    def _get_message_builder_blobs(
            self, tx) -> List[BlobSpec]:
        blobs = []
        def tx_blob_id(uri) -> Optional[str]:
            blob_uri = parse_blob_uri(uri)
            # xxx not uri.startswith('/')
            # other limits? ascii printable, etc?
            if blob_uri is None:
                blobs.append(BlobSpec(uri=BlobUri('', blob=uri)))
                return uri
            if blob_uri.tx_body:  # xxx bad?
                return None
            blobs.append(BlobSpec(uri=blob_uri))
            return blob_uri.blob

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
        needs_create = self.rest_id is None and self.create_leased
        try:
            upstream_delta = self._update(tx, tx_delta)
            if self.tx_cursor is not None:
                upstream_delta.version = tx.version = self.tx_cursor.version
            return upstream_delta
        finally:
            if needs_create and self.upstream_cursor is None:
                # i.e. uncaught exception
                with self.mu:
                    self.created_err = True
                    self.cv.notify_all()

    def _update(self,
               tx : TransactionMetadata,
               tx_delta : TransactionMetadata
               ) -> Optional[TransactionMetadata]:
        logging.debug('StorageWriterFilter._update tx %s %s',
                      self.rest_id, tx)
        logging.debug('StorageWriterFilter._update tx_delta %s %s',
                      self.rest_id, tx_delta)

        if tx_delta.cancelled:
            for i in range(0,5):
                assert self.tx_cursor is not None
                try:
                    # storage has special-case logic to noop if
                    # tx has final_attempt_reason
                    self.tx_cursor.write_envelope(
                        tx_delta, final_attempt_reason='downstream cancelled')
                    break
                except VersionConflictException:
                    if i == 4:
                        raise
                    backoff(i)
                    self.tx_cursor.load()
            assert self.tx_cursor is not None
            version = TransactionMetadata(version=self.tx_cursor.version)
            tx.merge_from(version)
            return version

        downstream_tx = tx.copy()
        downstream_delta = tx_delta.copy()
        if getattr(downstream_tx, 'rest_id', None) is not None:
            del downstream_tx.rest_id

        # TODO move to TransactionMetadata.from_json()?
        body_fields = 0
        for body_field in [ 'body', 'message_builder']:
            if getattr(tx, body_field):
                body_fields += 1
        if body_fields > 1:
            err_delta = TransactionMetadata(data_response=Response(
                550, 'invalid tx: multiple body fields (StorageWriterFilter)'))
            tx.merge_from(err_delta)
            return err_delta

        blobs = []
        if tx_delta.message_builder:
            blobs = self._get_message_builder_blobs(tx_delta)
            logging.debug('StorageWriterFilter.update blobs %s', blobs)
        elif tx.body:
            spec = BlobSpec()
            if isinstance(tx.body, BlobUri):
                spec.uri = tx.body
            elif isinstance(tx.body, Blob):
                if not isinstance(tx.body, BlobCursor):
                    spec.uri = BlobUri(tx_id='', tx_body=True)  # create
                else:  # xxx
                    spec.uri = tx.body.blob_uri
                spec.blob = tx.body
            blobs = [spec]

        created = False
        if self.rest_id is None:
            created = True
            self._create(downstream_tx, blobs=blobs))
            tx.rest_id = self.rest_id
        else:
            if self.tx_cursor is None:
                self._load()
            # caller handles VersionConflictException
            self.tx_cursor.write_envelope(downstream_delta, blobs=blobs)

        logging.debug('StorageWriterFilter.update %s result %s input tx %s',
                      self.rest_id, self.tx_cursor.tx, tx)

        version = TransactionMetadata(version=self.tx_cursor.version)

        if created and self.create_leased:
            with self.mu:
                self.upstream_cursor = self.tx_cursor
                self.tx_cursor = None
                self.cv.notify_all()

        # TODO even though this no longer does inflight waiting on the
        # upstream, it's possible one of the version conflict paths
        # yielded upstream responses?
        tx.merge_from(version)
        return version

    def get_blob_writer(self,
                        create : bool,
                        blob_rest_id : Optional[str] = None,
                        tx_body : Optional[bool] = None
                        ) -> Optional[WritableBlob]:
        assert tx_body or blob_rest_id
        blob = None
        if create:
            for i in range(0,5):
                try:
                    blob_uri = BlobUri(tx_id=self.rest_id,
                                       blob=blob_rest_id,
                                       tx_body=tx_body)
                    blob = self.storage.create_blob(blob_uri)
                    break
                except VersionConflictException:
                    if i == 4:
                        raise
                    backoff(i)
        else:
            blob = self.storage.get_blob_for_append(
                BlobUri(tx_id=self.rest_id, blob=blob_rest_id, tx_body=tx_body))

        return blob
