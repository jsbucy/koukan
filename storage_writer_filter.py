from typing import Callable, Dict, List, Optional, Any
import logging
import json
import secrets
import time

from storage import Storage, TransactionCursor, BlobReader, BlobWriter
from storage_schema import InvalidActionException, VersionConflictException
from response import Response
from filter import AsyncFilter, Filter, HostPort, Mailbox, TransactionMetadata
from blob import Blob

# this is only going to implement AsyncFilter
class StorageWriterFilter(AsyncFilter):
    storage : Storage
    tx_cursor : Optional[TransactionCursor] = None
    rest_id_factory : Optional[Callable[[], str]] = None
    rest_id : Optional[str] = None

    def __init__(self, storage,
                 rest_id_factory : Optional[Callable[[], str]] = None,
                 rest_id : Optional[str] = None):
        self.storage = storage
        self.rest_id_factory = rest_id_factory
        self.rest_id = rest_id

    def version(self):
        # XXX BEFORE SUBMIT
        if self.tx_cursor is None:
            self._load()
        return self.tx_cursor.version

    def _create(self, tx : TransactionMetadata,
                reuse_blob_rest_id : Optional[List[str]] = None):
        assert tx.host is not None
        self.tx_cursor = self.storage.get_transaction_cursor()
        self.rest_id = self.rest_id_factory()
        self.tx_cursor.create(
            self.rest_id, tx, reuse_blob_rest_id=reuse_blob_rest_id)

    def _load(self):
        self.tx_cursor = self.storage.get_transaction_cursor()
        self.tx_cursor.load(rest_id=self.rest_id)

    # AsyncFilter
    def get(self, timeout : Optional[float] = None
            ) -> Optional[TransactionMetadata]:
        # XXX BEFORE SUBMIT
        if self.tx_cursor is None:
            self._load()

        logging.debug('StorageWriterFilter.get %s %s %s', self.rest_id, timeout,
                      self.tx_cursor.tx)

        start = time.monotonic()
        deadline = start + timeout if timeout is not None else None
        while self.tx_cursor.tx.req_inflight():
            deadline_left = None
            if timeout is not None:
                deadline_left = deadline - time.monotonic()
                if deadline_left <= 0:
                    break
                logging.debug('StorageWriterFilter.get %s deadline_left %s '
                              'tx %s',
                              self.rest_id, deadline_left, self.tx_cursor.tx)
            self.tx_cursor.wait(deadline_left)

        logging.debug('StorageWriterFilter.get %s %s', self.rest_id,
                      self.tx_cursor.tx)

        return self.tx_cursor.tx

    def _body(self, tx):
        if isinstance(tx.body_blob, BlobReader):
            tx.body = tx.body_blob.rest_id
            return

        blob_writer = self.storage.get_blob_writer()
        # xxx this should be able to use storage id instead of rest id
        rest_id = self.rest_id_factory()
        blob_writer.create(rest_id)
        d = tx.body_blob.read(0)
        appended, length, content_length = blob_writer.append_data(
            0, d, len(d))
        if not appended or length != content_length or length != len(d):
            tx.data_response = Response(
                400, 'StorageWriterFilter: internal error')
        else:
            tx.body=rest_id


    # AsyncFilter
    def update(self, tx : TransactionMetadata,
               timeout : Optional[float] = None):
        reuse_blob_rest_id=None
        logging.debug('StorageWriterFilter.update body_blob %s',
                      tx.body_blob)
        if tx.body_blob is not None and (
                tx.body_blob.len() == tx.body_blob.content_length()):
            self._body(tx)
            if tx.data_response is not None:
                return
            reuse_blob_rest_id=[tx.body]
        # XXX BEFORE SUBMIT have been playing fast&loose with these mutations
        del tx.body_blob

        if self.rest_id is None:
            self._create(tx, reuse_blob_rest_id=reuse_blob_rest_id)
        else:
            if self.tx_cursor is None:
                self._load()

            while True:  # tx
                try:
                    self.tx_cursor.write_envelope(
                        tx, reuse_blob_rest_id=reuse_blob_rest_id)
                    break
                except VersionConflictException:
                    self.tx_cursor.load()

        logging.debug('StorageWriterFilter.update %s %s result %s',
                      self.rest_id, timeout, self.tx_cursor.tx)

        # xxx timeout_left, update might have taken some time
        updated = self.get(timeout)
        tx.replace_from(updated)
        tx.rest_id = self.rest_id
