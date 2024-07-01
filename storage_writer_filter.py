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
    TransactionMetadata )
from blob import Blob, WritableBlob
from deadline import Deadline
from message_builder import MessageBuilder

from rest_schema import BlobUri, parse_blob_uri


# this is only going to implement AsyncFilter
class StorageWriterFilter(AsyncFilter):
    storage : Storage
    tx_cursor : Optional[TransactionCursor] = None
    rest_id_factory : Optional[Callable[[], str]] = None
    rest_id : Optional[str] = None
    blob_writer : Optional[BlobWriter] = None
    create_leased : bool = False

    mu : Lock
    cv : Condition

    def __init__(self, storage,
                 rest_id_factory : Optional[Callable[[], str]] = None,
                 rest_id : Optional[str] = None,
                 create_leased : bool = False):
        self.storage = storage
        self.rest_id_factory = rest_id_factory
        self.rest_id = rest_id
        self.mu = Lock()
        self.cv = Condition(self.mu)

    def get_rest_id(self):
        with self.mu:
            self.cv.wait_for(lambda: self.rest_id is not None)
            return self.rest_id

    def version(self):
        # XXX
        if self.tx_cursor is None:
            self._load()
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

    def _load(self):
        self.tx_cursor = self.storage.get_transaction_cursor()
        self.tx_cursor.load(rest_id=self.rest_id)

    # AsyncFilter
    def _get(self, deadline : Deadline) -> Optional[TransactionMetadata]:
        # XXX
        if self.tx_cursor is None:
            self._load()

        logging.debug('StorageWriterFilter._get %s %s %s', self.rest_id,
                      deadline.deadline_left(), self.tx_cursor.tx)

        while self.tx_cursor.tx.req_inflight():
            deadline_left = deadline.deadline_left()
            if not deadline.remaining(1):
                break
            logging.debug('StorageWriterFilter._get %s deadline_left %s '
                          'tx %s',
                          self.rest_id, deadline_left, self.tx_cursor.tx)
            self.tx_cursor.wait(deadline.deadline_left())

        logging.debug('StorageWriterFilter._get %s %s', self.rest_id,
                      self.tx_cursor.tx)

        return self.tx_cursor.tx.copy()

    def get(self, timeout : Optional[float] = None
            ) -> Optional[TransactionMetadata]:
        return self._get(Deadline(timeout))

    def _body(self, tx):
        body_blob = tx.body_blob
        if isinstance(body_blob, BlobReader):
            tx.body = body_blob.rest_id
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
    def update(self,
               tx : TransactionMetadata,
               tx_delta : TransactionMetadata,
               timeout : Optional[float] = None
               ) -> Optional[TransactionMetadata]:
        logging.debug('StorageWriterFilter.update tx %s %s',
                      self.rest_id, tx)
        logging.debug('StorageWriterFilter.update tx_delta %s %s',
                      self.rest_id, tx_delta)

        reuse_blob_rest_id=None

        if tx_delta.cancelled:
            self.tx_cursor.write_envelope(
                tx_delta, final_attempt_reason='downstream cancelled')
            return tx_delta

        def tx_blob_id(uri):
            uri = parse_blob_uri(uri)
            if uri is None:
                return None
            if uri.tx_body:
                return None
            return uri.blob

        downstream_tx = tx.copy()
        downstream_delta = tx_delta.copy()
        if getattr(downstream_tx, 'rest_id', None) is not None:
            del downstream_tx.rest_id

        if downstream_delta.message_builder:
            reuse_blob_rest_id = MessageBuilder.get_blobs(
                downstream_delta.message_builder, tx_blob_id)
            logging.debug('StorageWriterFilter.update reuse_blob_rest_id %s',
                          reuse_blob_rest_id)

        deadline = Deadline(timeout)

        # internal paths: Exploder/Notification (rest uses get_blob_writer())
        body_blob = None
        if downstream_delta.body_blob is not None:
            body_blob = downstream_delta.body_blob
            if body_blob.finalized():
                self._body(downstream_delta)
                if downstream_delta.data_response is not None:
                    return  # XXX
                reuse_blob_rest_id=[downstream_delta.body]
            del downstream_delta.body_blob
        elif downstream_delta.body:
            uri = parse_blob_uri(downstream_delta.body)
            assert uri and uri.tx_body
            source_cursor = self.storage.get_transaction_cursor()
            source_cursor.load(rest_id=uri.tx_id)
            downstream_tx.body = downstream_delta.body = source_cursor.body_rest_id
            reuse_blob_rest_id = [source_cursor.body_rest_id]

        if downstream_tx.body_blob is not None:
            del downstream_tx.body_blob


        created = False
        if self.rest_id is None:
            created = True
            body_utf8 = None
            if downstream_delta.inline_body:
                # TODO need to worry about roundtripping
                # utf8 -> python str -> utf8 is ever lossy?
                body_utf8 = tx_delta.inline_body.encode('utf-8')
                del downstream_tx.inline_body
                del downstream_delta.inline_body

            self._create(downstream_delta,
                         reuse_blob_rest_id=reuse_blob_rest_id)
            reuse_blob_rest_id = None

            while True:
                try:
                    if body_blob is not None:
                        pass
                    elif body_utf8 is not None:
                        logging.debug('StorageWriterFilter inline body %d',
                                      len(body_utf8))
                        writer = self.storage.create_blob(
                            tx_rest_id=self.rest_id,
                            blob_rest_id=self.rest_id_factory(),
                            tx_body=True)
                        writer.append_data(0, body_utf8, len(body_utf8))
                        # create_blob() refs into tx for tx_body
                    break
                except VersionConflictException:
                    pass

            downstream_delta = TransactionMetadata()

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

        logging.debug('StorageWriterFilter.update %s %s result %s',
                      self.rest_id, timeout, self.tx_cursor.tx)

        updated = self._get(deadline)
        logging.debug('StorageWriterFilter.update %s updated %s',
                      self.rest_id, updated)

        upstream_delta = downstream_tx.delta(updated)
        assert upstream_delta is not None
        assert len(upstream_delta.rcpt_response) <= len(tx.rcpt_to)

        tx.merge_from(upstream_delta)
        tx.rest_id = self.rest_id
        upstream_delta.rest_id = self.rest_id
        return upstream_delta


    def get_blob_writer(self,
                        create : bool,
                        blob_rest_id : Optional[str] = None,
                        tx_body : Optional[bool] = None
                        ) -> Optional[WritableBlob]:
        assert tx_body or blob_rest_id

        if create:
            while True:
                try:
                    blob = self.storage.create_blob(
                        tx_rest_id=self.rest_id,
                        blob_rest_id=blob_rest_id,
                        tx_body=tx_body)
                    break
                except VersionConflictException:
                    pass
        else:
            blob = self.storage.get_blob_for_append(
                tx_rest_id=self.rest_id, blob_rest_id=blob_rest_id)

        return blob
