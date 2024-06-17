from typing import Any, Callable, Dict, List, Optional, Tuple
import logging
import json
import secrets
import time
from functools import partial

from storage import Storage, TransactionCursor, BlobReader, BlobWriter
from storage_schema import InvalidActionException, VersionConflictException
from response import Response
from filter import (
    AsyncFilter,
    HostPort,
    Mailbox,
    TransactionMetadata )
from blob import Blob
from deadline import Deadline
from message_builder import MessageBuilder

from rest_service import blob_to_uri, uri_to_blob


# this is only going to implement AsyncFilter
class StorageWriterFilter(AsyncFilter):
    storage : Storage
    tx_cursor : Optional[TransactionCursor] = None
    rest_id_factory : Optional[Callable[[], str]] = None
    rest_id : Optional[str] = None
    blob_writer : Optional[BlobWriter] = None

    def __init__(self, storage,
                 rest_id_factory : Optional[Callable[[], str]] = None,
                 rest_id : Optional[str] = None):
        self.storage = storage
        self.rest_id_factory = rest_id_factory
        self.rest_id = rest_id

    def version(self):
        # XXX
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
        elif isinstance(body_blob, BlobWriter):
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
        reuse_blob_rest_id=None

        def uri_to_blob2(tx_blob, uri):
            if uri.startswith('/transactions/'):
                if tx_blob[0] is None:
                    tx_blob[0] = True
                else:
                    assert tx_blob[0]
                u = uri.removeprefix('/transactions/')
                slash = u.find('/')
                out = u[slash+1:]
                out = out.removeprefix('blob/')
                logging.debug('uri_to_blob2 %s %s', uri, out)
                return out
            else:
                if tx_blob[0] is None:
                    tx_blob[0] = False
                else:
                    assert not tx_blob
                return uri_to_blob(uri)

        if tx_delta.message_builder:
            # xxx skip the /tx/123/blob ones
            # since they were ref'd when they were created
            tx_blob = [None]
            reuse_blob_rest_id = MessageBuilder.get_blobs(
                tx_delta.message_builder, partial(uri_to_blob2, tx_blob))
            logging.debug('StorageWriterFilter.update reuse_blob_rest_id %s',
                          reuse_blob_rest_id)
            if tx_blob:
                reuse_blob_rest_id = None

        deadline = Deadline(timeout)
        downstream_tx = tx.copy()
        if getattr(downstream_tx, 'rest_id', None) is not None:
            del downstream_tx.rest_id
        logging.debug('StorageWriterFilter.update downstream_tx %s',
                      downstream_tx)
        logging.debug('StorageWriterFilter.update delta %s', tx_delta)
        if tx_delta.body_blob is not None and self.blob_writer is None:
            body_blob = tx_delta.body_blob
            if ((isinstance(body_blob, BlobWriter) and
                 body_blob.length == body_blob.content_length) or (
                     tx_delta.body_blob.len() == tx_delta.body_blob.content_length())):
                self._body(tx_delta)
                if tx_delta.data_response is not None:
                    return  # XXX
                reuse_blob_rest_id=[tx_delta.body]
            del tx_delta.body_blob
        if downstream_tx.body_blob is not None:
            del downstream_tx.body_blob

        if self.rest_id is None:
            self._create(tx_delta, reuse_blob_rest_id=reuse_blob_rest_id)
        else:
            if self.tx_cursor is None:
                self._load()

            while True:
                try:
                    self.tx_cursor.write_envelope(
                        tx_delta, reuse_blob_rest_id=reuse_blob_rest_id)
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


