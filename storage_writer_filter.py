from typing import Callable, Dict, Optional, Any
import logging
import json
import secrets
import time

from storage import Storage, TransactionCursor, BlobReader, BlobWriter
from storage_schema import InvalidActionException, VersionConflictException
from response import Response
from filter import Filter, HostPort, Mailbox, TransactionMetadata
from blob import Blob

REST_ID_BYTES = 4  # XXX configurable, use more in prod

class StorageWriterFilter(Filter):
    storage : Storage
    tx_cursor : Optional[TransactionCursor] = None
    blob_id_factory : Callable[[], str]

    def __init__(self, storage,
                 blob_id_factory : Callable[[], str]):
        self.storage = storage
        self.blob_id_factory = blob_id_factory

    def _create(self, tx : TransactionMetadata):
        assert tx.host is not None
        self.tx_cursor = self.storage.get_transaction_cursor()
        rest_id = secrets.token_urlsafe(REST_ID_BYTES)
        self.tx_cursor.create(rest_id, tx)

    def on_update(self, tx : TransactionMetadata,
                  timeout : Optional[float] = None):
        if self.tx_cursor is None:
            self._create(tx)
        else:
            while True:
                try:
                    self.tx_cursor.write_envelope(tx)
                    break
                except VersionConflictException:
                    self.tx_cursor.load()

        wait_data_resp = False
        if tx.body_blob:
            logging.debug('on_update len %d content %s',
                          tx.body_blob.len(), tx.body_blob.content_length())
        if tx.body_blob and (
                tx.body_blob.len() == tx.body_blob.content_length()):
            # TODO this assumes cursor_to_endpoint buffers the entire
            # payload and nothing between there and here re-chunks it
            body_tx = TransactionMetadata()
            # xxx move this into Storage
            if isinstance(tx.body_blob, BlobReader):
                body_tx.body = tx.body_blob.rest_id
            else:
                blob_writer = self.storage.get_blob_writer()
                # xxx this should be able to use storage id instead of rest id
                rest_id = self.blob_id_factory()
                blob_writer.create(rest_id)
                d = tx.body_blob.read(0)
                blob_writer.append_data(d, len(d))
                body_tx.body=rest_id
            while True:
                try:
                    self.tx_cursor.write_envelope(
                        body_tx, reuse_blob_rest_id=[body_tx.body])
                    break
                except VersionConflictException:
                    self.tx_cursor.load()
            wait_data_resp = True

        start = time.monotonic()
        timed_out = False
        while True:
            done = True
            if self.tx_cursor.tx is None:
                done = False
            if done and tx.mail_from is not None and tx.mail_response is None:
                if self.tx_cursor.tx.mail_response is not None:
                    tx.mail_response = self.tx_cursor.tx.mail_response
                elif timed_out:
                    tx.mail_response = Response(
                        400, 'storage writer filter upstream timeout MAIL')
                else:
                    done = False
            # storage returns the full vector but we return the delta
            rcpt_response = self.tx_cursor.tx.rcpt_response[-len(tx.rcpt_to):]
            for i in range(0, len(tx.rcpt_to)):
                if len(tx.rcpt_response) <= i:
                    tx.rcpt_response.append(None)
                if tx.rcpt_response[i] is not None:
                    continue
                elif timed_out:
                    tx.rcpt_response[i] = Response(
                        400, 'storage writer filter upstream timeout RCPT')
                elif i >= len(rcpt_response):
                    done = False
                elif rcpt_response[i] is not None:
                    tx.rcpt_response[i] = rcpt_response[i]
                else:
                    done = False

            if done and wait_data_resp:
                if self.tx_cursor.tx.data_response:
                    tx.data_response = self.tx_cursor.tx.data_response
                elif timed_out:
                    tx.data_response = Response(
                        400, 'storage writer filter upstream timeout DATA')
                else:
                    done = False
            if done:
                break
            deadline_left = None
            if timeout is not None:
                assert not timed_out
                deadline_left = timeout - (time.monotonic() - start)
                if deadline_left <= 0:
                    timed_out = True
                    continue
            self.tx_cursor.wait(deadline_left)

    def abort(self):
        pass
