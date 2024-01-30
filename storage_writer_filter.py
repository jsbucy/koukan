from typing import Dict, Optional, Any
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

    def __init__(self, storage):
        self.storage = storage

    def _create(self):
        self.tx_cursor = self.storage.get_transaction_cursor()
        rest_id = secrets.token_urlsafe(REST_ID_BYTES)
        self.tx_cursor.create(rest_id)

    def on_update(self, tx : TransactionMetadata,
                  timeout : Optional[float] = None):
        if self.tx_cursor is None:
            self._create()
        if tx.durable:
            # xxx retry params from yaml
            self.tx_cursor.set_max_attempts(100)
            return
        while True:
            try:
                self.tx_cursor.write_envelope(tx)
                break
            except VersionConflictException:
                self.tx_cursor.load()
        start = time.monotonic()
        while True:
            done = True
            if self.tx_cursor.tx is None:
                done = False
            if done and self.tx_cursor.tx.mail_from is not None:
                if self.tx_cursor.tx.mail_response is not None:
                    tx.mail_response = self.tx_cursor.tx.mail_response
                else:
                    done = False
            if done and (len(self.tx_cursor.tx.rcpt_response) ==
                len(self.tx_cursor.tx.rcpt_to)):
                tx.rcpt_response = self.tx_cursor.tx.rcpt_response[-len(tx.rcpt_to):]
            else:
                done = False
            if done:
                break
            deadline_left = None
            if timeout is not None:
                timeout = (time.monotonic() - start)
            self.tx_cursor.wait(deadline_left)


    def append_data(self, last : bool, blob : Blob,
                    timeout : Optional[float] = None) -> Optional[Response]:
        no_blob_id = True
        if isinstance(blob, BlobReader):
            if (self.tx_cursor.append_blob(
                    blob_rest_id=blob.rest_id, last=last) ==
                TransactionCursor.APPEND_BLOB_OK):
                no_blob_id = False
        # TODO this could hit a precondition failure if it failed
        # upstream since the last append, need to check here
        if no_blob_id and (
                self.tx_cursor.append_blob(d=blob.contents(), last=last) !=
            TransactionCursor.APPEND_BLOB_OK):
            return Response.Internal('StorageWriterFilter append blob failed')

        if not last:
            return None

        start = time.monotonic()
        while self.tx_cursor.tx.data_response is None:
            timeout_left = None
            if timeout is not None:
                timeout_left = timeout - (time.monotonic() - start)
                if timeout_left <= 0:
                    break
            self.tx_cursor.wait(timeout_left)

        return self.tx_cursor.tx.data_response

    def abort(self):
        pass
