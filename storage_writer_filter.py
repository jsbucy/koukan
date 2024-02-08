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
            assert isinstance(tx.body_blob, BlobReader)
            body_tx = TransactionMetadata()
            body_tx.body = tx.body_blob.rest_id
            self.tx_cursor.write_envelope(body_tx)
            wait_data_resp = True

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
                # storage returns the full vector but we return the
                # delta downstream
                tx.rcpt_response = self.tx_cursor.tx.rcpt_response[
                    -len(tx.rcpt_to):]
            else:
                done = False

            if done and wait_data_resp:
                if self.tx_cursor.tx.data_response:
                    tx.data_response = self.tx_cursor.tx.data_response
                else:
                    done = False
            if done:
                break
            deadline_left = None
            if timeout is not None:
                timeout = (time.monotonic() - start)
            self.tx_cursor.wait(deadline_left)

    def abort(self):
        pass
