from typing import Optional
from threading import Lock, Condition
import logging

from response import Response, Esmtp
from filter import Filter, TransactionMetadata

class FakeEndpoint(Filter):
    start_response = None
    final_response = None
    aborted = False
    tx : Optional[TransactionMetadata] = None

    def __init__(self):
        self.blobs = []
        self.last = False

    def on_update(self, tx : TransactionMetadata):
        logging.info('FakeEndpoint.start %s %s', mail_from, rcpt_to)
        self.tx = tx
        self.tx.rcpt_response = self.start_response

    def append_data(self, last, blob):
        logging.info('FakeEndpoint.append_data %d %s', blob.len(), last)
        self.blobs.append(blob)
        if last:
            self.last = True
            return self.final_response
        return Response()

    def abort(self):
        self.aborted = True

class SyncEndpoint(Filter):
    start_response = None
    aborted = False

    lock : Lock
    cv : Condition
    tx : Optional[TransactionMetadata] = None

    def __init__(self):
        self.blobs = []
        self.last = False

        self.data_resp = []

        self.lock = Lock()
        self.cv = Condition(self.lock)

    def set_start_response(self, resp):
        with self.lock:
            self.start_response = resp
            self.cv.notify_all()

    def add_data_response(self, resp):
        with self.lock:
            self.data_resp.append(resp)
            self.cv.notify_all()

    def on_update(self, tx : TransactionMetadata):
        logging.info('SyncEndpoint.start %s %s', tx.mail_from, tx.rcpt_to)
        self.tx = tx
        with self.lock:
            self.cv.wait_for(lambda: self.start_response is not None)
            self.tx.rcpt_response = self.start_response
            self.start_response = None

    def append_data(self, last, blob):
        logging.info('SyncEndpoint.append_data %d %s', blob.len(), last)
        assert(not self.last)
        self.last = last
        self.blobs.append(blob)
        with self.lock:
            self.cv.wait_for(lambda: bool(self.data_resp))
            return self.data_resp.pop(0)

    def abort(self):
        self.aborted = True
