from typing import List, Optional
from threading import Lock, Condition
import logging

from response import Response, Esmtp
from filter import Filter, Mailbox, TransactionMetadata

class FakeEndpoint(Filter):
    mail_response : Optional[Response] = None
    rcpt_response : Optional[Response] = None
    data_response : Optional[Response] = None
    aborted = False
    tx : Optional[TransactionMetadata] = None

    def __init__(self):
        self.blobs = []
        self.last = False

    def on_update(self, tx : TransactionMetadata):
        logging.info('FakeEndpoint.start %s %s', tx.mail_from, tx.rcpt_to)
        self.tx = tx
        for x in ['mail_response', 'rcpt_response', 'data_response']:
            if getattr(self, x):
                setattr(self.tx, x, getattr(self, x))

    def append_data(self, last, blob):
        logging.info('FakeEndpoint.append_data %d %s', blob.len(), last)
        self.blobs.append(blob)
        if last:
            self.last = True
            return self.data_response
        return Response()

    def abort(self):
        self.aborted = True

class SyncEndpoint(Filter):
    rcpt_to : List[Mailbox]
    rcpt_response : List[Response]
    data_resp : List[Response]
    aborted = False

    lock : Lock
    cv : Condition
    tx : Optional[TransactionMetadata] = None

    def __init__(self):
        self.rcpt_to = []
        self.rcpt_response = []
        self.blobs = []
        self.last = False

        self.data_resp = []

        self.lock = Lock()
        self.cv = Condition(self.lock)

    def add_rcpt_response(self, resp : Response):
        with self.lock:
            self.rcpt_response.append(resp)
            self.cv.notify_all()

    def add_data_response(self, resp):
        with self.lock:
            self.data_resp.append(resp)
            self.cv.notify_all()

    def on_update(self, tx : TransactionMetadata):
        logging.info('SyncEndpoint.on_update %s %s', tx.mail_from, tx.rcpt_to)
        self.tx = tx
        if tx.mail_from:
            tx.mail_response = Response()
        if tx.rcpt_to:
            self.rcpt_to.extend(tx.rcpt_to)
            with self.lock:
                self.cv.wait_for(lambda: bool(self.rcpt_response))
                self.tx.rcpt_response = self.rcpt_response
                self.rcpt_response = []

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
