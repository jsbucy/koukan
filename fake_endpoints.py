from typing import List, Optional
from threading import Lock, Condition
import logging
import time

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

    def on_update(self, tx : TransactionMetadata,
                  timeout : Optional[float] = None):
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
    mail_from : Optional[Mailbox] = None
    mail_response : Optional[Response] = None
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

    def set_mail_response(self, resp : Response):
        with self.lock:
            self.mail_response = resp
            self.cv.notify_all()

    def add_rcpt_response(self, resp : Response):
        with self.lock:
            self.rcpt_response.append(resp)
            self.cv.notify_all()

    def add_data_response(self, resp):
        with self.lock:
            self.data_resp.append(resp)
            self.cv.notify_all()

    def on_update(self, tx : TransactionMetadata,
                  timeout : Optional[float] = None):
        logging.info('SyncEndpoint.on_update %s %s', tx.mail_from, tx.rcpt_to)
        self.tx = tx
        start = time.monotonic()
        deadline_left = None
        if timeout:
            deadline_left = timeout - (time.monotonic() - start)
        if tx.mail_from:
            assert not self.mail_from
            self.mail_from = tx.mail_from
            with self.lock:
                self.cv.wait_for(lambda: self.mail_response is not None,
                                 deadline_left)
                tx.mail_response = self.mail_response

        if timeout:
            deadline_left = timeout - (time.monotonic() - start)
        if tx.rcpt_to:
            self.rcpt_to.extend(tx.rcpt_to)
            with self.lock:
                self.cv.wait_for(
                    lambda: len(self.rcpt_response) == len(self.rcpt_to),
                    deadline_left)
                self.tx.rcpt_response = self.rcpt_response


    def append_data(self, last, blob,
                    timeout : Optional[float] = None):
        logging.info('SyncEndpoint.append_data %d %s', blob.len(), last)
        assert(not self.last)
        self.last = last
        self.blobs.append(blob)
        with self.lock:
            if not self.cv.wait_for(lambda: bool(self.data_resp), timeout):
                return None
            return self.data_resp.pop(0)

    def abort(self):
        self.aborted = True

    def get_data(self):
        out = b''
        for blob in self.blobs:
            out += blob.contents()
        return out
