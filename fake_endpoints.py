from typing import Callable, List, Optional
from threading import Lock, Condition
import logging
import time

from blob import Blob
from response import Response, Esmtp
from filter import (
    AsyncFilter,
    Filter,
    Mailbox,
    SyncFilter,
    TransactionMetadata )

class SyncEndpoint(Filter):
    mail_from : Optional[Mailbox] = None
    mail_response : Optional[Response] = None
    rcpt_to : List[Mailbox]
    rcpt_response : List[Response]
    data_resp : List[Response]
    aborted = False
    ok_rcpt = False

    lock : Lock
    cv : Condition
    tx : Optional[TransactionMetadata] = None
    body_blob : Optional[Blob] = None

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
            if resp.ok():
                self.ok_rcpt = True
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
        if tx.mail_from:
#            assert self.mail_from is None
            self.mail_from = tx.mail_from
        if tx.rcpt_to:
            self.rcpt_to.extend(tx.rcpt_to)
            with self.lock:
                self.cv.wait_for(
                    lambda: len(self.rcpt_response) == len(tx.rcpt_to),
                    deadline_left)
                self.tx.rcpt_response = self.rcpt_response
                self.rcpt_response = []
        if tx.body_blob and self.ok_rcpt:
            assert self.body_blob is None or self.body_blob == tx.body_blob
            self.body_blob = tx.body_blob
            with self.lock:
                if not self.cv.wait_for(lambda: bool(self.data_resp), timeout):
                    return None
                tx.data_response = self.data_resp.pop(0)

    def abort(self):
        self.aborted = True

    def get_data(self):
        out = b''
        for blob in self.blobs:
            out += blob.read(0)
        return out

class FakeAsyncEndpoint(AsyncFilter):
    tx : TransactionMetadata
    mu : Lock
    cv : Condition
    _version : int = 0
    rest_id : str

    def __init__(self, rest_id):
        self.tx = TransactionMetadata()
        self.mu = Lock()
        self.cv = Condition(self.mu)
        self.rest_id = rest_id

    def merge(self, tx):
        with self.mu:
            assert self.tx.merge_from(tx) is not None
            self._version += 1
            self.cv.notify_all()

    # AsyncFilter
    def update(self,
               tx : TransactionMetadata,
               delta : TransactionMetadata,
               timeout : Optional[float] = None
               ) -> Optional[TransactionMetadata]:
        with self.mu:
            assert self.tx.merge_from(delta)
            self._version += 1
            self.cv.notify_all()
            self.cv.wait_for(lambda: not self.tx.req_inflight(), timeout)
            upstream_delta = tx.delta(self.tx)
            assert upstream_delta is not None
            tx.replace_from(self.tx)
            return upstream_delta

    # AsyncFilter
    def get(self, timeout : Optional[float] = None
            ) -> Optional[TransactionMetadata]:
        with self.mu:
            self.cv.wait_for(lambda: not self.tx.req_inflight(), timeout)
            return self.tx.copy()

    def version(self):
        return self._version


Expectation = Callable[[TransactionMetadata,TransactionMetadata],
                       Optional[TransactionMetadata]]
class FakeSyncFilter(SyncFilter):
    expectation : List[Expectation]

    def __init__(self):
        self.expectation = []

    def add_expectation(self, exp : Expectation):
        self.expectation.append(exp)

    # SyncFilter
    def on_update(self,
                  tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:
        exp = self.expectation[0]
        self.expectation.pop(0)
        upstream_delta = exp(tx, tx_delta)
        assert upstream_delta is not None
        return upstream_delta
