from response import Response, Esmtp

from threading import Lock, Condition

import logging

class FakeEndpoint:
    start_response = None
    final_response = None
    aborted = False

    def __init__(self):
        self.local_host = None
        self.remote_host = None
        self.mail_from = None
        self.transaction_esmtp = None
        self.rcpt_to = None
        self.rcpt_esmtp = None

        self.blobs = []
        self.last = False

    def start(self,
              local_host, remote_host,
              mail_from, transaction_esmtp,
              rcpt_to, rcpt_esmtp):
        logging.info('FakeEndpoint.start %s %s', mail_from, rcpt_to)
        self.local_host = local_host
        self.remote_host = remote_host
        self.mail_from = mail_from
        self.transaction_esmtp = transaction_esmtp
        self.rcpt_to = rcpt_to
        self.rcpt_esmtp = rcpt_esmtp
        return self.start_response

    def append_data(self, last, blob):
        logging.info('FakeEndpoint.append_data %d %s', blob.len(), last)
        self.blobs.append(blob)
        if last:
            self.last = True
            return self.final_response
        return Response()

    def abort(self):
        self.aborted = True

class SyncEndpoint:
    start_response = None
    aborted = False

    lock : Lock
    cv : Condition

    def __init__(self):
        self.local_host = None
        self.remote_host = None
        self.mail_from = None
        self.transaction_esmtp = None
        self.rcpt_to = None
        self.rcpt_esmtp = None

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

    def start(self,
              local_host, remote_host,
              mail_from, transaction_esmtp,
              rcpt_to, rcpt_esmtp):
        logging.info('SyncEndpoint.start %s %s', mail_from, rcpt_to)
        self.local_host = local_host
        self.remote_host = remote_host
        self.mail_from = mail_from
        self.transaction_esmtp = transaction_esmtp
        self.rcpt_to = rcpt_to
        self.rcpt_esmtp = rcpt_esmtp
        with self.lock:
            self.cv.wait_for(lambda: self.start_response is not None)
            resp = self.start_response
            self.start_response = None
            return resp

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
