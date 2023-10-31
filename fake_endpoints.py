from response import Response, Esmtp

import logging

class FakeEndpoint:
    start_response = None
    final_response = None

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
