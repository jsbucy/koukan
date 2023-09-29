
from typing import Any, List, Optional
from threading import Condition, Lock
from storage import Action, Status, TransactionCursor

from tags import Tag

from response import Response

from blob import Blob, InlineBlob
from executor import Executor

import time

import logging

import secrets

REST_ID_BYTES = 4  # XXX configurable, use more in prod

# TODO dedupe w/RestEndpoint impl of this?
class BlobIdMap:
    def __init__(self):
        self.id_map = {}
        self.lock = Lock()
        self.cv = Condition(self.lock)

    # TODO: gc

    def lookup_or_insert(self, id):
        with self.lock:
            if id not in self.id_map:
                self.id_map[id] = None
                return None
            self.cv.wait_for(lambda: self.id_map[id] is not None)
            return self.id_map[id]

    def finalize(self, id, id2):
        with self.lock:
            assert(id in self.id_map and self.id_map[id] is None)
            self.id_map[id] = id2
            self.cv.notify_all()

MAX_RETRY = 3 * 86400

class RouterTransaction:
    executor : Executor
    blobs : List[Blob]
    blob_upstream_queue : List[Blob]
    upstream_start_inflight = False
    upstream_append_inflight = False
    mx_multi_rcpt = False
    rest_id = None
    storage_tx : Optional[TransactionCursor] = None

    def __init__(self, executor, storage, blob_id_map : BlobIdMap,
                 blob_storage, next, host, msa, tag):
        self.executor = executor
        self.blobs = []
        self.blob_upstream_queue = []
        self.storage = storage
        self.blob_id_map = blob_id_map
        self.blob_storage = blob_storage
        self.next = next
        self.host = host
        self.next_start_resp = None
        self.next_final_resp = None
        self.msa = msa
        self.tag = tag

        self.lock = Lock()
        self.cv = Condition(self.lock)
        self.have_last_blob = False
        self.storage_id = None

        self.appended_action = False

        self.recovered = False

        self.local_host = None
        self.remote_host = None
        self.mail_from = None
        self.transaction_esmtp = None
        self.rcpt_to = None
        self.rcpt_esmtp = None

    def generate_rest_id(self):
        self.rest_id = secrets.token_urlsafe(REST_ID_BYTES)
        self.storage_tx = self.storage.get_transaction_cursor()
        self.storage_tx.create(self.rest_id)
        self.storage_id = self.storage_tx.id
        return self.rest_id

    def _start(self,
              local_host, remote_host,
              mail_from, transaction_esmtp,
              rcpt_to, rcpt_esmtp):
        logging.info('RouterTransaction._start %s %s', mail_from, rcpt_to)
        self.local_host = local_host
        self.remote_host = remote_host
        self.mail_from = mail_from
        self.transaction_esmtp = transaction_esmtp
        self.rcpt_to = rcpt_to
        self.rcpt_esmtp = rcpt_esmtp
        self.upstream_start_inflight = True

    def start(self,
              local_host, remote_host,
              mail_from, transaction_esmtp,
              rcpt_to, rcpt_esmtp):
        assert(self.rest_id)
        assert(rcpt_to)
        if not self.storage_tx.write_envelope(
            local_host, remote_host,
            mail_from, transaction_esmtp,
            rcpt_to, rcpt_esmtp,
            self.host):
            return None

        self._start(local_host, remote_host,
                    mail_from, transaction_esmtp,
                    rcpt_to, rcpt_esmtp)
        self.executor.enqueue(self.tag, lambda: self.start_upstream())

    def load(self, storage_tx : TransactionCursor):
        logging.info("RouterTransaction.load %s %s %s",
                     storage_tx.id, storage_tx.mail_from, storage_tx.rcpt_to)
        self.storage_tx = storage_tx
        self.recovered = True
        self.msa = False
        try:
            self._start(
                storage_tx.local_host, storage_tx.remote_host,
                storage_tx.mail_from, storage_tx.transaction_esmtp,
                storage_tx.rcpt_to, storage_tx.rcpt_esmtp)
            resp = self._start_upstream()
            logging.info("RouterTransaction.load start resp %s %s",
                         storage_tx.id, resp)
            if resp.ok():
                offset = 0
                while offset < storage_tx.length:
                    blob = storage_tx.read_content(offset)
                    assert(blob is not None)
                    offset += blob.len()
                    last = offset == storage_tx.length
                    resp = self._append_blob_upstream(last=last, blob=blob)
                    if resp.err(): break
        except:
            resp = Response.Internal('RouterTransaction.load exception')

        action = None
        if resp.ok():
            action = Action.DELIVERED
        elif resp.perm() or (time.time() - storage_tx.creation) > MAX_RETRY:
            # permfail/bounce
            action = Action.PERM_FAIL
        else:
            action = Action.TEMP_FAIL

        self.storage_tx.append_action(action)


    def _start_upstream(self):
        resp = self.next.start(
            self.local_host, self.remote_host,
            self.mail_from, self.transaction_esmtp,
            self.rcpt_to, self.rcpt_esmtp)
        logging.info('RouterTransaction._start_upstream %s', resp)
        return resp

    def start_upstream(self):
        start_resp = self._start_upstream()

        # TODO cf append_blob_upstream()
        # start_resp = None
        # if resp is not None and resp.ok():
        #    start_resp = resp
        # if self.msa:
        #     if resp.perm():
        #         start_resp = resp
        # else:  # mx
        #     if resp.err():
        #         start_resp = resp
        with self.lock:
            if start_resp: self.next_start_resp = start_resp
            self.upstream_start_inflight = False
            self.cv.notify_all()

    def set_mx_multi_rcpt(self):
        self.mx_multi_rcpt = True

    def get_start_result(self, timeout=1) -> Optional[Response]:
        with self.lock:
            if self.upstream_start_inflight:
                self.cv.wait_for(lambda: self.next_start_resp is not None,
                                 timeout=timeout)
            return self.next_start_resp

    def get_final_status(self, timeout=1):
        with self.lock:
            if self.have_last_blob and self.upstream_append_inflight:
                self.cv.wait_for(lambda: self.next_final_resp is not None,
                                 timeout=timeout)
            return self.next_final_resp

    def append_data(self,
                    last : bool,
                    blob : Blob) -> Optional[Response]:
        logging.info('RouterTransaction.append_data %s %d', last, blob.len())
        with self.lock:
           if self.next_final_resp:
               return self.next_final_resp
           # XXX the definitive logic for reporting upstream errors is
           # in append_blob_upstream, the commented-out code (below)
           # is redundant?
           if self.next_final_resp:
               return self.next_final_resp
           # XXX we enforce these invariants in rest service, this can
           # just assert?
           # if self.msa:
           #     if self.next_start_resp.perm():
           #         return Response(400, "upstream start transaction perm msa")
           # elif not self.mx_multi_rcpt:  # mx single rcpt
           #     if self.next_start_resp.err():
           #         return Response(
           #             400, "upstream start transaction failed mx")
           # else:  # mx_multi_rcpt
           #     # for multi-rcpt mx, keep going on errs since we
           #     # don't know whether it's ultimately going to
           #     # set_durable and need the the payload to generate
           #     # the bounce
           #     pass

           self.blobs.append(blob)
           self.blob_upstream_queue.append(blob)
           if last: self.have_last_blob = True
           if not self.upstream_append_inflight:
               self.inflight = True
               self.upstream_append_inflight = True  # XXX redundant?
               self.executor.enqueue(Tag.DATA, lambda: self.append_upstream())
        return None  # async

    def append_upstream(self):
        while True:
            last = False
            with self.lock:
                logging.info('RouterTransaction.append_upstream %d %s',
                             len(self.blob_upstream_queue), self.have_last_blob)
                if not self.blob_upstream_queue:
                    self.inflight = False
                    self.upstream_append_inflight = False
                    return
                blob = self.blob_upstream_queue.pop(0)
                last = ((len(self.blob_upstream_queue) == 0) and
                        self.have_last_blob)
            self.append_blob_upstream(last, blob)

    def _append_blob_upstream(self, last, blob):
        logging.info('RouterTransaction._append_blob_upstream %s %d',
                     last, blob.len())
        try:
           resp = self.next.append_data(last, blob)
           assert(resp is not None)
        except:
           resp = Response.Internal(
               'RouterTransaction._append_blob_upstream exception')

        logging.info('RouterTransaction._append_blob_upstream done %s %s',
                     last, resp)
        return resp

    def append_blob_upstream(self,
                             last : bool,
                             blob : Blob) -> Response:
        # we early returned in append_data() if there was already an
        # upstream response
        resp = self._append_blob_upstream(last, blob)

        final_resp = None
        if last and resp.ok():
            # XXX possibly internal endpoints should return None for
            # non-last appends?
            final_resp = resp
        # TODO continuing to refine my understanding of this
        # - RouterTransaction should transparently propagate upstream
        #   errors rather than this convoluted logic
        # - smtp gw should have the logic e.g. msa swallows start
        #   error -> rcpt 200 but mx doesn't
        # - absence of previous errors still a precondion here
        # if self.msa:
        #     if resp.perm():
        #         final_resp = resp
        # elif not self.mx_multi_rcpt:  # mx single rcpt
        #     if resp.err():
        #         final_resp = resp
        # else:  # mx multi rcpt
        #     # for multi-rcpt mx, keep going on errs since we
        #     # don't know whether it's ultimately going to
        #     # set_durable and need the the payload to generate
        #     # the bounce
        #     if last and resp.err():
        #         final_resp = resp

        if last:
            final_resp = resp

        # make the status durable before possibly surfacing to clients/rest
        if last:
            self.maybe_append_action(final_resp)

        if final_resp:
            with self.lock:
                self.next_final_resp = final_resp
                self.cv.notify_all()

        return resp

    # append(last) finishing upstream or set_durable() can happen in
    # either order
    def maybe_append_action(self, next_final_resp):
        logging.info('RouterTransaction.maybe_append_action %s', self.rest_id)
        if self.appended_action: return
        assert(self.storage_id)
        if next_final_resp is None: return

        action = Action.TEMP_FAIL
        if next_final_resp.ok():
            action = Action.DELIVERED
        elif next_final_resp.perm():
            # permfail/bounce
            action = Action.PERM_FAIL
        self.storage_tx.append_action(action)
        self.appended_action = True

    def abort(self):
        pass

    def set_durable(self):
        logging.info('RouterTransaction.set_durable')
        assert(self.have_last_blob)
        assert(self.storage_id)

        status = Status.WAITING
        if self.next_final_resp and (
                self.next_final_resp.ok() or self.next_final_resp.perm()):
            status = Status.DONE
        # xxx msa upstream_start_inflight?
        if self.have_last_blob and self.upstream_append_inflight:
            status = Status.INFLIGHT

        logging.info('RouterTransaction.set_durable status=%d %s',
                     status, self.next_final_resp)

        # don't persist the payload if it already succeeded or permfailed
        if status != Status.DONE:
            for blob in self.blobs:
                # XXX errs?
                if blob.id():
                    self.append_blob(blob)
                else:
                    self.storage_tx.append_data(blob.contents())

            # XXX all this really does that append_transaction_actions()
            # doesn't is set the length
            self.storage_tx.finalize_payload(status)

        # append_data last may have already finished before set_durable() was
        # called, write the action here
        # upstream append may still be inflight, in that case
        # next_final_resp is None, this early returns
        self.maybe_append_action(self.next_final_resp)

        return self.storage_id

    def append_blob(self, blob):
        assert(blob.id() is not None)
        db_blob = self.blob_id_map.lookup_or_insert(blob.id())
        logging.info('RouterTransaction.append_blob %s %s', blob.id(), db_blob)
        if db_blob is None:
            blob_writer = self.storage.get_blob_writer()
            db_blob = blob_writer.start()
            blob_writer.append_data(blob.contents())
            blob_writer.finalize()
            self.blob_id_map.finalize(blob.id(), db_blob)
        assert(self.storage_tx.append_blob(db_blob) ==
               TransactionCursor.APPEND_BLOB_OK)
