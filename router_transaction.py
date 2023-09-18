
from typing import Any, List, Optional
from threading import Condition, Lock
from storage import Action, Status

from tags import Tag

from response import Response

from blob import Blob
from executor import Executor

import logging

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


class RouterTransaction:
    executor : Executor
    blobs : List[Blob]
    blob_upstream_queue : List[Blob]
    upstream_start_inflight = False
    upstream_append_inflight = False
    mx_multi_rcpt = False

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

        self.local_host = None
        self.remote_host = None
        self.mail_from = None
        self.transaction_esmtp = None
        self.rcpt_to = None
        self.rcpt_esmtp = None

    def start(self,
              local_host, remote_host,
              mail_from, transaction_esmtp,
              rcpt_to, rcpt_esmtp):
        logging.info('RouterTransaction.start %s %s', mail_from, rcpt_to)
        self.local_host = local_host
        self.remote_host = remote_host
        self.mail_from = mail_from
        self.transaction_esmtp = transaction_esmtp
        self.rcpt_to = rcpt_to
        self.rcpt_esmtp = rcpt_esmtp
        self.upstream_start_inflight = True
        self.executor.enqueue(self.tag, lambda: self.start_upstream())

    def start_upstream(self):
        resp = self.next.start(
            self.local_host, self.remote_host,
            self.mail_from, self.transaction_esmtp,
            self.rcpt_to, self.rcpt_esmtp)
        logging.info('RouterTransaction.start_body %s', self.next_start_resp)

        start_resp = None
        if resp is not None and resp.ok():
           start_resp = resp
        if self.msa:
            if resp.perm():
                start_resp = resp
        else:  # mx
            if resp.err():
                start_resp = resp
        with self.lock:
            if start_resp: self.next_start_resp = start_resp
            self.upstream_start_inflight = False
            self.cv.notify_all()

    def set_mx_multi_rcpt(self):
        self.mx_multi_rcpt = True

    def get_start_result(self, timeout=1) -> Optional[Response]:
        with self.lock:
            if self.upstream_start_inflight:
                self.cv.wait_for(lambda: self.next_start_resp is not None)
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
               self.executor.enqueue(Tag.DATA, lambda: self.append_upstream())
        return None  # async

    def append_upstream(self):
        while True:
            last = False
            with self.lock:
                logging.info('RouterTransaction.append_upstream %d %s', len(self.blob_upstream_queue), self.have_last_blob)
                if not self.blob_upstream_queue:
                    inflight = False
                    return
                blob = self.blob_upstream_queue.pop(0)
                last = (len(self.blob_upstream_queue) == 0) and self.have_last_blob
            self.append_blob_upstream(last, blob)


    def append_blob_upstream(self,
                             last : bool,
                             blob : Blob) -> Response:
        logging.info('RouterTransaction.append_blob_upstream %s %d', last, blob.len())

        if last:
                logging.info('have last blob %s', self.rcpt_to)

        resp = None
        try:
            resp = self.next.append_data(last, blob)
        except:
            resp = Response.Internal('RouterTransaction.append_data exception')

        logging.info('RouterTransaction.append_blob_upstream done %s %s',
                     last, resp)

        final_resp = None
        if last and resp.ok():
            final_resp = resp
        if self.msa:
            if resp.perm():
                final_resp = resp
        elif not self.mx_multi_rcpt:
            if resp.err():
                final_resp = resp
        else:  # mx single rcpt
            # for multi-rcpt mx, keep going on errs since we
            # don't know whether it's ultimately going to
            # set_durable and need the the payload to generate
            # the bounce
            if last and resp.err():
                final_resp = resp

        if final_resp:
            with self.lock:
                self.next_final_resp = final_resp
                self.cv.notify_all()

        if last:
            self.maybe_append_action()

        return resp

    # append(last) finishing upstream or set_durable() can happen in
    # either order
    def maybe_append_action(self):
        logging.info('RouterTransaction.maybe_append_action')
        if self.appended_action: return
        if self.storage_id is None: return
        if self.next_final_resp is None: return

        # XXX dedupe with router_service
        action = Action.TEMP_FAIL
        resp = self.next_final_resp
        if resp.ok():
            action = Action.DELIVERED
        elif resp.perm():
            # permfail/bounce
            action = Action.PERM_FAIL
        self.storage.append_transaction_actions(self.storage_id, action)
        self.appended_action = True

    def abort(self):
        pass

    def set_durable(self):
        # XXX should this noop if the upstream transaction already succeeded?
        logging.info('RouterTransaction.set_durable')
        assert(self.have_last_blob)

        transaction_writer = self.storage.get_transaction_writer()

        # XXX this could be any of
        status = Status.WAITING
        if self.next_final_resp:
            status = Status.DONE
        with self.lock:
            if self.have_last_blob and self.upstream_append_inflight: status = Status.INFLIGHT

        if not transaction_writer.start(
            self.local_host, self.remote_host,
            self.mail_from, self.transaction_esmtp,
            self.rcpt_to, self.rcpt_esmtp, self.host, status):
            return None

        # XXX errs?
        for blob in self.blobs:
            if blob.id():
                self.append_blob(transaction_writer, blob)
            else:
                transaction_writer.append_data(blob.contents())

        # XXX err?
        transaction_writer.finalize()

        logging.info('RouterTransaction.set_durable %s', transaction_writer.id)
        with self.lock:
            self.storage_id = transaction_writer.id

        # append_data last may have already finished before set_durable() was
        # called, write the action here
        self.maybe_append_action()

        return transaction_writer.id

    def append_blob(self, transaction_writer, blob):
        assert(blob.id() is not None)
        db_blob = self.blob_id_map.lookup_or_insert(blob.id())
        logging.info('RouterTransaction.append_blob %s %s', blob.id(), db_blob)
        if db_blob is None:
            blob_writer = self.storage.get_blob_writer()
            db_blob = blob_writer.start()
            blob_writer.append_data(blob.contents())
            blob_writer.finalize()
            self.blob_id_map.finalize(blob.id(), db_blob)
        assert(transaction_writer.append_blob(db_blob) ==
               transaction_writer.APPEND_BLOB_OK)
