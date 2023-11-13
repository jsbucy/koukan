
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
    blobs_received = -1
    blobs : List[Blob]
    blob_upstream_queue : List[Blob]

    # something is running on executor and we can't cancel
    busy = False

    mx_multi_rcpt = False
    rest_id = None
    storage_tx : Optional[TransactionCursor] = None
    done_cb = None
    finalized = False

    # if this transaction requires further downstream input to make
    # progress, when the last update was, for ttl
    last_update = None

    def __init__(self, executor, storage, blob_id_map : BlobIdMap,
                 blob_storage, next, host, msa, tag, storage_tx=None):
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
        self.received_last = False
        self.storage_id = None

        self.appended_action = False

        self.recovered = False

        self.local_host = None
        self.remote_host = None
        self.mail_from = None
        self.transaction_esmtp = None
        self.rcpt_to = None
        self.rcpt_esmtp = None

        if storage_tx is not None:
            self.storage_tx = storage_tx
            self.rest_id = storage_tx.rest_id
            self.storage_id = storage_tx.id

    def __del__(self):
        logging.debug('RouterTransaction.__del__ %s', self.rest_id)
        assert(self.finalized)

    # done_cb runs when
    # 1: downstream calls set_durable
    # or
    # 2: downstream appended last and upstream succeeds or permfails such that
    # set_durable would be a no-op
    # does not run on abort()
    def generate_rest_id(self, done_cb=None):
        assert(self.storage_tx is None)
        self.rest_id = secrets.token_urlsafe(REST_ID_BYTES)
        self.storage_tx = self.storage.get_transaction_cursor()
        self.storage_tx.create(self.rest_id)
        self.storage_id = self.storage_tx.id
        self.done_cb = done_cb
        return self.rest_id

    def _start(self,
              local_host, remote_host,
              mail_from, transaction_esmtp,
              rcpt_to, rcpt_esmtp):
        logging.info('RouterTransaction._start %s %s', mail_from, rcpt_to)
        self.last_update = time.monotonic()
        self.local_host = local_host
        self.remote_host = remote_host
        self.mail_from = mail_from
        self.transaction_esmtp = transaction_esmtp
        self.rcpt_to = rcpt_to
        self.rcpt_esmtp = rcpt_esmtp
        with self.lock:
            assert(not self.busy)
            self.busy = True

    def start(self,
              local_host, remote_host,
              mail_from, transaction_esmtp,
              rcpt_to, rcpt_esmtp) -> Optional[Response]:
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
        return None

    def load(self):
        assert(self.storage_tx)
        storage_tx = self.storage_tx
        logging.info("RouterTransaction.load id=%s from=%s to=%s length=%d",
                     storage_tx.id, storage_tx.mail_from, storage_tx.rcpt_to,
                     storage_tx.length)
        self.recovered = True
        self.msa = False
        self._start(
            storage_tx.local_host, storage_tx.remote_host,
            storage_tx.mail_from, storage_tx.transaction_esmtp,
            storage_tx.rcpt_to, storage_tx.rcpt_esmtp)
        resp = self.start_upstream()
        logging.info("RouterTransaction.load start resp %s %s",
                     storage_tx.id, resp)
        if resp is None or resp.err():
            return

        offset = 0
        while offset < storage_tx.length:
            blob = storage_tx.read_content(offset)
            assert(blob is not None)
            offset += blob.len()
            last = offset == storage_tx.length
            resp = self.append_blob_upstream(last=last, blob=blob)
            if resp.err(): break


    def start_upstream(self):
        with self.lock:
            assert(self.busy)
            if self.next_final_resp is not None:  # i.e. cancelled
                return

        resp = self.next.start(
            self.local_host, self.remote_host,
            self.mail_from, self.transaction_esmtp,
            self.rcpt_to, self.rcpt_esmtp)
        logging.info('RouterTransaction.start_upstream %s', resp)

        # any upstream start transaction error ends the upstream
        # transaction but the downstream transaction may be able to continue
        with self.lock:
            if resp.err():
                self.maybe_append_action_locked(resp)

            self.next_start_resp = resp
            if resp.err():
                self.next_final_resp = resp

            assert(self.busy)
            self.busy = False
            self.cv.notify_all()

            if not resp.err():
                # rest msa could have buffered an upstream append
                # concurrent with upstream start so send that upstream now
                self.maybe_start_append_upstream_locked()

        return resp

    def set_mx_multi_rcpt(self):
        self.mx_multi_rcpt = True

    def get_start_result(self, timeout=1) -> Optional[Response]:
        with self.lock:
            if self.busy:
                self.cv.wait_for(lambda: self.next_start_resp is not None,
                                 timeout=timeout)
            return self.next_start_resp

    def get_final_status(self, timeout=1):
        with self.lock:
            if self.received_last and self.busy:
                self.cv.wait_for(lambda: self.next_final_resp is not None,
                                 timeout=timeout)
            return self.next_final_resp

    # The rest api enforces the preconditions; a precondition failure
    # here is a bug so it asserts. Returns an error response if not ok
    # to append, None otherwise.
    def append_ok(self) -> Optional[Response]:
        if self.msa:
            # msa can continue as long as upstream hasn't permfailed
            # (start transaction errors propagate to next_final_resp)
            if self.next_final_resp is not None and self.next_final_resp.perm():
                return self.next_final_resp
        else:
            # precondition: mx can only append if upstream start has succeeded
            assert(self.next_start_resp is not None)
            assert(not self.next_start_resp.err())

            # Single-rcpt mx is sync on all upstream errors. A
            # transaction fanned out of a multi-rcpt smtp transaction
            # may have mixed results and need accept&bounce
            # semantics/set_durable. In that case, we continue here to
            # buffer the payload and bail in append_upstream().
            if not self.mx_multi_rcpt and self.next_final_resp is not None:
                return self.next_final_resp

        return None

    def append_data(self,
                    last : bool,
                    blob : Blob) -> Optional[Response]:
        logging.info('RouterTransaction.append_data %s %d', last, blob.len())
        self.last_update = time.monotonic()

        # Note the rest code doesn't currently propagate start errors
        # to final like this does.
        resp = self.append_ok()
        if resp is not None:
            return resp

        with self.lock:
            self.blobs_received += 1
            self.blobs.append(blob)
            if last: self.received_last = True
            self.blob_upstream_queue.append(blob)
            self.maybe_start_append_upstream_locked()

        return None  # async

    def maybe_start_append_upstream_locked(self):
        if (self.next_final_resp is None and  # i.e. cancelled
            not self.busy and
            self.blob_upstream_queue):
            self.busy = True
            self.executor.enqueue(Tag.DATA, lambda: self.append_upstream())

    def append_upstream(self):
        while True:
            last = False
            with self.lock:
                logging.info('RouterTransaction.append_upstream %d %s',
                             len(self.blob_upstream_queue), self.received_last)
                if self.next_final_resp is not None:  # i.e. cancelled
                    assert(self.busy)
                    self.busy = False
                    return

                # Bail here if the upstream transaction has already
                # failed. This is a precondition of the upstream
                # append and is synchronous here.
                # note: some downstream transactions (msa, multi-mx)
                # can continue after start temp but not upstream
                if self.append_ok() is not None or not self.blob_upstream_queue:
                    assert(self.busy)
                    self.busy = False
                    return
                blob = self.blob_upstream_queue.pop(0)
                last = (self.received_last and not self.blob_upstream_queue)
            self.append_blob_upstream(last, blob)

    def append_blob_upstream(self,
                             last : bool,
                             blob : Blob) -> Response:
        logging.info('RouterTransaction.append_blob_upstream %s %d',
                     last, blob.len())
        resp = self.next.append_data(last, blob)
        logging.info('RouterTransaction.append_blob_upstream done %s %s',
                     last, resp)

        # Don't propagate success append results prior to the last since they
        # don't denote the final status of the transaction.
        # XXX possibly internal endpoints should return None for
        # non-last appends?
        with self.lock:
            if self.next_final_resp is None and (last or resp.err()):
                # make the status durable before possibly surfacing to
                # clients/rest
                self.maybe_append_action_locked(resp)
                self.next_final_resp = resp
                self.cv.notify_all()

        return resp

    # append(last) finishing upstream or set_durable() can happen in
    # either order
    def maybe_append_action_locked(self, next_final_resp):
        logging.info('RouterTransaction.maybe_append_action %s %s appended=%s',
                     self.rest_id, next_final_resp, self.appended_action)
        self.last_update = time.monotonic()

        if self.appended_action: return
        assert(self.storage_id)
        if next_final_resp is None: return
        self.appended_action = True

        action = Action.TEMP_FAIL
        done_with_payload = False
        if next_final_resp.ok():
            action = Action.DELIVERED
            done_with_payload = True
        elif next_final_resp.perm():
            # permfail/bounce
            action = Action.PERM_FAIL
            done_with_payload = True

        # i.e. this will not retry so we don't need to persist the payload
        if done_with_payload:
            for blob in self.blobs:
                blob.unref(self)
            self.blobs = None

            if self.done_cb:
                # cb probably calls finalize()
                cb = self.done_cb
                self.done_cb = None
                cb()

        # TODO MAX_RETRY

        # TODO when upgrading temp to perm (e.g. after max retry), may
        # want to distinguish the internal perm result from the
        # upstream temp result
        self.storage_tx.append_action(action, next_final_resp)


    # returns True if aborted
    def abort_if_idle(self, idle_timeout, now):
        if self.recovered: return False
        with self.lock:
            if self.busy:
                return False
            if (now - self.last_update) < idle_timeout:
                return False

        self._abort()
        return True


    # noop if already delivered
    # writes an abort action
    def abort(self):
        with self.lock:
            assert(not self.busy or self.next_final_resp is None)
            self.cv.wait_for(lambda: not self.busy)
        self._abort()

    def _abort(self):
        with self.lock:
            assert(not self.busy)
            if self.next_final_resp is None:
                self.next_final_resp = Response(400, 'cancelled')
                self.cv.notify_all()

        logging.info('RouterTransaction.abort %s', self.rest_id)

        if not self.appended_action:
            self.storage_tx.append_action(Action.ABORT, self.next_final_resp)
            self.appended_action = True

        self.next.abort()

        if self.done_cb:
            self.done_cb = None

    def set_durable(self):
        logging.info('RouterTransaction.set_durable %s', self.rest_id)
        # the downstream transaction must have sent the last append
        # but the upstream may still be inflight
        assert(self.received_last)
        assert(self.storage_id)

        # xxx yuck refactor with append_action/storage
        status = Status.WAITING
        if self.next_final_resp:
            if self.next_final_resp.ok() or self.next_final_resp.perm():
                status = Status.DONE
            # else temp, leave it WAITING
        elif self.received_last and self.busy:
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

            self.storage_tx.finalize_payload(status)

        # upstream append_data last may have already finished before
        # set_durable() was called, write the action here upstream
        # append may still be inflight, in that case next_final_resp
        # is None, this early returns
        with self.lock:
            self.maybe_append_action_locked(self.next_final_resp)

        return Response()

    def finalize(self):
        logging.info('RouterTransaction.finalize %s', self.rest_id)
        assert(self.done_cb is None)
        assert(not self.finalized)
        assert(self.appended_action)

        if self.blobs:
            for blob in self.blobs:
                blob.unref(self)
            blobs = None

        self.finalized = True


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
