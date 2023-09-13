
from typing import Any, List, Optional
from threading import Condition, Lock
from storage import Action

from tags import Tag

from response import Response

from blob import Blob

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
    blobs : List[Blob]
    def __init__(self, storage, blob_id_map : BlobIdMap,
                 blob_storage, next, host, msa):
        self.blobs = []
        self.storage = storage
        self.blob_id_map = blob_id_map
        self.blob_storage = blob_storage
        self.next = next
        self.host = host
        self.next_start_resp = None
        self.next_final_resp = None
        self.msa = msa


        self.lock = Lock()
        self.cv = Condition(self.lock)
        self.have_last_blob = False
        self.last_inflight = False
        self.durable = False
        self.storage_id = None

        self.appended_action = False

    def start(self,
              local_host, remote_host,
              mail_from, transaction_esmtp,
              rcpt_to, rcpt_esmtp) -> Response:
        logging.info('RouterTransaction.start %s %s', mail_from, rcpt_to)
        self.local_host = local_host
        self.remote_host = remote_host
        self.mail_from = mail_from
        self.transaction_esmtp = transaction_esmtp
        self.rcpt_to = rcpt_to
        self.rcpt_esmtp = rcpt_esmtp
        self.next_start_resp = self.next.start(
            self.local_host, self.remote_host,
            self.mail_from, self.transaction_esmtp,
            self.rcpt_to, self.rcpt_esmtp)
        logging.info('RouterTransaction.start_body %s', self.next_start_resp)
        return self.next_start_resp

    def append_data(self,
                    last : bool,
                    blob : Blob) -> Response:
        logging.info('RouterTransaction.append_data %s %d', last, blob.len())
        # msa may have timed out start transaction and will eventually
        # detach this
        if self.next_start_resp and self.next_start_resp.perm():
            return Response(500, "upstream start transaction perm")
        if not self.msa and (self.next_start_resp is None or
                             self.next_start_resp.temp()):
            return Response(400, "upstream start transaction failed temp mx")

        self.blobs.append(blob)
        if last:
            with self.lock:
                logging.info('have last blob %s', self.rcpt_to)
                self.have_last_blob = True
                self.last_inflight = True
                self.cv.notify()

        if self.next_start_resp is None or self.next_start_resp.err():
            # XXX or self.final_resp is not None??
            # this is only for msa which is going to set_durable/detach
            # mx was a precondition failure (above)
            return Response()


        resp = None
        try:
            resp = self.next.append_data(last, blob)
        except:
            resp = Response.Internal('RouterTransaction.append_data exception')

        logging.info('RouterTransaction.append_data_body done %s %s',
                     last, resp)
        if last:
            self.next_final_resp = resp
            with self.lock:
                logging.info('durable %s', self.durable)
                if self.durable:
                    # XXX this hangs forever if the write failed
                    self.cv.wait_for(lambda: self.storage_id is not None)
                    logging.info('storage_id %s', self.storage_id)
                    self.append_action()

        return resp

    def append_action(self):
        # XXX hack
        if self.appended_action: return

        self.appended_action = True
        # XXX dedupe with router_service
        action = Action.TEMP_FAIL
        resp = self.next_final_resp
        if resp is not None:
            if resp.ok():
                action = Action.DELIVERED
            elif resp.perm():
                # permfail/bounce
                action = Action.PERM_FAIL
        self.storage.append_transaction_actions(self.storage_id, action)

    def abort(self):
        pass

    def set_durable(self):
        logging.info('RouterTransaction.set_durable')
        assert(not self.durable)
        with self.lock:
            self.durable = True
            self.cv.notify()
        with self.lock:
            logging.info('RouterTransaction.set_durable %s have last blob %s',
                         self.rcpt_to, self.have_last_blob)
            self.cv.wait_for(lambda: self.have_last_blob)

        transaction_writer = self.storage.get_transaction_writer()
        if not transaction_writer.start(
            self.local_host, self.remote_host,
            self.mail_from, self.transaction_esmtp,
            self.rcpt_to, self.rcpt_esmtp, self.host, self.last_inflight):
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
            self.cv.notify()

        # append_data last may have already finished before set_durable() was
        # called, write the action here
        if self.next_final_resp is not None:
            self.append_action()

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
