from typing import Any, Callable, Optional, Dict, List, Tuple
import sqlite3
import json
import time
import logging
from threading import Lock, Condition
from hashlib import sha256
from base64 import b64encode
from functools import reduce

from sqlalchemy import create_engine
from sqlalchemy.engine import CursorResult, Engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.sql.functions import count, current_time

from sqlalchemy import (
    LargeBinary, MetaData, String, Table,
    and_, cast, case as sa_case, column,
    delete, event, func, insert, join, literal, not_, or_, select,
    true as sa_true, update, union_all, values)

import psutil

from blob import Blob, InlineBlob, WritableBlob
from response import Response
from storage_schema import InvalidActionException, VersionConflictException
from filter import TransactionMetadata, WhichJson

# the implementation of CursorResult.rowcount apparently involves too
# much metaprogramming for pytype to infer correctly
def rowcount(res : CursorResult) -> int:
    count = res.rowcount
    assert isinstance(count, int)
    return count

class TransactionCursor:
    id : Optional[int] = None
    rest_id : Optional[str] = None
    attempt_id : Optional[int] = None

    version : Optional[int] = None

    creation : Optional[int] = None

    input_done : Optional[bool] = None
    final_attempt_reason : Optional[str] = None

    tx : Optional[TransactionMetadata] = None

    message_builder : Optional[dict] = None  # json

    no_final_notification : Optional[bool] = None

    body_blob_id : Optional[int] = None
    body_rest_id : Optional[str] = None

    def __init__(self, storage):
        self.parent = storage
        self.id = None

    def etag(self) -> str:
        base = '%d.%d.%d' % (self.creation, self.id, self.version)
        return base
        # xxx enable in prod
        #return b64encode(
        #    sha256(base.encode('us-ascii')).digest()).decode('us-ascii')

    def create(self,
               rest_id : str,
               tx : TransactionMetadata,
               reuse_blob_rest_id : List[str] = [],
               create_leased : bool = False):
        parent = self.parent
        self.creation = int(time.time())
        with self.parent.begin_transaction() as db_tx:
            # xxx dead?
            self.last = False
            self.version = 0

            db_json = tx.to_json(WhichJson.DB)
            logging.debug('TxCursor.create %s %s', rest_id, db_json)
            ins = insert(self.parent.tx_table).values(
                rest_id = rest_id,
                creation = self.creation,
                last_update = self.creation,
                version = self.version,
                last = self.last,
                json = db_json,
                creation_session_id = self.parent.session_id,
            ).returning(self.parent.tx_table.c.id)

            if create_leased:
                ins = ins.values(
                    inflight_session_id = self.parent.session_id)

            res = db_tx.execute(ins)
            row = res.fetchone()
            self.id = row[0]
            self.tx = TransactionMetadata()  # tx

            self._write(db_tx, tx, reuse_blob_rest_id)

    def _reuse_blob(self, db_tx, blob_rest_ids : List[str]
                    ) -> List[Tuple[int, str, bool]]:
                    # -> id, rest_id, input_done
        if not blob_rest_ids:
            return []
        assert isinstance(blob_rest_ids, list)

        # sqlite handles some forms of VALUES e.g
        # WITH tt(i) AS (VALUES(2),(3)) SELECT t.i FROM t JOIN tt ON t.i = tt.i;
        # but SA renders the following as
        # WITH anon_1 AS
        # (SELECT v.rest_id AS rest_id
        # FROM (VALUES (?)) AS v (rest_id))
        # SELECT blob.id, blob.rest_id, length(blob.content)
        #   AS length_1, blob.length
        # FROM blob JOIN anon_1 ON blob.rest_id = anon_1.rest_id]
        # which sqlite throws a syntax error on

        if str(self.parent.engine.url).find('sqlite') == -1:
            val = select(values(column('rest_id', String), name='v').data(
                [(x,) for x in blob_rest_ids])).cte()
        else:
            # OTOH chaining up UNION ALL works fine and we don't expect it
            # to be so much worse as to make a difference anywhere you
            # would actually use sqlite!
            literals = [select(literal(x).label('rest_id')) for x in blob_rest_ids]
            val = reduce(lambda x,y: x.union_all(y),
                         literals[1:], literals[0].cte())

        sel = select(self.parent.blob_table.c.id,
                     self.parent.blob_table.c.rest_id,
                     func.length(self.parent.blob_table.c.content),
                     self.parent.blob_table.c.length
                     ).join_from(self.parent.blob_table, val,
                                 self.parent.blob_table.c.rest_id ==
                                 val.c.rest_id)

        res = db_tx.execute(sel)
        ids = {}
        for row in res:
               blob_id, rest_id, length, content_length = row

               done = (length == content_length)
               ids[rest_id] = (blob_id, done)

        out = []
        for rest_id in blob_rest_ids:
            if rest_id not in ids:
                raise ValueError()  # invalid rest id
            blob_id, done = ids[rest_id]
            out.append((blob_id, rest_id, done))

        return out

    def write_envelope(self,
                       tx_delta : TransactionMetadata,
                       reuse_blob_rest_id : List[str] = [],
                       final_attempt_reason : Optional[str] = None,
                       finalize_attempt : Optional[bool] = None,
                       next_attempt_time : Optional[int] = None,
                       notification_done : Optional[bool] = None):
        with self.parent.begin_transaction() as db_tx:
            self._write(db_tx=db_tx,
                        tx_delta=tx_delta,
                        reuse_blob_rest_id=reuse_blob_rest_id,
                        require_finalized_blobs=True,
                        final_attempt_reason=final_attempt_reason,
                        finalize_attempt=finalize_attempt,
                        next_attempt_time = next_attempt_time,
                        notification_done=notification_done)
        self.parent.tx_versions.update(self.id, self.version)

    def _write_blob(self,
                    db_tx,
                    tx,
                    upd,  # sa update
                    reuse_blob_rest_id : List[str] = [],
                    require_finalized = True
                    ) -> Tuple[Any, bool]:  # SA update
        sel_blobrefs = (
            select(self.parent.tx_blobref_table.c.rest_id)
            .where(self.parent.tx_blobref_table.c.transaction_id == self.id))
        res = db_tx.execute(sel_blobrefs)
        refd_blobs = set()
        for row in res:
            refd_blobs.add(row[0])
        unrefd_blobs = [b for b in reuse_blob_rest_id if b not in refd_blobs]

        reuse_blob_id = self._reuse_blob(db_tx, unrefd_blobs)
        logging.debug('_write_blob reuse_blob_id %s', reuse_blob_id)

        if reuse_blob_id and require_finalized:
            for blob_id, rest_id, done in reuse_blob_id:
                if not done:
                    raise ValueError()

        logging.debug('TransactionCursor._write_blob %d body %s',
                      self.id, tx.body)

        if tx.body:
            assert reuse_blob_rest_id == [tx.body]
            assert len(reuse_blob_id) == 1
            assert reuse_blob_id[0][1] == tx.body
            logging.debug('TransactionCursor._write_blob %d reuse', self.id)
            upd = upd.values(
                body_blob_id = reuse_blob_id[0][0],
                body_rest_id = reuse_blob_rest_id[0])

        blobrefs = []
        blobs_done = True
        for blob_id, rest_id, input_done in reuse_blob_id:
            if not input_done:
                blobs_done = False
            blobrefs.append({ "transaction_id": self.id,
                              "blob_id": blob_id,
                              "rest_id": rest_id })
        logging.debug('TransactionCursor._write_blob %d %s', self.id, blobrefs)

        if blobrefs:
            ins = insert(self.parent.tx_blobref_table).values(blobrefs)
            res = db_tx.execute(ins)

        return upd, blobs_done

    def _write(self,
               db_tx,
               tx_delta : TransactionMetadata,
               reuse_blob_rest_id : List[str] = [],
               require_finalized_blobs = True,
               final_attempt_reason : Optional[str] = None,
               finalize_attempt : Optional[bool] = None,
               next_attempt_time : Optional[int] = None,
               notification_done : Optional[bool] = None,
               # only for upcalls from BlobWriter
               input_done = False):
        assert final_attempt_reason != 'oneshot'  # internal-only

        assert self.final_attempt_reason == 'oneshot' or self.final_attempt_reason is None or final_attempt_reason is None

        assert self.tx is not None
        tx_to_db = self.tx.merge(tx_delta)
        # e.g. overwriting an existing field
        # xxx return/throw
        assert tx_to_db is not None
        tx_to_db_json = tx_to_db.to_json(WhichJson.DB)
        attempt_json = tx_to_db.to_json(WhichJson.DB_ATTEMPT)
        logging.debug(
            'TransactionCursor._write %d %s version=%d '
            'final_attempt_reason=%s %s %s',
            self.id, self.rest_id, self.version, final_attempt_reason,
            tx_to_db_json, attempt_json)
        new_version = self.version + 1
        # TODO all updates '... WHERE inflight_session_id =
        #   self.parent.session' ?
        now = int(time.time())
        upd = (update(self.parent.tx_table)
               .where(self.parent.tx_table.c.id == self.id,
                      self.parent.tx_table.c.version == self.version)
               .values(json = tx_to_db_json,
                       version = new_version,
                       last_update = now)
               .returning(self.parent.tx_table.c.version))

        if attempt_json:
            upd_att = (update(self.parent.attempt_table)
                       .where(self.parent.attempt_table.c.transaction_id ==
                                self.id,
                              self.parent.attempt_table.c.attempt_id ==
                                self.attempt_id)
                       .values(responses = attempt_json))
            res = db_tx.execute(upd_att)

        upd = upd.values(notification = bool(tx_to_db.notification))

        # TODO possibly assert here, the first case is
        # downstream/Exploder, #2/3 are upstream/OutputHandler
        if ((tx_to_db.retry is not None or tx_to_db.notification is not None
             ) and self.final_attempt_reason == 'oneshot'):
            self.final_attempt_reason = None
            upd = upd.values(final_attempt_reason = None)
        elif final_attempt_reason:
            upd = upd.values(
                no_final_notification = not bool(tx_to_db.notification),
                final_attempt_reason = final_attempt_reason)
        elif notification_done:
            assert tx_to_db.notification
            upd = upd.values(no_final_notification = False)

        if finalize_attempt:
            upd = upd.values(inflight_session_id = None,
                             next_attempt_time = next_attempt_time)

        # XXX update preconditions
        # if reuse_blob_rest_id and (
        #         not tx_delta.body and not tx_delta.message_builder):
        #     raise ValueError()

        blobs_done = True
        if reuse_blob_rest_id:
            upd,blobs_done = self._write_blob(
                db_tx, tx_delta, upd, reuse_blob_rest_id,
                require_finalized_blobs)

        if tx_delta.message_builder:
            upd = upd.values(message_builder = tx_delta.message_builder)
        if input_done or (
                blobs_done and (tx_delta.body or tx_delta.message_builder)):
            upd = upd.values(input_done = True)
            self.input_done = True

        res = db_tx.execute(upd)
        row = res.fetchone()

        if row is None or row[0] != new_version:
            logging.info('Storage._write version conflict id=%d '
                         'expected %d db %s', self.id, new_version, row)
            raise VersionConflictException()
        self.version = row[0]

        # TODO or RETURNING json
        # XXX doesn't include response fields? tx.merge() should
        # handle that?
        self.tx = self.tx.merge(tx_delta)

        logging.info('_write id=%d %s version=%d',
                     self.id, self.rest_id, self.version)

        return True

    def load(self, db_id : Optional[int] = None,
             rest_id : Optional[str] = None,
             start_attempt : bool = False) -> Optional[TransactionMetadata]:
        if self.id is not None:
            assert(db_id is None and rest_id is None)
            db_id = self.id
        assert(db_id is not None or rest_id is not None)
        started = False
        with self.parent.begin_transaction() as db_tx:
            res = self._load_db(db_tx, db_id, rest_id)
            if start_attempt:
                self._start_attempt_db(db_tx, self.id, self.version)
                started = True
        if started:
            self.parent.tx_versions.update(self.id, self.version)
        return res

    def _load_db(self, db_tx,
                 db_id : Optional[int] = None,
                 rest_id : Optional[str] = None
                 ) -> Optional[TransactionMetadata]:
        where = None
        where_id = None

        sel = select(self.parent.tx_table.c.id,
                     self.parent.tx_table.c.rest_id,
                     self.parent.tx_table.c.creation,
                     self.parent.tx_table.c.json,
                     self.parent.tx_table.c.version,
                     self.parent.tx_table.c.last,
                     self.parent.tx_table.c.input_done,
                     self.parent.tx_table.c.final_attempt_reason,
                     self.parent.tx_table.c.body_rest_id,
                     self.parent.tx_table.c.message_builder,
                     self.parent.tx_table.c.no_final_notification,
                     self.parent.tx_table.c.body_blob_id)

        if db_id is not None:
            sel = sel.where(self.parent.tx_table.c.id == db_id)
        elif rest_id is not None:
            sel = sel.where(self.parent.tx_table.c.rest_id == rest_id)
        res = db_tx.execute(sel)
        row = res.fetchone()
        if not row:
            return None

        (self.id,
         self.rest_id,
         self.creation,
         trans_json,
         self.version,
         self.last,
         self.input_done,
         self.final_attempt_reason,
         self.body_rest_id,
         self.message_builder,
         self.no_final_notification,
         self.body_blob_id) = row

        self.tx = TransactionMetadata.from_json(trans_json, WhichJson.DB)
        # TODO this (and the db col) are probably vestigal? The
        # references are tracked in TransactionBlobRefs and the tx
        # json is supposed to be opaque to the storage code. The
        # message_builder spec makes somewhat more sense since it
        # could have inline bodies in it and be ~large?
        self.tx.body = self.body_rest_id
        self.tx.message_builder = self.message_builder
        self.tx.tx_db_id = self.id

        sel = (select(self.parent.attempt_table.c.attempt_id,
                      self.parent.attempt_table.c.responses)
               .where(self.parent.attempt_table.c.transaction_id == self.id)
               .order_by(self.parent.attempt_table.c.attempt_id.desc())
               .limit(1))
        res = db_tx.execute(sel)
        row = res.fetchone()
        resp_json = None
        if row is not None:
            resp_json = row[1]
            if resp_json is not None:
                responses = TransactionMetadata.from_json(
                    resp_json, WhichJson.DB_ATTEMPT)
                assert self.tx is not None  # set above
                assert self.tx.merge_from(responses)

        logging.debug('TransactionCursor._load_db %d %s version=%d %s %s %s',
                      self.id, self.rest_id, self.version,
                      row, trans_json, resp_json)


        return self.tx

    def _start_attempt_db(self, db_tx, db_id, version):
        assert self.parent.session_id is not None
        self._load_db(db_tx, db_id=db_id)
        assert self.tx is not None
        new_version = version + 1
        # TODO the session_id == our session id case is the common
        # case for cutthrough where it's created in the downstream
        # flow with create_leased=True but then ~synchronously loaded in
        # the output flow with start_attempt=True
        upd = (update(self.parent.tx_table)
               .where(self.parent.tx_table.c.id == db_id,
                      self.parent.tx_table.c.version == version,
                      or_(self.parent.tx_table.c.inflight_session_id ==
                            self.parent.session_id,
                          self.parent.tx_table.c.inflight_session_id.is_(None)))
               .values(version = new_version,
                       inflight_session_id = self.parent.session_id)
               .returning(self.parent.tx_table.c.version))

        # tx without retries enabled can only be loaded once
        if self.tx.retry is None:
            self.final_attempt_reason = "oneshot"
            upd = upd.values(final_attempt_reason = self.final_attempt_reason)

        res = db_tx.execute(upd)
        row = res.fetchone()
        if row is None or row[0] != new_version:
            raise VersionConflictException

        # This is a sort of pseudo-attempt for re-entering the
        # notification logic if it was enabled after the transaction
        # reached a final result.
        if self.no_final_notification:
            self.version = new_version
            return

        max_attempt_id = (
            select(func.max(self.parent.attempt_table.c.attempt_id)
                   .label('max'))
            .where(self.parent.attempt_table.c.transaction_id == db_id)
            .subquery())
        new_attempt_id = select(
            sa_case((max_attempt_id.c.max == None, 1),
                    else_=max_attempt_id.c.max + 1)
        ).scalar_subquery()

        ins = (insert(self.parent.attempt_table)
               .values(transaction_id = db_id,
                       attempt_id = new_attempt_id)
               .returning(self.parent.attempt_table.c.attempt_id))
        res = db_tx.execute(ins)
        assert (row := res.fetchone())
        self.attempt_id = row[0]
        self._load_db(db_tx, db_id=db_id)

    def wait(self, timeout=None) -> bool:
        with Waiter(self.parent.tx_versions, self.id, self.version, self
                    ) as id_version:
            old = self.version
            self.load()
            if self.version > old:
                return True
            old = self.version

            if not id_version.wait(old, timeout):
                logging.debug('TransactionCursor.wait timed out')
                return False
            self.load()
            if self.version <= old:
                logging.critical(
                    'TransactionCursor.wait() BUG id=%d old=%d new=%d',
                    self.id, old, self.version)
            return self.version > old  # xxx assert?


class BlobWriter(WritableBlob):
    id = None
    length : int = 0  # max offset+len from BlobContent, next offset to write
    _content_length : Optional[int] = None  # overall length from content-range
    rest_id = None
    last = False
    update_tx : Optional[int] = None
    finalize_tx : bool = False

    def __init__(self, storage,
                 update_tx : Optional[int] = None,
                 finalize_tx : Optional[bool] = False):
        self.parent = storage
        self.update_tx = update_tx
        self.finalize_tx = finalize_tx

    def len(self):
        return self.length
    def content_length(self):
        return self._content_length

    def _create(self, rest_id : str, db_tx):
        ins = insert(self.parent.blob_table).values(
            last_update=int(time.time()),
            rest_id=rest_id,
            content=bytes()
        ).returning(self.parent.blob_table.c.id)

        res = db_tx.execute(ins)
        row = res.fetchone()

        self.id = row[0]
        self.rest_id = rest_id
        return self.id

    def create(self, rest_id : str):
        with self.parent.begin_transaction() as db_tx:
            self._create(rest_id, db_tx)
            return self.id

    def load(self, rest_id : str, tx_rest_id : Optional[str] = None):
        with self.parent.begin_transaction() as db_tx:
            blob_id = None
            if tx_rest_id:
                # TODO put tx_rest_id in blobrefs to save this read?
                sel_tx = select(
                    self.parent.tx_table.c.id
                ).where(
                    self.parent.tx_table.c.rest_id == tx_rest_id)

                res = db_tx.execute(sel_tx)
                tx_id = res.fetchone()[0]

                sel_blobref = select(
                    self.parent.tx_blobref_table.c.blob_id
                ).where(
                    self.parent.tx_blobref_table.c.transaction_id == tx_id,
                    self.parent.tx_blobref_table.c.rest_id == rest_id)
                res = db_tx.execute(sel_blobref)
                blob_id = res.fetchone()[0]

            sel_blob = select(
                self.parent.blob_table.c.id,
                self.parent.blob_table.c.length,
                self.parent.blob_table.c.last_update,
                func.length(self.parent.blob_table.c.content))

            if blob_id:
                sel_blob = sel_blob.where(
                    self.parent.blob_table.c.id == blob_id)
            else:
                sel_blob = sel_blob.where(
                    self.parent.blob_table.c.rest_id == rest_id)

            res = db_tx.execute(sel_blob)
            row = res.fetchone()

        if not row:
            return None
        self.id, self._content_length, self.last_update, self.length = row
        self.rest_id = rest_id
        self.last = (self.length == self._content_length)

        return self.id

    # WritableBlob
    # TODO this should probably just take Blob and share the data
    # under the hood if isinstance(blob, BlobReader), etc
    def append_data(self, offset: int, d : bytes,
                    content_length : Optional[int] = None
                    ) -> Tuple[bool, int, Optional[int]]:
        logging.info('BlobWriter.append_data %d %s length=%d d.len=%d '
                     'content_length=%s new content_length=%s',
                     self.id, self.rest_id, self.length, len(d),
                     self._content_length, content_length)

        assert content_length is None or (
            content_length >= (offset + len(d)))

        tx_version = None
        with self.parent.begin_transaction() as db_tx:
            stmt = select(
                func.length(self.parent.blob_table.c.content),
                self.parent.blob_table.c.length).where(
                    self.parent.blob_table.c.id == self.id)
            res = db_tx.execute(stmt)
            row = res.fetchone()
            db_length, db_content_length = row
            if offset != db_length:
                return False, db_length, db_content_length
            if (db_content_length is not None) and (
                    (content_length is None) or
                    (content_length != db_content_length)):
                return False, db_length, db_content_length

            now = int(time.time())
            upd = (update(self.parent.blob_table)
                   .where(self.parent.blob_table.c.id == self.id)
                   .where(func.length(self.parent.blob_table.c.content) ==
                          self.length)
                   .where(or_(self.parent.blob_table.c.length == None,
                              self.parent.blob_table.c.length ==
                              content_length))
                   # in sqlite, the result of blob||blob seems to be text
                   .values(content =
                           cast(self.parent.blob_table.c.content.concat(d),
                                LargeBinary),
                           length = content_length,
                           last_update = now)
                   .returning(func.length(self.parent.blob_table.c.content)))

            res = db_tx.execute(upd)
            row = res.fetchone()
            logging.debug('append_data %d %d %d', row[0], self.length, len(d))
            assert row[0] == (self.length + len(d))

            self.length = row[0]
            self._content_length = content_length
            self.last = (self.length == self._content_length)
            logging.debug('append_data %d %s last=%s',
                          self.length, self._content_length, self.last)

            if self.update_tx is not None:
                cursor = self.parent.get_transaction_cursor()
                tx = cursor._load_db(db_tx, db_id=self.update_tx)
                kwargs = {}
                if self.last and self.finalize_tx:
                    kwargs['input_done'] = True
                cursor._write(db_tx, TransactionMetadata(), **kwargs)
                tx_version = cursor.version

        if self.update_tx is not None and tx_version is not None:
            self.parent.tx_versions.update(self.update_tx, tx_version)


        return True, self.length, self._content_length


class BlobReader(Blob):
    blob_id : Optional[int] = None
    last = False
    length = 0  # number of bytes currently readable
    # final length declared by client in content-length header
    _content_length = None
    rest_id = None

    def __init__(self, storage):
        self.parent = storage

    def len(self):
        return self.length

    def content_length(self):
        return self._content_length

    def id(self):
        return 'storage_%s' % self.blob_id

    def _check_ref(self, db_tx, blob_id : int,
                   tx_id : Optional[int] = None) -> bool:
        if tx_id is None:
            return True

        sel_ref = select(
            self.parent.tx_blobref_table.c.transaction_id,
            self.parent.tx_blobref_table.c.blob_id
        ).where(
            self.parent.tx_blobref_table.c.transaction_id == tx_id,
            self.parent.tx_blobref_table.c.blob_id == blob_id)
        ref_res = db_tx.execute(sel_ref)
        ref_row = ref_res.fetchone()
        logging.debug('BlobReader._check_ref blobref %s', ref_row)
        return ref_row and ref_row[0]

    # tx_id should be passed in most situations to verify that the
    # blob is referenced by the transaction in TransactionBlobRefs
    # no_tx_id must only be used in a path that is adding a blob
    # reference to a transaction
    def load(self, db_id = None, rest_id = None,
             tx_id : Optional[int] = None,
             no_tx_id : Optional[bool] = None
             ) -> Optional[int]:
        assert tx_id is not None or no_tx_id
        if self.blob_id:
            db_id = self.blob_id

        sel = select(
            self.parent.blob_table.c.id,
            self.parent.blob_table.c.rest_id,
            self.parent.blob_table.c.length,
            func.length(self.parent.blob_table.c.content))
        if db_id is not None:
            sel = sel.where(self.parent.blob_table.c.id == db_id)
        elif rest_id is not None:
            sel = sel.where(self.parent.blob_table.c.rest_id == rest_id)

        with self.parent.begin_transaction() as db_tx:
            res = db_tx.execute(sel)
            row = res.fetchone()
            if not row:
                return None
            blob_id = row[0]
            if not self._check_ref(db_tx, blob_id, tx_id):
                return None

        self.blob_id = row[0]
        self.rest_id = row[1]
        self._content_length = row[2]
        self.length = row[3]
        self.last = (self.length == self._content_length)

        return self.length

    def read(self, offset, length=None) -> Optional[bytes]:
        # TODO this should maybe have the same effect as load() if the
        # blob isn't finalized?
        l = length if length else self.length - offset
        stmt = (
            select(func.substr(self.parent.blob_table.c.content, offset+1, l))
            .where(self.parent.blob_table.c.id == self.blob_id))
        with self.parent.begin_transaction() as db_tx:
            res = db_tx.execute(stmt)
            row = res.fetchone()
            logging.debug('read blob row %s', row)  # xxx debug
            if row is None:
                return None
            if row[0] is None:
                return bytes()
            return row[0]

    # wait for self.length to increase or timeout
    def wait(self, timeout=None):
        # version is len(content) in BlobContent
        with Waiter(self.parent.blob_versions, self.blob_id, self.length, self
                    ) as id_version:
            old_len = self.length
            self.load()
            if self.length > old_len:
                return True
            if self.last:
                return False
            old_len = self.length
            if not id_version.wait(old_len, timeout):
                return False
            self.load()
            return self.length > old_len

class IdVersion:
    id : int
    lock : Lock
    cv : Condition

    version : int
    waiters : set[object]
    def __init__(self, db_id, version):
        self.id = db_id
        self.lock = Lock()
        self.cv = Condition(self.lock)

        self.waiters = set()
        self.version = version

    def wait(self, version, timeout):
        with self.lock:
            logging.debug('IdVersion.wait %d %d %d %d',
                          id(self), self.id, self.version, version)
            rv = self.cv.wait_for(lambda: self.version > version, timeout)
            logging.debug('IdVersion.wait done %d %d %d %d',
                          self.id, self.version, version, rv)
            return rv

    def update(self, version):
        with self.lock:
            logging.debug('IdVersion.update %d id=%d version=%d new %d',
                          id(self), self.id, self.version, version)
            # There is an expected edge case case where a waiter reads
            # the db and updates this in between a write commiting to
            # the db and updating this so == is expected
            if version < self.version:
                logging.critical('IdVersion.update precondition failure '
                                 ' id=%d cur=%d new=%d',
                                 self.id, self.version, version)
                assert not 'IdVersion.update precondition failure '
            self.version = version
            self.cv.notify_all()

class Waiter:
    def __init__(self, parent, db_id, version, obj):
        self.parent = parent
        self.db_id = db_id
        self.obj = obj
        self.id_version = self.parent.get_id_version(db_id, version, obj)
    def __enter__(self):
        return self.id_version
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.parent.del_waiter(self.db_id, self.id_version, self.obj)

class IdVersionMap:
    lock : Lock
    id_version_map : dict[int,IdVersion]  # db-id

    def __init__(self):
        self.id_version_map = {}
        self.lock = Lock()

    def del_waiter(self, id, id_version, obj):
        with self.lock:
            id_version.waiters.remove(obj)
            if not id_version.waiters:
                del self.id_version_map[id]

    def update(self, db_id, version):
        with self.lock:
            if db_id not in self.id_version_map:
                logging.debug('IdVersionMap.update id=%d version %d no waiters',
                              db_id, version)
                return
            waiter = self.id_version_map[db_id]
            waiter.update(version)

    def get_id_version(self, db_id, version, obj):
        with self.lock:
            if db_id not in self.id_version_map:
                self.id_version_map[db_id] = IdVersion(db_id, version)
            waiter = self.id_version_map[db_id]
            waiter.waiters.add(obj)
            return waiter


class Storage():
    session_id = None
    tx_versions : IdVersionMap
    engine : Optional[Engine] = None

    session_table : Optional[Table] = None
    blob_table : Optional[Table] = None
    tx_table : Optional[Table] = None
    tx_blobref_table : Optional[Table] = None
    attempt_table : Optional[Table] = None

    def __init__(self, engine : Optional[Engine] = None):
        self.engine = engine
        self.tx_versions = IdVersionMap()

    @staticmethod
    def get_sqlite_inmemory_for_test():
        engine = create_engine("sqlite+pysqlite://",
                               connect_args={'check_same_thread':False},
                               poolclass=QueuePool,
                               pool_size=1, max_overflow=0)
        with engine.connect() as conn:
            dbapi_conn = conn.connection
            with open("init_storage.sql", "r") as f:
                cursor = dbapi_conn.cursor()
                # however SA is proxying all the methods from the
                # underlying sqlite dbapi cursor isn't visible to
                # pytype
                # pytype: disable=attribute-error
                cursor.executescript(f.read())
                # pytype: enable=attribute-error
        s = Storage(engine)
        s._init_session()
        return s

    @staticmethod
    def connect_sqlite(filename):
        engine = create_engine("sqlite+pysqlite:///" + filename,
                               pool_size=1, max_overflow=0)
        with engine.begin() as db_tx:
            cursor = db_tx.connection.cursor()
            # should be sticky from schema but set it here anyway
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA foreign_keys=ON")
            # https://www.sqlite.org/pragma.html#pragma_synchronous
            # FULL=2, flush WAL on every write,
            # NORMAL=1 not durable after power loss
            cursor.execute("PRAGMA synchronous=2")
            cursor.execute("PRAGMA auto_vacuum=2")

        s = Storage(engine)
        s._init_session()
        return s

    @staticmethod
    def connect_postgres(
            db_user=None, db_name=None, host=None, port=None,
            unix_socket_dir=None):
        db_url = 'postgresql+psycopg://' + db_user + '@'
        if host:
            db_url += '%s:%d' % (host, port)
        db_url += '/' + db_name
        if unix_socket_dir:
            db_url += ('?host=' + unix_socket_dir)
            if port:
                db_url += ('&port=%d' % port)
        logging.info('Storage.connect_postgres %s', db_url)
        engine = create_engine(db_url)
        s = Storage(engine)
        s._init_session()
        return s


    def begin_transaction(self):
        return self.engine.begin()

    def _init_session(self):
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)
        # the tablenames seem to be lowercased for postgres, sqlite
        # doesn't care?
        self.session_table = Table(
            'sessions', self.metadata, autoload_with=self.engine)
        self.blob_table = Table(
            'blob', self.metadata, autoload_with=self.engine)
        self.tx_table = Table(
            'transactions', self.metadata, autoload_with=self.engine)
        self.tx_blobref_table = Table(
            'transactionblobrefs', self.metadata, autoload_with=self.engine)

        self.attempt_table = Table(
            'transactionattempts', self.metadata, autoload_with=self.engine)

        with self.begin_transaction() as db_tx:
            # for the moment, we only evict sessions that the pid no
            # longer exists but as this evolves, we might also
            # periodically check that our own session hasn't been evicted
            proc_self = psutil.Process()
            ins = (insert(self.session_table).values(
                pid = proc_self.pid,
                pid_create = int(proc_self.create_time()))
                   .returning(self.session_table.c.id))
            res = db_tx.execute(ins)
            self.session_id = res.fetchone()[0]

    def recover(self):
        with self.begin_transaction() as db_tx:
            sel = select(self.session_table.c.id,
                         self.session_table.c.pid,
                         self.session_table.c.pid_create)
            res = db_tx.execute(sel)
            for row in res:
                (db_id, pid, pid_create) = row
                if not Storage.check_pid(pid, pid_create):
                    logging.info('deleting stale session %s %s %s',
                                 db_id, pid, pid_create)
                    dele = (delete(self.session_table)
                            .where(self.session_table.c.id == db_id))
                    db_tx.execute(dele)

    @staticmethod
    def check_pid(pid, pid_create):
        try:
            proc = psutil.Process(pid)
        except psutil.NoSuchProcess:
            return False
        if int(proc.create_time()) != pid_create:
            return False
        return True

    def get_blob_writer(self) -> BlobWriter:
        return BlobWriter(self)
    def get_blob_reader(self) -> BlobReader:
        return BlobReader(self)

    def create_blob(self,
                    tx_rest_id : str,
                    blob_rest_id : Optional[str] = None,
                    tx_body : bool = False,
                    ) -> Optional[WritableBlob]:
        with self.begin_transaction() as db_tx:
            writer = None
            writer = BlobWriter(self)
            writer._create(blob_rest_id, db_tx)

            cursor = self.get_transaction_cursor()
            cursor._load_db(db_tx, rest_id=tx_rest_id)
            tx = TransactionMetadata()
            if tx_body:
                tx.body=blob_rest_id
            cursor._write(
                db_tx, tx,
                reuse_blob_rest_id=[blob_rest_id],
                require_finalized_blobs=False)
            if writer:
                writer.update_tx = cursor.id
                writer.finalize_tx = tx_body

            return writer

    def get_blob_for_append(
            self,
            tx_rest_id : str,
            blob_rest_id : Optional[str] = None,
            tx_body : bool = False
    ) -> Optional[WritableBlob]:
        tx_cursor = self.get_transaction_cursor()
        tx_cursor.load(rest_id=tx_rest_id)
        if tx_body:
            blob_rest_id = tx_cursor.body_rest_id

        blob_writer = BlobWriter(self)
        if blob_writer.load(blob_rest_id, tx_rest_id) is None:
            return None
        blob_writer.update_tx = tx_cursor.id
        blob_writer.finalize_tx = tx_body

        return blob_writer

    def get_transaction_cursor(self) -> TransactionCursor:
        return TransactionCursor(self)

    def load_one(self):
        with self.begin_transaction() as db_tx:
            # TODO this is currently a scan, index on/ORDER BY
            # next_attempt_time?

            # NOTE we currently don't recover !input_done from
            # different sessions. We set final_attempt_reason in
            # _start_attempt_db() if retries aren't enabled so non-durable
            # transactions effectively can only be loaded once.

            # TODO We need to surface some more information in the
            # rest tx (at least attempt#) for the client to have a
            # chance of handling it correctly. Longer-term, we'd like
            # to make this work. Possibly we need a more explicit
            # "handoff" from the input side to the output side,
            # otherwise another process/instance might steal it?

            # TODO this should not recover newly-inserted/cutthrough
            now = int(time.time())
            sel = (select(self.tx_table.c.id,
                          self.tx_table.c.version)
                   .where(self.tx_table.c.inflight_session_id.is_(None),
                          or_(
                              and_(
                                  or_(self.tx_table.c.input_done.is_(True),
                                      self.tx_table.c.creation_session_id ==
                                      self.session_id),
                                  or_(self.tx_table.c.next_attempt_time.is_(None),
                                      self.tx_table.c.next_attempt_time <= now),
                                  self.tx_table.c.final_attempt_reason.is_(None),
                                  self.tx_table.c.json.is_not(None)),
                              and_(
                                  # TODO it seems like it should be possible to
                                  # query effectively
                                  # json.get('notification', {}) != {}
                                  # but I haven't been able to get SA to do it
                                  self.tx_table.c.notification.is_(True),
                                  self.tx_table.c.no_final_notification.is_(True)
                              )))
                   .limit(1))
            res = db_tx.execute(sel)
            row = res.fetchone()
            if not row:
                return None
            db_id,version = row

            tx = self.get_transaction_cursor()
            tx._start_attempt_db(db_tx, db_id, version)
            self.tx_versions.update(tx.id, tx.version)

            # TODO: if the last n consecutive attempts weren't
            # finalized, this transaction may be crashing the system
            # -> quarantine

            return tx

    def _gc(self, db_tx, ttl : int) -> Tuple[int, int]:
        # It would be fairly easy to support a staged policy like ttl
        # blobs after 1d but tx after 7d. Then there would be a
        # separate delete from TransactionBlobRefs with the shorter
        # ttl, etc.
        # This could also be finer-grained in terms of shorter ttl for
        # !input_done, blobs, etc.
        del_tx = delete(self.tx_table).where(
            self.tx_table.c.inflight_session_id == None,
            or_(self.tx_table.c.input_done.is_not(sa_true()),
                self.tx_table.c.final_attempt_reason.is_not(None)),
            (time.time() - self.tx_table.c.last_update) > ttl)
        res = db_tx.execute(del_tx)
        deleted_tx = rowcount(res)

        # If you wanted to reuse a blob for longer than the ttl
        # (unlikely?), you might need tx creation reusing the
        # blob to bump the blob last_update so it stays live?
        j = join(self.blob_table, self.tx_blobref_table,
             self.blob_table.c.id == self.tx_blobref_table.c.blob_id,
             isouter=True)
        sel = (select(self.blob_table.c.id).select_from(j)
               .where(self.tx_blobref_table.c.transaction_id == None))
        del_blob = delete(self.blob_table).where(
            (time.time() - self.blob_table.c.last_update) > ttl,
            self.blob_table.c.id.in_(sel)
        )
        res = db_tx.execute(del_blob)
        deleted_blob = rowcount(res)
        return deleted_tx, deleted_blob

    def gc(self, ttl) -> Tuple[int, int]:
        with self.begin_transaction() as db_tx:
            deleted = self._gc(db_tx, ttl)
            return deleted

