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
from storage_schema import (
    TX_BODY,
    VersionConflictException,
    body_blob_uri )
from filter import TransactionMetadata, WhichJson
from rest_schema import BlobUri

from version_cache import IdVersion, IdVersionMap

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

    id_version : Optional[IdVersion] = None

    def __init__(self, storage,
                 db_id : Optional[int] = None,
                 rest_id : Optional[str] = None):
        self.parent = storage
        self.id = db_id
        self.rest_id = rest_id
        if (self.id is not None) or (self.rest_id is not None):
            self.id_version = self.parent.tx_versions.get(self.id, self.rest_id)
            if self.id_version is not None:
                self.version = self.id_version.version

    def _update_version_cache(self):
        assert (self.id is not None) and (self.rest_id is not None)
        id_version = self.parent.tx_versions.insert_or_update(
            self.id, self.rest_id, self.version)
        if self.id_version is not None:
            assert id_version == self.id_version
        self.id_version = id_version

    def create(self,
               rest_id : str,
               tx : TransactionMetadata,
               reuse_blob_rest_id : List[BlobUri] = [],
               create_leased : bool = False,
               blobs_to_create : bool = False):
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
            self.rest_id = rest_id
            self.tx = TransactionMetadata()  # for _write() to take a delta?

            reuse_blob = None
            if reuse_blob_rest_id is not None:
                reuse_blob = []
                for blob in reuse_blob_rest_id:
                    blob = body_blob_uri(blob)
                    reuse_blob.append(blob)

            self._write(db_tx, tx, reuse_blob_rest_id=reuse_blob,
                        blobs_to_create=blobs_to_create)
            self.tx.rest_id = rest_id

        self._update_version_cache()

    def _reuse_blob(self, db_tx, blob_rest_id : List[BlobUri]
                    ) -> List[Tuple[int, str, bool]]:
                    # -> id, rest_id, input_done
        sel_blobrefs = (
            select(self.parent.tx_blobref_table.c.rest_id)
            .where(self.parent.tx_blobref_table.c.transaction_id == self.id))

        res = db_tx.execute(sel_blobrefs)
        refd_blobs = set()
        for row in res:
            refd_blobs.add(row[0])
        unrefd_blobs = [b for b in blob_rest_id
                        if b.blob not in refd_blobs]

        logging.debug('_write_blob reuse %s', blob_rest_id)
        logging.debug('_write_blob refd %s', refd_blobs)
        logging.debug('_write_blob unrefd %s', unrefd_blobs)

        if not unrefd_blobs:
            return []
        assert isinstance(unrefd_blobs, list)

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
            val = select(
                values(column('rest_id', String),
                       column('tx_rest_id', String), name='v').data(
                    [(x.blob, x.tx_id) for x in unrefd_blobs])).cte()
        else:
            # OTOH chaining up UNION ALL works fine and we don't expect it
            # to be so much worse as to make a difference anywhere you
            # would actually use sqlite!
            literals = [ select(literal(uri.blob).label('rest_id'),
                                literal(uri.tx_id).label('tx_rest_id'))
                         for uri in unrefd_blobs ]
            val = reduce(lambda x,y: x.union_all(y),
                         literals[1:], literals[0].cte())

        j = join(self.parent.blob_table,
                 self.parent.tx_blobref_table,
                 self.parent.blob_table.c.id ==
                   self.parent.tx_blobref_table.c.blob_id)

        j2 = join(j, val, and_(
            self.parent.tx_blobref_table.c.tx_rest_id ==
            val.c.tx_rest_id,
            self.parent.tx_blobref_table.c.rest_id ==
            val.c.rest_id))

        sel = select(self.parent.tx_blobref_table.c.blob_id,
                     self.parent.tx_blobref_table.c.rest_id,
                     func.length(self.parent.blob_table.c.content),
                     self.parent.blob_table.c.length
                     ).select_from(j2)

        res = db_tx.execute(sel)
        ids = {}
        for row in res:
            logging.debug('_reuse_blob %s', row)
            blob_id, rest_id, length, content_length = row
            done = (length == content_length)
            ids[rest_id] = (blob_id, done)
        logging.debug(ids)
        logging.debug(unrefd_blobs)
        out = []
        for rest_id in unrefd_blobs:
            if rest_id.blob not in ids:
                raise ValueError()  # invalid rest id
            blob_id, done = ids[rest_id.blob]
            out.append((blob_id, rest_id.blob, done))

        return out

    def _ref_blob(self, db_tx, blob_id : int, blob_uri : BlobUri
                  ) -> List[Tuple[int, str, bool]]:
        res = db_tx.execute(
            select(func.length(self.parent.blob_table.c.content),
                   self.parent.blob_table.c.length).where(
                       self.parent.blob_table.c.id == blob_id))
        if not res:
            return []
        row = res.fetchone()
        if row is None:
            return []
        return [(blob_id, blob_uri.blob, row[0] == row[1])]

    def write_envelope(self,
                       tx_delta : TransactionMetadata,
                       reuse_blob_rest_id : List[BlobUri] = [],
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
        self._update_version_cache()

    def _write_blob(self,
                    db_tx,
                    tx,
                    upd,  # sa update
                    reuse_blob_rest_id : List[BlobUri] = [],
                    ref_blob_id : Optional[Tuple[int, BlobUri]] = None,
                    require_finalized = True
                    ) -> Tuple[Any, bool]:  # SA update
        assert reuse_blob_rest_id or ref_blob_id
        assert not(reuse_blob_rest_id and ref_blob_id)
        if ref_blob_id:
            reuse_blob_id = self._ref_blob(
                db_tx, ref_blob_id[0], ref_blob_id[1])
        elif reuse_blob_rest_id:
            reuse_blob_id = self._reuse_blob(db_tx, reuse_blob_rest_id)
            logging.debug('_write_blob reuse_blob_id %s', reuse_blob_id)

        if reuse_blob_id and require_finalized:
            for blob_id, rest_id, done in reuse_blob_id:
                if not done:
                    raise ValueError()

        logging.debug('TransactionCursor._write_blob %d body %s',
                      self.id, tx.body)

        blobrefs = []
        blobs_done = True
        for blob_id, rest_id, input_done in reuse_blob_id:
            if not input_done:
                blobs_done = False
            blobrefs.append({ "transaction_id": self.id,
                              "tx_rest_id": self.rest_id,
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
               reuse_blob_rest_id : List[BlobUri] = [],
               ref_blob_id : Optional[Tuple[int, BlobUri]] = None,
               require_finalized_blobs = True,
               final_attempt_reason : Optional[str] = None,
               finalize_attempt : Optional[bool] = None,
               next_attempt_time : Optional[int] = None,
               notification_done : Optional[bool] = None,
               # only for upcalls from BlobWriter
               input_done = False,
               ping_tx = False,
               blobs_to_create = False):
        logging.debug('TxCursor._write %s %s', self.rest_id, tx_delta)
        assert final_attempt_reason != 'oneshot'  # internal-only

        assert (self.final_attempt_reason == 'oneshot' or
                self.final_attempt_reason is None or
                final_attempt_reason is None)

        assert self.tx is not None

        if (tx_delta.empty(WhichJson.DB) and
            tx_delta.empty(WhichJson.DB_ATTEMPT) and
            (not tx_delta.body) and
            (not reuse_blob_rest_id) and
            (ref_blob_id is None) and
            (final_attempt_reason is None) and
            (notification_done is None) and
            (not input_done) and
            (not finalize_attempt) and
            (not ping_tx)):
            logging.debug(
                'TransactionCursor._write %d %s empty delta %s',
                self.id, self.rest_id, tx_delta)
            return True

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

        blobs_done = True

        if reuse_blob_rest_id or ref_blob_id:
            upd,blobs_done = self._write_blob(
                db_tx, tx_delta, upd, reuse_blob_rest_id,
                ref_blob_id=ref_blob_id,
                require_finalized=require_finalized_blobs)

        logging.debug('_write %s blobs_done %s', self.rest_id, blobs_done)
        if tx_delta.message_builder:
            upd = upd.values(message_builder = tx_delta.message_builder)
        if input_done or (
                (tx_delta.message_builder and not blobs_to_create) or
                (reuse_blob_rest_id is not None
                 and len(reuse_blob_rest_id) == 1 and
                 reuse_blob_rest_id[0].tx_body)):
            logging.debug('_write %s input_done', self.rest_id)
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
        if self.id is not None or self.rest_id is not None:
            assert(db_id is None and rest_id is None)
            db_id = self.id
            rest_id = self.rest_id
        else:
            assert(db_id is not None or rest_id is not None)
        started = False
        with self.parent.begin_transaction() as db_tx:
            res = self._load_db(db_tx, db_id, rest_id)
            if start_attempt:
                self._start_attempt_db(db_tx, self.id, self.version)
                started = True
        if res is not None:
            self._update_version_cache()
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
                     self.parent.tx_table.c.message_builder,
                     self.parent.tx_table.c.no_final_notification)

        if db_id is not None:
            sel = sel.where(self.parent.tx_table.c.id == db_id)
        elif rest_id is not None:
            sel = sel.where(self.parent.tx_table.c.rest_id == rest_id)
        else:
            raise ValueError
        res = db_tx.execute(sel)
        row = res.fetchone()
        if not row:
            return None

        if self.id is not None:
            assert row[0] == self.id
        if self.rest_id is not None:
            assert row[1] == self.rest_id

        (self.id,
         self.rest_id,
         self.creation,
         trans_json,
         self.version,
         self.last,
         self.input_done,
         self.final_attempt_reason,
         self.message_builder,
         self.no_final_notification) = row

        self.tx = TransactionMetadata.from_json(trans_json, WhichJson.DB)
        if self.tx is None:
            return None

        sel_body = select(self.parent.tx_blobref_table.c.blob_id).where(
            self.parent.tx_blobref_table.c.transaction_id == self.id,
            self.parent.tx_blobref_table.c.rest_id == TX_BODY)
        res = db_tx.execute(sel_body)
        if res and res.fetchone():
            # TODO this is a minor kludge, this needs to be any
            # non-empty string to be converted to a placeholder in the
            # rest read path
            self.tx.body = 'b'
        self.tx.message_builder = self.message_builder
        self.tx.tx_db_id = self.id
        assert self.tx.rest_id is None or (self.tx.rest_id == self.rest_id)
        self.tx.rest_id = self.rest_id

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
        self._update_version_cache()

    def wait(self, timeout : Optional[float] = None) -> bool:
        return self.id_version.wait(self.version, timeout)

    async def wait_async(self, timeout : float) -> bool:
        return await self.id_version.wait_async(self.version, timeout)


    # returns True if all blobs ref'd from this tx are finalized
    def check_input_done(self, db_tx) -> bool:
        j = join(self.parent.blob_table, self.parent.tx_blobref_table,
             self.parent.blob_table.c.id == self.parent.tx_blobref_table.c.blob_id,
             isouter=False)
        sel = (select(self.parent.blob_table.c.id).select_from(j)
               .where(self.parent.tx_blobref_table.c.transaction_id == self.id,
                      or_(self.parent.blob_table.c.length.is_(None),
                          func.length(self.parent.blob_table.c.content) !=
                          self.parent.blob_table.c.length))
               .limit(1))
        res = db_tx.execute(sel)
        row = res.fetchone()
        return row is None


class BlobCursor(Blob, WritableBlob):
    id = None
    length : int = 0  # max offset+len from BlobContent, next offset to write
    _content_length : Optional[int] = None  # overall length from content-range
    last = False
    update_tx : Optional[str] = None
    blob_uri : Optional[BlobUri] = None

    def __init__(self, storage,
                 update_tx : Optional[str] = None,
                 finalize_tx : Optional[bool] = False):
        self.parent = storage
        self.update_tx = update_tx

    def len(self):
        return self.length
    def content_length(self):
        return self._content_length

    def _create(self, db_tx):
        ins = insert(self.parent.blob_table).values(
            last_update=int(time.time()),
            content=bytes()
        ).returning(self.parent.blob_table.c.id)

        res = db_tx.execute(ins)
        row = res.fetchone()

        self.id = row[0]
        return self.id

    # TODO only used in a few tests now?
    def create(self):
        with self.parent.begin_transaction() as db_tx:
            self._create(db_tx)
        return self.id

    def load(self, blob_uri : Optional[BlobUri] = None,
             blob_id : Optional[int] = None):
        with self.parent.begin_transaction() as db_tx:
            if self.id is not None:
                blob_id = self.id
            elif blob_id is not None:
                pass
            elif blob_uri is not None:
                self.blob_uri = blob_uri
                sel_blobref = select(
                    self.parent.tx_blobref_table.c.blob_id
                ).where(
                    self.parent.tx_blobref_table.c.tx_rest_id == blob_uri.tx_id,
                    self.parent.tx_blobref_table.c.rest_id == blob_uri.blob)
                res = db_tx.execute(sel_blobref)
                row = res.fetchone()
                if row is None:
                    return None
                blob_id = row[0]
            else:
                raise ValueError()

            sel_blob = select(
                self.parent.blob_table.c.id,
                self.parent.blob_table.c.length,
                self.parent.blob_table.c.last_update,
                func.length(self.parent.blob_table.c.content)).where(
                    self.parent.blob_table.c.id == blob_id)

            res = db_tx.execute(sel_blob)
            row = res.fetchone()

        if not row:
            return None
        self.id, self._content_length, self.last_update, self.length = row
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
            while True:
                try:
                    with self.parent.begin_transaction() as db_tx:
                        cursor._load_db(db_tx, rest_id=self.update_tx)
                        kwargs = {}
                        input_done = False
                        if self.last:
                            input_done = cursor.check_input_done(db_tx)
                        if input_done:
                            kwargs['input_done'] = True
                        else:
                            kwargs['ping_tx'] = True  # ping last_update
                        logging.debug('BlobWriter.append_data tx %s %s',
                                      self.update_tx, kwargs)
                        cursor._write(db_tx, TransactionMetadata(), **kwargs)
                        break
                except VersionConflictException:
                    pass
            cursor._update_version_cache()
        return True, self.length, self._content_length


    def read(self, offset, length=None) -> Optional[bytes]:
        # TODO this should maybe have the same effect as load() if the
        # blob isn't finalized?
        l = length if length else self.length - offset
        stmt = (
            select(func.substr(self.parent.blob_table.c.content, offset+1, l))
            .where(self.parent.blob_table.c.id == self.id))
        with self.parent.begin_transaction() as db_tx:
            res = db_tx.execute(stmt)
            row = res.fetchone()
            logging.debug('read blob row %s',
                          row if not row or not row[0] else len(row[0]))
            if row is None:
                return None
            if row[0] is None:
                return bytes()
            return row[0]


class Storage():
    session_id = None
    tx_versions : IdVersionMap
    engine : Optional[Engine] = None

    session_table : Optional[Table] = None
    blob_table : Optional[Table] = None
    tx_table : Optional[Table] = None
    tx_blobref_table : Optional[Table] = None
    attempt_table : Optional[Table] = None

    def __init__(self, version_cache : IdVersionMap,
                 engine : Optional[Engine] = None):
        self.tx_versions = version_cache
        self.engine = engine

    @staticmethod
    def get_sqlite_inmemory_for_test(
            version_cache : Optional[IdVersionMap] = None):
        if version_cache is None:
            version_cache = IdVersionMap()
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
        s = Storage(version_cache, engine)
        s._init_session()
        return s

    @staticmethod
    def connect_sqlite(version_cache : IdVersionMap, filename):
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

        s = Storage(version_cache, engine)
        s._init_session()
        return s

    @staticmethod
    def connect_postgres(
            version_cache : IdVersionMap,
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
        s = Storage(version_cache, engine)
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

    # TODO these blob methods should move into TransactionCursor?
    def create_blob(self, blob_uri : BlobUri) -> Optional[WritableBlob]:
        with self.begin_transaction() as db_tx:
            writer = None
            writer = BlobCursor(self)
            writer._create(db_tx)
            if writer is None:
                return None

        blob_uri = body_blob_uri(blob_uri)

        cursor = None
        while True:
            try:
                with self.begin_transaction() as db_tx:
                    cursor = self.get_transaction_cursor()
                    cursor._load_db(db_tx=db_tx, rest_id=blob_uri.tx_id)
                    tx = TransactionMetadata()
                    cursor._write(
                        db_tx=db_tx,
                        tx_delta=tx,
                        ref_blob_id=(writer.id, blob_uri),
                        require_finalized_blobs=False)
                    break
            except VersionConflictException:
                pass
        cursor._update_version_cache()

        writer.update_tx = blob_uri.tx_id

        return writer

    def get_blob_for_append(self, blob_uri : BlobUri) -> Optional[WritableBlob]:
        blob_uri = body_blob_uri(blob_uri)

        blob_writer = BlobCursor(self)
        if blob_writer.load(blob_uri) is None:
            return None
        blob_writer.update_tx = blob_uri.tx_id

        return blob_writer

    def get_blob_for_read(self, blob_uri : BlobUri) -> Optional[WritableBlob]:
        blob_uri = body_blob_uri(blob_uri)
        blob_reader = BlobCursor(self)
        if blob_reader.load(blob_uri) is None:
            return None
        return blob_reader


    def get_transaction_cursor(self, db_id : Optional[int] = None,
                               rest_id : Optional[str] = None
                               ) -> TransactionCursor:
        return TransactionCursor(self, db_id, rest_id)

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

