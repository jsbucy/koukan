# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
import json
import logging
from threading import Lock, Condition
from hashlib import sha256
from base64 import b64encode
from functools import reduce
from datetime import datetime, timedelta
import atexit
from contextlib import nullcontext
import copy

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, CursorResult, Engine, Transaction
from sqlalchemy.pool import QueuePool
from sqlalchemy.sql.functions import count, current_time
from sqlalchemy.sql.expression import CTE, Select, exists
from sqlalchemy import event

from sqlalchemy import (
    LargeBinary, MetaData, String, Table,
    and_, cast, case as sa_case, column,
    delete, event, func, insert, join, literal, not_, or_, select,
    true as sa_true, update, union_all, values)

from koukan.backoff import backoff
from koukan.blob import Blob, InlineBlob, WritableBlob
from koukan.response import Response
from koukan.storage_schema import (
    TX_BODY,
    BlobSpec,
    VersionConflictException,
    body_blob_uri )
from koukan.filter import TransactionMetadata, WhichJson
from koukan.rest_schema import BlobUri

from koukan.version_cache import IdVersion, IdVersionMap

from koukan.message_builder import MessageBuilderSpec

# the implementation of CursorResult.rowcount apparently involves too
# much metaprogramming for pytype to infer correctly
def rowcount(res : CursorResult) -> int:
    count = res.rowcount
    assert isinstance(count, int)
    return count

class TransactionCursor:
    db_id : Optional[int] = None
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

    in_attempt : bool = False

    # this TransactionCursor object created this tx db row
    created : bool = False

    # uri of owner session if different from this process
    session_uri : Optional[str] = None

    blobs : Optional[List['BlobCursor']] = None

    def __init__(self, storage,
                 db_id : Optional[int] = None,
                 rest_id : Optional[str] = None,
                 no_id_version = False):
        self.parent = storage
        self.db_id = db_id
        self.rest_id = rest_id
        if no_id_version:
            pass
        elif (self.db_id is not None) or (self.rest_id is not None):
            self.id_version = self.parent.tx_versions.get(self.db_id, self.rest_id)
            if self.id_version is not None:
                self.version = self.id_version.version

    def clone(self) -> 'TransactionCursor':
        out = TransactionCursor(self.parent, self.db_id, self.rest_id, no_id_version=True)
        out.copy_from(self)
        return out

    def copy_from(self, rhs : 'TransactionCursor'):
        if self.db_id is None:
            self.db_id = rhs.db_id
        else:
            assert self.db_id == rhs.db_id
        if self.rest_id is None:
            self.rest_id = rhs.rest_id
        else:
            assert self.rest_id == rhs.rest_id
        #self.attempt_id = rhs.attempt_id
        assert rhs.version is not None
        self.version = rhs.version
        #self.creation = rhs.creation
        self.input_done = rhs.input_done
        self.final_attempt_reason = rhs.final_attempt_reason
        # xxx parity with _load_db()
        if rhs.tx is not None:
            self.tx = rhs.tx.copy()
            if rhs.final_attempt_reason != 'oneshot':
                self.tx.final_attempt_reason = rhs.final_attempt_reason
            else:
                self.tx.final_attempt_reason = None
        else:
            self.tx = None

        self.message_builder = rhs.message_builder
        self.no_final_notification = rhs.no_final_notification
        #self.id_version = rhs.id_version
        # XXX self.in_attempt
        self.created = rhs.created
        self.session_uri = rhs.session_uri
        self.blobs = [b.clone() for b in rhs.blobs] if rhs.blobs else None

    def _update_version_cache(self):
        assert (self.db_id is not None) and (self.rest_id is not None) and (self.version is not None)
        clone = self.clone()
        clone.tx.version = None  # XXX

        if clone.blobs and len(clone.blobs) == 1 and clone.blobs[0].blob_uri.tx_body:
            clone.tx.body = clone.blobs[0]
            logging.debug(clone.tx.body)
        logging.debug(clone.tx)
        idv = self.parent.tx_versions.insert_or_update(
            self.db_id, self.rest_id, self.version, clone)
        if self.id_version is None:
            self.id_version = idv
        else:
            assert self.id_version == idv

    def create(self,
               rest_id : str,
               tx : TransactionMetadata,
               create_leased : bool = False):
        parent = self.parent
        with self.parent.begin_transaction() as db_tx:
            self.version = 0

            db_json = tx.to_json(WhichJson.DB)
            logging.debug('TxCursor.create %s %s', rest_id, db_json)
            ins = insert(self.parent.tx_table).values(
                rest_id = rest_id,
                creation = self.parent._current_timestamp_epoch(),
                last_update = self.parent._current_timestamp_epoch(),
                version = self.version,
                json = db_json,
                creation_session_id = self.parent.session_id,
            ).returning(self.parent.tx_table.c.id,
                        self.parent.tx_table.c.creation)

            if create_leased:
                ins = ins.values(
                    inflight_session_id = self.parent.session_id,
                    inflight_session_live = True)

            res = db_tx.execute(ins)
            row = res.fetchone()
            self.db_id = row[0]
            self.creation = row[1]
            self.rest_id = rest_id
            self.tx = TransactionMetadata()  # for _write() to take a delta?

            self._write(db_tx, tx)
            self.tx.rest_id = rest_id

        self._update_version_cache()
        self.created = True

    def _reuse_blob(self, db_tx : Connection, blob_uris : List[BlobUri]
                    ) -> List['BlobCursor']:
        assert self.rest_id is not None
        if not blob_uris:
            return []

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

        val : CTE
        if str(self.parent.engine.url).find('sqlite') == -1:
            val = select(
                values(column('rest_id', String),
                       column('tx_rest_id', String), name='v').data(
                    [(x.blob, x.tx_id) for x in blob_uris])).cte()
        else:
            # OTOH chaining up UNION ALL works fine and we don't expect it
            # to be so much worse as to make a difference anywhere you
            # would actually use sqlite!
            literals = [ select(literal(uri.blob).label('rest_id'),
                                literal(uri.tx_id).label('tx_rest_id'))
                         for uri in blob_uris ]
            val = literals[0].union_all(*literals[1:]).cte()

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
                     self.parent.blob_table.c.length,
                     self.parent.blob_table.c.last_update
                     ).select_from(j2)

        res = db_tx.execute(sel)
        ids : Set[str] = set()
        out : List[BlobCursor] = []
        for row in res:
            blob_id, blob_rest_id, length, content_length, last_update = row
            ids.add(blob_rest_id)

            if content_length is None or length != content_length:
                raise ValueError()  # not finalized
            blob_cursor = BlobCursor(self.parent)
            blob_cursor.init(self.rest_id, blob_id, blob_rest_id,
                             content_length, last_update, length)
            out.append(blob_cursor)

        for blob_uri in blob_uris:
            if blob_uri.blob not in ids:
                raise ValueError()  # invalid rest id

        return out

    def write_envelope(self,
                       tx_delta : TransactionMetadata,
                       final_attempt_reason : Optional[str] = None,
                       finalize_attempt : Optional[bool] = None,
                       next_attempt_time : Optional[int] = None,
                       notification_done : Optional[bool] = None,
                       ping_tx : bool = False):
        with self.parent.begin_transaction() as db_tx:
            self._write(db_tx=db_tx,
                        tx_delta=tx_delta,
                        final_attempt_reason=final_attempt_reason,
                        finalize_attempt=finalize_attempt,
                        next_attempt_time = next_attempt_time,
                        notification_done=notification_done,
                        ping_tx=ping_tx)
        logging.debug(self.tx)
        self._update_version_cache()

    def _maybe_write_blob(self, db_tx : Connection, tx : TransactionMetadata
                          ) -> bool:  # blobs done
        assert self.rest_id is not None
        blob_specs : List[BlobSpec]
        body = tx.body
        if isinstance(body, BlobSpec):
            blob_specs = [body]
        elif isinstance(body, MessageBuilderSpec):
            blob_specs = body.blob_specs
        elif isinstance(body, Blob):
            blob_spec = BlobSpec(blob=body)
            if not isinstance(body, BlobCursor):
                blob_spec.create_tx_body = True
            blob_specs = [blob_spec]
        else:
            raise ValueError()

        blobs : List[BlobCursor] = []

        # filter out blobs we need to create first vs those we can
        # reuse directly
        reuse_uris : List[BlobUri] = []
        for i,blob_spec in enumerate(blob_specs):
            blob = blob_spec.blob
            if isinstance(blob, BlobCursor):
                assert blob.blob_uri is not None
                reuse_uris.append(blob.blob_uri)
                continue

            if blob_spec.reuse_uri:
                reuse_uris.append(body_blob_uri(blob_spec.reuse_uri))
            elif (blob_spec.create_id or blob_spec.create_tx_body or
                  isinstance(blob_spec.blob, Blob)):
                blob_cursor = BlobCursor(self.parent, db_tx = db_tx)
                blob_cursor._create(db_tx)
                if blob_spec.blob:
                    blob_cursor.append_blob(blob_spec.blob)
                if not blob_spec.create_id and not blob_spec.create_tx_body:
                    create_id = str(i)
                blob_rest_id = (TX_BODY if blob_spec.create_tx_body
                                else blob_spec.create_id)
                blob_cursor.blob_uri = BlobUri(
                    self.rest_id, tx_body = (blob_rest_id == TX_BODY),
                    blob = blob_rest_id)
                blob_cursor.update_tx = self.rest_id
                blob_cursor.db_tx = None  # ugh
                blobs.append(blob_cursor)
            else:
                raise ValueError()

        blobs.extend(self._reuse_blob(db_tx, reuse_uris))

        blobrefs = []
        blobs_done = True
        for blob_cursor in blobs:
            assert blob_cursor.blob_uri is not None
            if not blob_cursor.finalized():
                blobs_done = False
            blobrefs.append({ "transaction_id": self.db_id,
                              "tx_rest_id": self.rest_id,
                              "blob_id": blob_cursor.db_id,
                              "rest_id": blob_cursor.blob_uri.blob })

        logging.debug('TransactionCursor._write_blob %d %s all done %s',
                      self.db_id, blobrefs, blobs_done)

        if blobrefs:
            ins = insert(self.parent.tx_blobref_table).values(blobrefs)
            res = db_tx.execute(ins)

        self.blobs = blobs

        if not blobs_done:
            return False
        if isinstance(tx.body, MessageBuilderSpec):
            return True
        assert blobs[0].blob_uri is not None
        assert len(blobs) == 1 and blobs[0].blob_uri.blob == TX_BODY
        return True

    def _write(self,
               db_tx : Connection,
               tx_delta : TransactionMetadata,
               final_attempt_reason : Optional[str] = None,
               finalize_attempt : Optional[bool] = None,
               next_attempt_time : Optional[int] = None,
               notification_done : Optional[bool] = None,
               # only for upcalls from BlobWriter
               input_done = False,
               ping_tx = False):
        assert self.version is not None
        dd = tx_delta.copy_valid(WhichJson.DB)
        logging.debug(dd)
        dd.copy_valid_from(WhichJson.DB_ATTEMPT, tx_delta)
        # XXX body doesn't have DB validity?
        dd.body = tx_delta.body
        tx_delta = dd

        logging.debug('TxCursor._write %s %s %s',
                      self.rest_id, tx_delta,
                      finalize_attempt)
        assert final_attempt_reason != 'oneshot'  # internal-only
        assert not(finalize_attempt and not self.in_attempt)

        if tx_delta.cancelled and (
                self.final_attempt_reason is not None and
                self.final_attempt_reason != 'oneshot'):
            return

        if (self.final_attempt_reason is not None and
            self.final_attempt_reason != 'oneshot' and
            final_attempt_reason is not None):
            logging.error('%s %s', self.final_attempt_reason,
                          final_attempt_reason)
            raise ValueError()
        assert self.tx is not None

        if (tx_delta.empty(WhichJson.ALL) and
            (not tx_delta.body) and
            (final_attempt_reason is None) and
            (notification_done is None) and
            (not input_done) and
            (not finalize_attempt) and
            (not ping_tx)):
            logging.debug(
                'TransactionCursor._write %d %s empty delta %s',
                self.db_id, self.rest_id, tx_delta)
            return True

        tx_to_db = self.tx.merge(tx_delta)
        assert tx_to_db is not None
        tx_to_db_json = tx_to_db.to_json(WhichJson.DB)
        attempt_json = None
        if not tx_delta.empty(WhichJson.DB_ATTEMPT):
            attempt_json = tx_to_db.to_json(WhichJson.DB_ATTEMPT)
        # It doesn't make sense to request reliable notifications for
        # an ephemeral transaction: if you want "attempt once and
        # send a notification" use retry={'max_attempts': 1}
        # Otherwise this will interact badly with the
        # oneshot/no_final_notification workflows.
        assert tx_to_db.retry is not None or tx_to_db.notification is None
        logging.debug(
            'TransactionCursor._write %d %s version=%d '
            'final_attempt_reason=%s %s %s',
            self.db_id, self.rest_id, self.version, final_attempt_reason,
            tx_to_db_json, attempt_json)
        new_version = self.version + 1
        # TODO if in_attempt, validate inflight_session_id
        # the update returning 0 rows -> VerisonConflictException
        # then on the reload, _load() already asserts if
        # inflight_session_id mismatch which should terminate the OH
        # I think the version would surely mismatch in that case but
        # it costs ~nothing to verify
        upd = (update(self.parent.tx_table)
               .where(self.parent.tx_table.c.id == self.db_id,
                      self.parent.tx_table.c.version == self.version)
               .values(json = tx_to_db_json,
                       version = new_version,
                       last_update = self.parent._current_timestamp_epoch())
               .returning(self.parent.tx_table.c.version))

        if attempt_json:
            assert self.attempt_id is not None
            upd_att = (update(self.parent.attempt_table)
                       .where(self.parent.attempt_table.c.transaction_id ==
                              self.db_id,
                              self.parent.attempt_table.c.attempt_id ==
                              self.attempt_id)
                       .values(responses = attempt_json,
                               last_update = self.parent._current_timestamp_epoch()))
            res = db_tx.execute(upd_att)
            assert rowcount(res) == 1

        upd = upd.values(notification = tx_to_db.notification is not None)

        # TODO possibly assert here, the first case is
        # downstream/Exploder, #2/3 are upstream/OutputHandler

        # Exploder upstream transactions are started as oneshot and only if
        # there are mixed responses and it has to store&forward, it
        # enables notifications/retries at the end. If the upstream tx
        # had already finished by this point, we need to clear the
        # oneshot final_attempt_reason.
        if ((tx_to_db.retry is not None or tx_to_db.notification is not None)
             and self.final_attempt_reason == 'oneshot'):
            self.final_attempt_reason = None
            upd = upd.values(final_attempt_reason = None)
        elif final_attempt_reason:
            self.final_attempt_reason = final_attempt_reason
            upd = upd.values(
                no_final_notification = not bool(tx_to_db.notification),
                final_attempt_reason = final_attempt_reason)
        elif notification_done:
            assert tx_to_db.notification is not None
            upd = upd.values(no_final_notification = False)

        if finalize_attempt:
            upd = upd.values(inflight_session_id = None,
                             inflight_session_live = None,
                             next_attempt_time = next_attempt_time)

        if tx_delta.body is not None:
            input_done |= self._maybe_write_blob(db_tx, tx_delta)

        body = tx_delta.body
        if isinstance(body, MessageBuilderSpec):
            upd = upd.values(message_builder = body.json)

        if input_done:
            upd = upd.values(input_done = True)
            self.input_done = True

        res = db_tx.execute(upd)
        row = res.fetchone()
        if row is None or row[0] != new_version:
            logging.info('Storage._write version conflict id=%d '
                         'expected %d db %s', self.db_id, new_version, row)
            raise VersionConflictException()
        self.version = row[0]

        # TODO or RETURNING json
        # XXX doesn't include response fields? tx.merge() should
        # handle that?
        self.tx.merge_from(tx_delta)
        # xxx final_attempt_reason, other cols?
        # self.tx.final_attempt_reason = self.final_attempt_reason

        if finalize_attempt:
            self.in_attempt = False

        logging.info('_write id=%d %s version=%d',
                     self.db_id, self.rest_id, self.version)

        return True

    def load(self, db_id : Optional[int] = None,
             rest_id : Optional[str] = None,
             start_attempt : bool = False) -> Optional[TransactionMetadata]:
        for i in range(0,5):
            try:
                return self._load(db_id, rest_id, start_attempt)
            # _update_version_cache() throws if an update got in
            # between the db read and cache update
            except VersionConflictException:
                logging.debug('VersionConflictException')
                if i == 4:
                    # unexpected to repeatedly conflict but if so,
                    # will leave an open attempt?
                    raise
                backoff(i)
        assert False, 'unreachable'

    def _load(self, db_id : Optional[int] = None,
              rest_id : Optional[str] = None,
              start_attempt : bool = False) -> Optional[TransactionMetadata]:
        assert db_id is None or self.db_id is None or db_id == self.db_id
        assert rest_id is None or self.rest_id is None or rest_id == self.rest_id
        assert not (self.in_attempt and start_attempt)
        if self.db_id is not None or self.rest_id is not None:
            db_id = self.db_id
            rest_id = self.rest_id
        else:
            assert db_id is not None or rest_id is not None
        with self.parent.begin_transaction() as db_tx:
            rv = self._load_and_start_attempt_db(
                db_tx, db_id, rest_id, start_attempt)
        if rv:
            self._update_version_cache()
        return rv

    def _load_and_start_attempt_db(
            self, db_tx : Connection,
            db_id : Optional[int] = None,
            rest_id : Optional[str] = None,
            start_attempt : bool = False) -> Optional[TransactionMetadata]:
        tx = self._load_db(db_tx, db_id, rest_id, start_attempt)
        if self.db_id is None:
            return None
        assert self.version is not None
        if tx is None:
            return None
        if start_attempt:
            self._start_attempt_db(db_tx, self.db_id, self.version)
        return tx

    def _load_db(self, db_tx : Connection,
                 db_id : Optional[int] = None,
                 rest_id : Optional[str] = None,
                 start_attempt : bool = False
                 ) -> Optional[TransactionMetadata]:
        where = None
        where_id = None

        sel = select(self.parent.tx_table.c.id,
                     self.parent.tx_table.c.rest_id,
                     self.parent.tx_table.c.creation,
                     self.parent.tx_table.c.json,
                     self.parent.tx_table.c.version,
                     self.parent.tx_table.c.input_done,
                     self.parent.tx_table.c.final_attempt_reason,
                     self.parent.tx_table.c.message_builder,
                     self.parent.tx_table.c.no_final_notification,
                     self.parent.tx_table.c.inflight_session_id,
                     self.parent.tx_table.c.inflight_session_live)

        # TODO WHERE version != self.version ?

        if start_attempt and not self.created:
            sel = sel.where(
                or_(self.parent.tx_table.c.inflight_session_id.is_(None),
                    not_(self.parent.tx_table.c.inflight_session_live)))

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

        if self.db_id is not None:
            assert row[0] == self.db_id
        if self.rest_id is not None:
            assert row[1] == self.rest_id

        (self.db_id,
         self.rest_id,
         self.creation,
         trans_json,
         self.version,
         self.input_done,
         self.final_attempt_reason,
         self.message_builder,
         self.no_final_notification,
         session_id,
         session_live) = row

        self.tx = TransactionMetadata.from_json(trans_json, WhichJson.DB)
        if self.tx is None:
            return None
        # XXX causes conflicts
        if self.final_attempt_reason != 'oneshot':
            self.tx.final_attempt_reason = self.final_attempt_reason

        # TODO save finalized body above, skip load and restore here
        self._load_blobs(db_tx)

        self.tx.tx_db_id = self.db_id
        assert self.tx.rest_id is None or (self.tx.rest_id == self.rest_id)
        self.tx.rest_id = self.rest_id

        # TODO cross-join this with the tx read to get it all in one round-trip?

        # XXX if in_attempt, query on attempt_id?? (cf assert below)
        sel = (select(self.parent.attempt_table.c.attempt_id,
                      self.parent.attempt_table.c.responses)
               .where(self.parent.attempt_table.c.transaction_id == self.db_id)
               .order_by(self.parent.attempt_table.c.attempt_id.desc())
               .limit(1))
        res = db_tx.execute(sel)
        row = res.fetchone()
        resp_json = None
        attempt_id = None
        if row is not None:
            attempt_id = row[0]
            assert self.tx is not None  # set above
            self.tx.attempt_count = attempt_id
            resp_json = row[1]
            if resp_json is not None:
                responses = TransactionMetadata.from_json(
                    resp_json, WhichJson.DB_ATTEMPT)
                assert self.tx.merge_from(responses)

        logging.debug('TransactionCursor._load_db %d %s version=%d tx %s '
                      'attempt %s %s',
                      self.db_id, self.rest_id, self.version,
                      trans_json, attempt_id, resp_json)

        if self.in_attempt:
            # TODO this should maybe be a new
            # storage_schema.InvalidSessionException
            assert attempt_id == self.attempt_id

        if session_live and session_id != self.parent.session_id:
            sel = select(self.parent.session_table.c.uri).where(
                self.parent.session_table.c.id == session_id)
            res = db_tx.execute(sel)
            row = res.fetchone()
            assert row is not None
            self.session_uri = row[0]
            self.tx.session_uri = self.session_uri

        return self.tx

    def _load_blobs(self, db_tx):
        blobref_cols = self.parent.tx_blobref_table.c
        sel_blobrefs = select(
            blobref_cols.blob_id,
            blobref_cols.rest_id).where(
                blobref_cols.transaction_id == self.db_id).subquery()
        j = join(sel_blobrefs, self.parent.blob_table,
                 sel_blobrefs.c.blob_id == self.parent.blob_table.c.id)
        sel_blob = select(
            sel_blobrefs.c.rest_id,
            self.parent.blob_table.c.id,
            self.parent.blob_table.c.length,
            self.parent.blob_table.c.last_update,
            func.length(self.parent.blob_table.c.content)
        ).order_by(self.parent.blob_table.c.id).select_from(j)

        res = db_tx.execute(sel_blob)
        blobs = []
        for row in res:
            blob_rest_id, blob_id, content_length, last_update, length = row
            blob = BlobCursor(self.parent)
            blob.init(self.rest_id, blob_id, blob_rest_id,
                      content_length, last_update, length)
            blobs.append(blob)
        self.blobs = blobs
        if len(blobs) == 1 and blobs[0].blob_uri.tx_body:
            self.tx.body = blobs[0]
        elif self.message_builder:
            message_builder = MessageBuilderSpec(self.message_builder, blobs)
            message_builder.check_ids()
            self.tx.body = message_builder
        elif blobs:
            raise ValueError()

    def _start_attempt_db(self,
                          db_tx : Connection, db_id : int, version : int):
        logging.debug('TxCursor._start_attempt_db %d', db_id)
        assert self.parent.session_id is not None
        assert self.tx is not None
        new_version = version + 1

        upd = (update(self.parent.tx_table)
               .where(self.parent.tx_table.c.id == db_id,
                      self.parent.tx_table.c.version == version)
               .values(version = new_version,
                       inflight_session_id = self.parent.session_id,
                       inflight_session_live = True,
                       last_update = self.parent._current_timestamp_epoch())
               .returning(self.parent.tx_table.c.version))

        if self.created:
            upd = upd.where(self.parent.tx_table.c.inflight_session_id ==
                            self.parent.session_id)
        else:
            upd = upd.where(
                self.parent.tx_table.c.inflight_session_id.is_(None))

        # tx without retries enabled can only be loaded once
        if self.tx.retry is None:
            self.final_attempt_reason = "oneshot"
            upd = upd.values(final_attempt_reason = self.final_attempt_reason)

        res = db_tx.execute(upd)
        row = res.fetchone()

        # Without READ_CONSISTENT a downstream write can get in
        # between here and cause a version mismatch
        if row is None:
            raise VersionConflictException()

        # This is a sort of pseudo-attempt for re-entering the
        # notification logic if it was enabled after the transaction
        # reached a final result.
        if not self.no_final_notification:
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
                           attempt_id = new_attempt_id,
                           creation=self.parent._current_timestamp_epoch(),
                           last_update=self.parent._current_timestamp_epoch())
                   .returning(self.parent.attempt_table.c.attempt_id))
            res = db_tx.execute(ins)
            assert (row := res.fetchone())
            self.attempt_id = row[0]
        self._load_db(db_tx, db_id=db_id)
        self.in_attempt = True

    def try_cache(self):
        if self.id_version is None:
            return None
        return self.id_version.wait(0, 0, self)

    def wait(self, timeout : Optional[float] = None, clone = False
             ) -> Tuple[bool, bool]:
        assert self.id_version is not None
        assert self.version is not None
        return self.id_version.wait(
            self.version, timeout, self if clone else None)

    async def wait_async(self, timeout : Optional[float], clone = False
                         ) -> Tuple[bool, bool]:
        assert self.id_version is not None
        assert self.version is not None
        return await self.id_version.wait_async(
            self.version, timeout, self if clone else None)

    # returns True if all blobs ref'd from this tx are finalized
    def check_input_done(self, db_tx : Connection) -> bool:
        j = join(self.parent.blob_table, self.parent.tx_blobref_table,
             self.parent.blob_table.c.id == self.parent.tx_blobref_table.c.blob_id,
             isouter=False)
        sel = (select(self.parent.blob_table.c.id).select_from(j)
               .where(self.parent.tx_blobref_table.c.transaction_id == self.db_id,
                      or_(self.parent.blob_table.c.length.is_(None),
                          func.length(self.parent.blob_table.c.content) !=
                          self.parent.blob_table.c.length))
               .limit(1))
        res = db_tx.execute(sel)
        row = res.fetchone()
        return row is None

    def get_blob_for_append(self, blob_uri : BlobUri) -> Optional[WritableBlob]:
        blob_uri = body_blob_uri(blob_uri)
        if not self.blobs:
            return None
        for b in self.blobs:
            if b.blob_uri == blob_uri:
                return b
        return None

class BlobCursor(Blob, WritableBlob):
    db_id = None  # Blob.id
    length : int = 0  # max offset+len from BlobContent, next offset to write
    _content_length : Optional[int] = None  # overall length from content-range
    last = False
    update_tx : Optional[str] = None
    blob_uri : Optional[BlobUri] = None
    _session_uri : Optional[str] = None
    db_tx : Optional[Connection] = None
    last_update : Optional[int] = None

    def __init__(self, storage,
                 update_tx : Optional[str] = None,
                 finalize_tx : Optional[bool] = False,
                 db_tx : Optional[Connection] = None):
        self.parent = storage
        self.update_tx = update_tx
        self.db_tx = db_tx

    def __hash__(self):
        return hash(self.db_id)

    def clone(self):
        return copy.copy(self)

    def delta(self, rhs):
        if not isinstance(rhs, BlobCursor):
            return None
        if self.db_id != rhs.db_id:
            return None
        if self.length > rhs.length:
            return None
        if self._content_length is not None and self._content_length != rhs._content_length:
            return None
        return self.length < rhs.length

    def init(self, tx_rest_id : str, blob_id : int, blob_rest_id : str,
             content_length : Optional[int], last_update : int, length : int):
        self.db_id = blob_id
        self._content_length = content_length
        self.length = length
        self.last_update = last_update
        self.last = (self.length == self._content_length)
        self.blob_uri = BlobUri(
            tx_rest_id, tx_body=(blob_rest_id == TX_BODY), blob=blob_rest_id)
        self.update_tx = tx_rest_id

    def __eq__(self, x):
        if not isinstance(x, BlobCursor):
            return False
        return self.db_id == x.db_id

    def rest_id(self) -> Optional[str]:
        if self.blob_uri is None:
            return None
        if self.blob_uri.blob == TX_BODY:
            return None
        return self.blob_uri.blob

    def len(self):
        return self.length
    def content_length(self):
        return self._content_length

    def session_uri(self) -> Optional[str]:
        return self._session_uri

    def _create(self, db_tx : Connection):
        ins = insert(self.parent.blob_table).values(
            creation=self.parent._current_timestamp_epoch(),
            last_update=self.parent._current_timestamp_epoch(),
            content=bytes()
        ).returning(self.parent.blob_table.c.id)

        res = db_tx.execute(ins)
        row = res.fetchone()
        assert row is not None
        self.db_id = row[0]
        return self.db_id

    # WritableBlob
    def append_data(self, offset: int, d : bytes,
                    content_length : Optional[int] = None,
                    # last: set content_length to offset + len(d)
                    last : Optional[bool] = None
                    ) -> Tuple[bool, int, Optional[int]]:
        logging.info('BlobWriter.append_data %d [%s] offset=%d self.length=%d d.len=%d '
                     'content_length=%s new content_length=%s',
                     self.db_id, self.blob_uri, offset, self.length, len(d),
                     self._content_length, content_length)

        if last:
            content_length = offset + len(d)

        assert content_length is None or (
            content_length >= (offset + len(d)))

        tx_version = None
        cursor = None
        with (nullcontext(self.db_tx) if self.db_tx is not None
              else self.parent.begin_transaction() as db_tx):
            # TODO is this extra read redundant with the WHERE
            # conditions on the update?
            stmt = select(
                func.length(self.parent.blob_table.c.content),
                self.parent.blob_table.c.length,
                self.parent.blob_table.c.last_update,
                self.parent._current_timestamp_epoch()).where(
                    self.parent.blob_table.c.id == self.db_id)
            res = db_tx.execute(stmt)
            row = res.fetchone()
            assert row is not None
            logging.debug(row)
            db_length, db_content_length, last_update, db_now = row
            self.length = db_length
            self._content_length = db_content_length
            self.last_update = last_update
            if offset != db_length or self.length != db_length:
                return False, db_length, db_content_length
            if (db_content_length is not None) and (
                    (content_length is None) or
                    (content_length != db_content_length)):
                return False, db_length, db_content_length

            upd = (update(self.parent.blob_table)
                   .where(self.parent.blob_table.c.id == self.db_id)
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
                           last_update = self.parent._current_timestamp_epoch())
                   .returning(func.length(self.parent.blob_table.c.content)))

            res = db_tx.execute(upd)
            row = res.fetchone()
            # we should have early-returned after the select if the offset
            # didn't match, etc.
            assert row is not None
            logging.debug('append_data %d %d %d', row[0], self.length, len(d))
            assert row[0] == (self.length + len(d))
            blob_done = content_length is not None and (
                row[0] == content_length)

            self.length = row[0]
            self._content_length = content_length
            self.last = (self.length == self._content_length)
            logging.debug('append_data %d %s last=%s',
                          self.length, self._content_length, self.last)

        stale = (db_now - last_update) > self.parent.blob_tx_refresh_interval
        if self.update_tx is not None:
            if stale or blob_done:
                cursor = self.parent.get_transaction_cursor()
                for i in range(0,5):
                    try:
                        # TODO should this be in the same db_tx as the
                        # blob write? as it is, possible for a reader to
                        # see finalized body but input_done == False
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
                        logging.debug('VersionConflictException')
                        if i == 4:
                            raise
                        backoff(i)
            else:
                cursor = self.parent.get_transaction_cursor(rest_id=self.update_tx)
                logging.debug(self)
                if cursor.try_cache() and cursor.blobs:
                    for i,blob in enumerate(cursor.blobs):
                        if blob.db_id != self.db_id:
                            continue
                        logging.debug(blob)
                        cursor.blobs[i] = self.clone()
                        break
                else:
                    cursor = None


        if cursor is not None:
            cursor._update_version_cache()
            self._session_uri = cursor.session_uri
        return True, self.length, self._content_length


    def pread(self, offset, length=None) -> Optional[bytes]:
        # TODO this should maybe have the same effect as load() if the
        # blob isn't finalized?
        l = length if length else self.length - offset
        stmt = (
            select(func.substr(self.parent.blob_table.c.content, offset+1, l))
            .where(self.parent.blob_table.c.id == self.db_id))
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

    def __repr__(self):
        return 'BlobCursor id=%d uri=%s length=%d content_length=%s' % (self.db_id, str(self.blob_uri), self.length, self._content_length)

class Storage():
    session_id : Optional[int] = None
    tx_versions : IdVersionMap
    engine : Optional[Engine] = None

    session_table : Optional[Table] = None
    blob_table : Optional[Table] = None
    tx_table : Optional[Table] = None
    tx_blobref_table : Optional[Table] = None
    attempt_table : Optional[Table] = None
    session_uri : Optional[str] = None
    blob_tx_refresh_interval : int

    def __init__(self, version_cache : Optional[IdVersionMap] = None,
                 engine : Optional[Engine] = None,
                 session_uri : Optional[str] = None,
                 blob_tx_refresh_interval : int = 10):
        if version_cache is not None:
            self.tx_versions = version_cache
        else:
            self.tx_versions = IdVersionMap()
        self.engine = engine
        self.session_uri = session_uri
        self.blob_tx_refresh_interval = blob_tx_refresh_interval

    @staticmethod
    def _sqlite_pragma(dbapi_conn, con_record):
        # isn't sticky from schema so set it again here
        dbapi_conn.execute("PRAGMA journal_mode=WAL")
        dbapi_conn.execute("PRAGMA foreign_keys=ON")
        # https://www.sqlite.org/pragma.html#pragma_synchronous
        # FULL=2, flush WAL on every write,
        # NORMAL=1 not durable after power loss
        dbapi_conn.execute("PRAGMA synchronous=2")
        dbapi_conn.execute("PRAGMA auto_vacuum=2")

    @staticmethod
    def connect(url, session_uri, blob_tx_refresh_interval : int = 10):
        engine = create_engine(url)
        if 'sqlite' in url:
            event.listen(engine, 'connect', Storage._sqlite_pragma)

        s = Storage(engine=engine, session_uri=session_uri,
                    blob_tx_refresh_interval=blob_tx_refresh_interval)
        s._init_session()
        return s

    def __del__(self):
        self._del_session()

    def begin_transaction(self):
        assert self.engine is not None
        return self.engine.begin()

    def _del_session(self):
        if self.session_id is None:
            return
        try:
            with self.begin_transaction() as db_tx:
                upd = update(self.session_table).values(
                    live = False,
                    last_update=self._current_timestamp_epoch()
                ).where(self.session_table.c.id == self.session_id
                ).returning(self.session_table.c.id)
                res = db_tx.execute(upd)
                row = res.fetchone()
                logging.info('Storage._del_session deleted session %d',
                             self.session_id)
                self.session_id = None
        except:
            logging.exception('Storage._del_session failed to delete session')

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
            ins = (insert(self.session_table).values(
                creation = self._current_timestamp_epoch(),
                last_update = self._current_timestamp_epoch(),
                live = True,
                uri = self.session_uri)
                   .returning(self.session_table.c.id))
            res = db_tx.execute(ins)
            self.session_id = res.fetchone()[0]

        atexit.register(self._del_session)

    def _current_timestamp_epoch(self):
        assert self.engine is not None
        if str(self.engine.url).find('sqlite') != -1:
            return select(func.unixepoch(func.current_timestamp())
                          ).scalar_subquery()
        elif str(self.engine.url).find('postgres') != -1:
            return select(func.extract('epoch', func.current_timestamp())
                          ).scalar_subquery()
        raise NotImplementedError()

    def _refresh_session(self):
        with self.begin_transaction() as db_tx:

            upd = update(self.session_table).values(
                last_update = self._current_timestamp_epoch()).where(
                    self.session_table.c.id == self.session_id,
                    self.session_table.c.live).returning(
                        self.session_table.c.live)
            res = db_tx.execute(upd)
            if not res:
                return False
            row = res.fetchone()
            if not row:
                return False
            if not row[0]:
                return False
            return True

    def testonly_get_session(self, session_id) -> dict:
        assert self.session_table is not None
        with self.begin_transaction() as db_tx:
            sel = select('*').where(self.session_table.c.id == session_id)
            res = db_tx.execute(sel)
            row = res.fetchone()
            return row._mapping


    def _gc_session(self, ttl : timedelta) -> Optional[int]:
        assert self.session_table is not None
        with self.begin_transaction() as db_tx:
            upd = (update(self.session_table).values(
                live = False,
                last_update = self._current_timestamp_epoch()
            ).where(
                (self._current_timestamp_epoch() -
                 self.session_table.c.last_update) > ttl.total_seconds(),
                self.session_table.c.live.is_(True)
            ).returning(self.session_table.c.id,
                        self.session_table.c.last_update))

            # TODO should delete old sessions eventually, maybe after
            # all tx/blob created in that session have been gc'd?

            res = db_tx.execute(upd)
            if not res:
                return None
            rows = 0
            for row in res:
                logging.debug('_gc_session id %d last update %s',
                              row[0], row[1])
                rows += 1

        return rows

    def recover(self, session_ttl=timedelta(seconds=1)) -> Optional[int]:
        return self._gc_session(session_ttl)

    def get_transaction_cursor(self, db_id : Optional[int] = None,
                               rest_id : Optional[str] = None
                               ) -> TransactionCursor:
        return TransactionCursor(self, db_id, rest_id)

    def load_one(self) -> Optional[TransactionCursor]:
        try:
            return self._load_one()
        except VersionConflictException:
            logging.debug('VersionConflictException')
            return None

    def _load_one(self) -> Optional[TransactionCursor]:
        assert self.tx_table is not None
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

            sel = (select(self.tx_table.c.id,
                          self.tx_table.c.version)
                   .where(self.tx_table.c.inflight_session_id.is_(None),
                          or_(
                              and_(
                                  or_(self.tx_table.c.input_done.is_(True),
                                      self.tx_table.c.creation_session_id ==
                                      self.session_id),
                                  or_(self.tx_table.c.next_attempt_time.is_(None),
                                      self.tx_table.c.next_attempt_time <= self._current_timestamp_epoch()),
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
            db_id = row[0]
            version = row[1]

            cursor = self.get_transaction_cursor()
            cursor._load_and_start_attempt_db(
                db_tx, db_id=db_id, start_attempt=True)

            # TODO: if the last n consecutive attempts weren't
            # finalized, this transaction may be crashing the system
            # -> quarantine

        cursor._update_version_cache()
        return cursor

    def _gc(self, db_tx : Connection, ttl : timedelta) -> Tuple[int, int]:
        assert self.tx_table is not None
        assert self.tx_blobref_table is not None
        assert self.blob_table is not None

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
            (self._current_timestamp_epoch() - self.tx_table.c.last_update) >
            ttl.total_seconds())
        res = db_tx.execute(del_tx)
        deleted_tx = rowcount(res)

        # If you wanted to reuse a blob for longer than the ttl
        # (unlikely?), you might need tx creation reusing the
        # blob to bump the blob last_update so it stays live?
        del_blob = delete(self.blob_table).where(
            ~exists().where(
                self.tx_blobref_table.c.blob_id == self.blob_table.c.id))

        res = db_tx.execute(del_blob)
        deleted_blob = rowcount(res)
        return deleted_tx, deleted_blob

    def gc(self, ttl : timedelta) -> Tuple[int, int]:
        with self.begin_transaction() as db_tx:
            deleted = self._gc(db_tx, ttl)
            return deleted


    def debug_dump(self):
        out = ''
        with self.begin_transaction() as db_tx:
            for table in [ self.session_table,
                           self.blob_table,
                           self.tx_table,
                           self.tx_blobref_table,
                           self.attempt_table ]:
                out += str(table) + '\n'
                sel = table.select()
                res = db_tx.execute(sel)
                for row in res:
                    out += str(row._mapping)
                    out += '\n'
        return out
