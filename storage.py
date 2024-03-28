from typing import Callable, Optional, Dict, List, Tuple
import sqlite3
import json
import time
import logging
from threading import Lock, Condition
from hashlib import sha256
from base64 import b64encode
from functools import reduce

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool
from sqlalchemy import LargeBinary, MetaData, String, Table, cast, case as sa_case, column, delete, event, func, insert, join, literal, or_, select, true as sa_true, update, union_all, values

import psutil

from blob import Blob, InlineBlob
from response import Response
from storage_schema import InvalidActionException, VersionConflictException
from filter import TransactionMetadata, WhichJson


class TransactionCursor:
    id : Optional[int] = None
    rest_id : Optional[str] = None
    attempt_id : Optional[int] = None
    max_attempts : Optional[int] = None

    version : Optional[int] = None

    creation : Optional[int] = None
    input_done : Optional[bool] = None
    output_done : Optional[bool] = None

    tx : Optional[TransactionMetadata] = None

    message_builder : Optional[dict] = None  # json

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
               rest_id,
               tx : TransactionMetadata,
               reuse_blob_rest_id : List[str] = []):
        parent = self.parent
        self.creation = int(time.time())
        with self.parent.conn() as conn:
            # xxx dead?
            self.last = False
            self.version = 0

            max_attempts = tx.max_attempts if tx.max_attempts else 1
            db_json = tx.to_json(WhichJson.DB)
            logging.debug('TxCursor.create %s %s', rest_id, db_json)
            ins = insert(self.parent.tx_table).values(
                rest_id = rest_id,
                creation = self.creation,
                last_update = self.creation,
                version = self.version,
                last = self.last,
                json = db_json,
                attempt_count = 0,
                max_attempts = max_attempts,
            ).returning(self.parent.tx_table.c.id)

            res = conn.execute(ins)
            row = res.fetchone()
            self.id = row[0]
            self.tx = TransactionMetadata()  # tx

            self._write(conn, tx, reuse_blob_rest_id)

            conn.commit()

        with parent.created_lock:
            logging.debug('TransactionCursor.create id %d', self.id)
            if parent.created_id is None or self.id > parent.created_id:
                parent.created_id = self.id
                parent.created_cv.notify_all()

    def _reuse_blob(self, conn, rest_id : List[str]
                    ) -> List[Tuple[int, str, bool]]:
                    # -> id, rest_id, input_done
        if not rest_id:
            return []
        assert isinstance(rest_id, list)

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
                [(x,) for x in rest_id])).cte()
        else:
            # OTOH chaining up UNION ALL works fine and we don't expect it
            # to be so much worse as to make a difference anywhere you
            # would actually use sqlite!
            literals = [select(literal(x).label('rest_id')) for x in rest_id]
            val = reduce(lambda x,y: x.union_all(y),
                         literals[1:], literals[0].cte())

        sel = select(self.parent.blob_table.c.id,
                     self.parent.blob_table.c.rest_id,
                     func.length(self.parent.blob_table.c.content),
                     self.parent.blob_table.c.length
                     ).join_from(self.parent.blob_table, val,
                                 self.parent.blob_table.c.rest_id ==
                                 val.c.rest_id)

        res = conn.execute(sel)
        ids = {}
        for row in res:
               blob_id, rid, length, content_length = row

               done = (length == content_length)
               ids[rid] = (blob_id, done)

        out = []
        for rid in rest_id:
            if rid not in ids:
                raise ValueError()  # invalid rest id
            blob_id, done = ids[rid]
            out.append((blob_id, rid, done))

        return out

    def write_envelope(self,
                       tx_delta : TransactionMetadata,
                       reuse_blob_rest_id : List[str] = []):
        with self.parent.conn() as conn:
            self._write(conn, tx_delta, reuse_blob_rest_id)
            conn.commit()
            self.parent.tx_versions.update(self.id, self.version)

    def _write_blob(self,
                    conn,
                    tx,
                    upd,  # sa update
                    reuse_blob_rest_id : List[str] = []):
        reuse_blob_id = self._reuse_blob(conn, reuse_blob_rest_id)
        if not reuse_blob_id:
            return upd

        input_done = True
        if reuse_blob_id:
            for blob_id, rest_id, done in reuse_blob_id:
                if not done:
                    # TODO this can be precondition now: don't PATCH the
                    # blob id in until it's finalized
                    input_done = False
                    break

        logging.debug('TransactionCursor._write_blob %d %s',
                      self.id, input_done)

        logging.debug('TransactionCursor._write_blob %d body %s',
                      self.id, tx.body)
        if tx.body:
            if reuse_blob_rest_id:
                logging.debug('TransactionCursor._write_blob %d reuse', self.id)
                upd = upd.values(
                    body_blob_id = reuse_blob_id[0][0],
                    body_rest_id = reuse_blob_rest_id[0])
            else:
                raise ValueError()

        upd = upd.values(input_done = input_done)

        reused_blob_ids = [y[0] for y in reuse_blob_id]
        blobrefs = [{"transaction_id": self.id, "blob_id": blob_id}
                    for blob_id in reused_blob_ids]
        logging.debug('TransactionCursor._write_blob %d %s', self.id, blobrefs)

        ins = insert(self.parent.tx_blob_table).values(blobrefs)
        res = conn.execute(ins)

        self.input_done = input_done
        return upd

    def _write(self,
               conn,
               tx_delta : TransactionMetadata,
               reuse_blob_rest_id : List[str] = []):
        assert self.tx is not None
        tx_to_db = self.tx.merge(tx_delta)
        # e.g. overwriting an existing field
        # xxx return/throw
        assert tx_to_db is not None
        logging.debug('write_envelope %s', tx_to_db.to_json(WhichJson.DB))
        new_version = self.version + 1
        # TODO all updates '... WHERE inflight_session_id =
        #   self.parent.session' ?
        now = int(time.time())
        upd = (update(self.parent.tx_table)
               .where(self.parent.tx_table.c.id == self.id)
               .where(self.parent.tx_table.c.version == self.version)
               .values(json = tx_to_db.to_json(WhichJson.DB),
                       version = new_version,
                       attempt_count = 0,
                       last_update = now)
               .returning(self.parent.tx_table.c.version))

        if tx_delta.max_attempts is not None:
            upd = upd.values(max_attempts = tx_delta.max_attempts)

        if tx_delta.message_builder:
            upd = upd.values(message_builder = tx_delta.message_builder)

        upd = self._write_blob(conn, tx_delta, upd, reuse_blob_rest_id)

        res = conn.execute(upd)
        row = res.fetchone()

        if row is None or row[0] != new_version:
            raise VersionConflictException()
        self.version = row[0]

        # TODO or RETURNING json
        # XXX doesn't include response fields? tx.merge() should
        # handle that?
        self.tx = self.tx.merge(tx_delta)

        logging.info('write_envelope id=%d version=%d',
                     self.id, self.version)

        return True

    def _set_response(self, col : str, response : Response):
        assert(self.attempt_id is not None)
        with self.parent.conn() as conn:
            upd_att = (update(self.parent.attempt_table)
                   .where(self.parent.attempt_table.c.transaction_id == self.id,
                          self.parent.attempt_table.c.attempt_id ==
                          self.attempt_id)
                   .values({col: response.to_json()}))
            res = conn.execute(upd_att)

            new_version = self.version + 1
            upd_tx = (update(self.parent.tx_table)
                      .where(self.parent.tx_table.c.id == self.id,
                             self.parent.tx_table.c.version == self.version)
                      .values(version = new_version)
                      .returning(self.parent.tx_table.c.version))
            res = conn.execute(upd_tx)
            if (row := res.fetchone()) is None or row[0] != new_version:
                raise VersionConflictException()
            conn.commit()
            self.version = row[0]
            self.parent.tx_versions.update(self.id, new_version)


    def set_mail_response(self, response : Response):
        # XXX enforce that req field is populated
        self._set_response('mail_response', response)

    def add_rcpt_response(self, response : List[Response]):
        assert(self.attempt_id is not None)
        with self.parent.conn() as conn:
            # TODO load-that's-not-a-load, cf write_envelope()
            sel = (select(self.parent.tx_table.c.version,
                          self.parent.tx_table.c.json)
                   .where(self.parent.tx_table.c.id == self.id))
            res = conn.execute(sel)
            db_version,tx_json = res.fetchone()
            if db_version != self.version:
                raise VersionConflictException()

            sel = (select(self.parent.attempt_table.c.rcpt_response)
                   .where(self.parent.attempt_table.c.transaction_id == self.id,
                          self.parent.attempt_table.c.attempt_id ==
                          self.attempt_id))
            res = conn.execute(sel)

            old_resp = []
            row = res.fetchone()
            if row and row[0]:
                old_resp = row[0]

            old_resp.extend([r.to_json() for r in response])
            if len(old_resp) > len(tx_json['rcpt_to']):
                logging.critical(
                    'add_rcpt_response %d %s %s %s',
                    self.id, self.rest_id, old_resp, tx_json['rcpt_to'])
                assert False

            upd = (update(self.parent.attempt_table)
                   .where(self.parent.attempt_table.c.transaction_id == self.id,
                          self.parent.attempt_table.c.attempt_id ==
                          self.attempt_id)
                   .values(rcpt_response = old_resp))
            res = conn.execute(upd)

            new_version = self.version + 1
            upd = (update(self.parent.tx_table)
                   .where(self.parent.tx_table.c.id == self.id,
                          self.parent.tx_table.c.version == self.version)
                   .values(version = new_version)
                   .returning(self.parent.tx_table.c.version))
            res = conn.execute(upd)
            if (row := res.fetchone()) is None or row[0] != new_version:
                raise VersionConflictException()
            self.version = row[0]
            conn.commit()
            self.parent.tx_versions.update(self.id, new_version)

    def set_data_response(self, response : Response):
        self._set_response('data_response', response)

    def load(self, db_id : Optional[int] = None,
             rest_id : Optional[str] = None) -> Optional[TransactionMetadata]:
        if self.id is not None:
            assert(db_id is None and rest_id is None)
            db_id = self.id
        assert(db_id is not None or rest_id is not None)
        with self.parent.conn() as conn:
            #cursor = conn.connection.cursor()
            return self._load_db(conn, db_id, rest_id)

    def _load_db(self, conn,
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
                     self.parent.tx_table.c.output_done,
                     self.parent.tx_table.c.body_rest_id,
                     self.parent.tx_table.c.max_attempts,
                     self.parent.tx_table.c.message_builder)


        if db_id is not None:
            sel = sel.where(self.parent.tx_table.c.id == db_id)
        elif rest_id is not None:
            sel = sel.where(self.parent.tx_table.c.rest_id == rest_id)
        res = conn.execute(sel)
        row = res.fetchone()
        if not row:
            return None

        (self.id, self.rest_id,
         self.creation, trans_json, self.version, self.last,
         self.input_done, self.output_done, self.body_rest_id,
         self.max_attempts, self.message_builder) = row

        logging.debug('TransactionCursor._load_db %s %s %s',
                      self.rest_id, row, trans_json)

        self.tx = TransactionMetadata.from_json(trans_json, WhichJson.DB)
        self.tx.body = self.body_rest_id
        self.tx.message_builder = self.message_builder

        sel = (select(self.parent.attempt_table.c.attempt_id,
                     self.parent.attempt_table.c.mail_response,
                     self.parent.attempt_table.c.rcpt_response,
                     self.parent.attempt_table.c.data_response)
               .where(self.parent.attempt_table.c.transaction_id == self.id)
               .order_by(self.parent.attempt_table.c.attempt_id.desc())
               .limit(1))
        res = conn.execute(sel)
        row = res.fetchone()
        if row is not None:
            self.attempt_id,mail_json,rcpt_json,data_json = row
            if mail_json:
                self.tx.mail_response = Response.from_json(mail_json)
            if rcpt_json:
                self.tx.rcpt_response = [
                    Response.from_json(r) for r in rcpt_json]
            if data_json:
                self.tx.data_response = Response.from_json(data_json)

        return self.tx

    def _start_attempt_db(self, conn, db_id, version):
        assert self.parent.session_id is not None
        new_version = version + 1
        upd = (update(self.parent.tx_table)
               .where(self.parent.tx_table.c.id == db_id,
                      self.parent.tx_table.c.version == version)
               .values(version = new_version,
                       inflight_session_id = self.parent.session_id)
               .returning(self.parent.tx_table.c.version))
        res = conn.execute(upd)
        assert res.fetchone()[0] == new_version

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
        res = conn.execute(ins)

        assert (row := res.fetchone())
        self.attempt_id = row[0]
        self._load_db(conn, db_id=db_id)

    def finalize_attempt(self, output_done):
        with self.parent.conn() as conn:
            self._finalize_attempt(conn, output_done)
            conn.commit()

    def _finalize_attempt(self, conn, output_done):
        new_version = self.version + 1
        now = int(time.time())
        upd = (update(self.parent.tx_table)
               .where(self.parent.tx_table.c.id == self.id,
                      self.parent.tx_table.c.version == self.version)
               .values(inflight_session_id = None,
                       output_done = output_done,
                       last_update = now,
                       version = new_version,
                       attempt_count = self.attempt_id)
               .returning(self.parent.tx_table.c.version))
        res = conn.execute(upd)

        if (row := res.fetchone()) is None or row[0] != new_version:
            raise VersionConflictException()
        self.version = new_version
        self.parent.tx_versions.update(self.id, self.version)

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

    def _abort(self, conn):
        logging.info('TransactionCursor.abort %d %s', self.id, self.rest_id)
        new_version = self.version + 1
        now = int(time.time())
        upd = (update(self.parent.tx_table)
               .where(self.parent.tx_table.c.id == self.id,
                      self.parent.tx_table.c.version == self.version)
               .values(max_attempts = 0,
                       last_update = now,
                       version = new_version)
               .returning(self.parent.tx_table.c.version))
        res = conn.execute(upd)
        if (row := res.fetchone()) is None or row[0] != new_version:
            raise VersionConflictException()
        self.version = new_version
        self.parent.tx_versions.update(self.id, self.version)

    def abort(self):
        with self.parent.conn() as conn:
            return self._abort(conn)


class BlobWriter:
    id = None
    length = 0  # max offset+len from BlobContent, next offset to write
    content_length = None  # overall length from content-range
    rest_id = None
    last = False

    def __init__(self, storage):
        self.parent = storage

    def _create(self, rest_id, conn):
        ins = insert(self.parent.blob_table).values(
            last_update=int(time.time()),
            rest_id=rest_id,
            content=bytes()
        ).returning(self.parent.blob_table.c.id)

        res = conn.execute(ins)
        row = res.fetchone()

        self.id = row[0]
        self.rest_id = rest_id
        return self.id

    def create(self, rest_id):
        with self.parent.conn() as conn:
            self._create(rest_id, conn)
            conn.commit()
            return self.id

    def load(self, rest_id):
        stmt = select(
            self.parent.blob_table.c.id,
            self.parent.blob_table.c.length,
            self.parent.blob_table.c.last_update,
            func.length(self.parent.blob_table.c.content)).where(
                self.parent.blob_table.c.rest_id == rest_id)

        with self.parent.conn() as conn:
            res = conn.execute(stmt)
            row = res.fetchone()

        if not row:
            return None
        self.id, self.content_length, self.last_update, self.length = row
        self.rest_id = rest_id
        self.last = (self.length == self.content_length)

        return self.id

    # TODO this should probably just take Blob and share the data
    # under the hood if isinstance(blob, BlobReader), etc
    def append_data(self, d : bytes, content_length=None):
        logging.info('BlobWriter.append_data %d %s length=%d d.len=%d '
                     'content_length=%s new content_length=%s',
                     self.id, self.rest_id, self.length, len(d),
                     self.content_length, content_length)
        assert self.content_length is None or (
            self.content_length == content_length)
        assert not self.last

        with self.parent.conn() as conn:
            stmt = select(
                func.length(self.parent.blob_table.c.content)).where(
                    self.parent.blob_table.c.id == self.id)
            res = conn.execute(stmt)
            row = res.fetchone()
            assert row[0] == self.length

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
                           last_update = int(time.time()))
                   .returning(func.length(self.parent.blob_table.c.content)))

            res = conn.execute(upd)
            row = res.fetchone()
            logging.debug('append_data %d %d %d', row[0], self.length, len(d))
            assert row[0] == (self.length + len(d))

            conn.commit()
            self.length = row[0]
            self.content_length = content_length
            self.last = (self.length == self.content_length)
            logging.debug('append_data %d %s last=%s',
                          self.length, self.content_length, self.last)

        return True


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

    def load(self, db_id = None, rest_id = None):
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

        with self.parent.conn() as conn:
            res = conn.execute(sel)
            row = res.fetchone()
            if not row:
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
        with self.parent.conn() as conn:
            res = conn.execute(stmt)
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

# sqlalchemy StaticPool used for sqlite in-memory/tests just returns
# the same connection to everyone but doesn't limit concurrent access
# as one might expect
class ConnLock:
    def __init__(self, parent):
        self.parent = parent
    def __enter__(self):
        self.parent._db_write_lock.acquire()
        self.conn = self.parent.engine.connect()
        return self.conn
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.rollback()  # yikes!
        self.parent._db_write_lock.release()
    def commit(self):
        self.conn.commit()

class Storage:
    session_id = None
    tx_versions : IdVersionMap
    engine : Optional[Engine] = None
    inmemory = False
    _db_write_lock : Lock

    # TODO IdVersionMap w/single object id 0?
    created_id = None
    created_lock = None
    created_cv = None

    session_table : Optional[Table] = None
    blob_table : Optional[Table] = None
    tx_table : Optional[Table] = None
    attempt_table : Optional[Table] = None

    post_commit : Dict[object, List[Callable[[], None]]]

    def __init__(self, engine=None):
        self.engine = engine
        self._db_write_lock = Lock()
        self.tx_versions = IdVersionMap()

        self.created_lock = Lock()
        self.created_cv = Condition(self.created_lock)
        self.post_commit = {}

    # TODO context manager thing for this?
    def commit(self, conn):
        logging.debug('Storage.on_commit %s %s',
                      conn, self.post_commit.get(conn, []))
        conn.commit()
        if conn not in self.post_commit:
            return
        for fn in self.post_commit[conn]:
            fn()
        del self.post_commit[conn]

    def add_post_commit(self, conn, fn):
        if conn not in self.post_commit:
            self.post_commit[conn] = []
        self.post_commit[conn].append(fn)

    @staticmethod
    def get_sqlite_inmemory_for_test():
        engine = create_engine("sqlite+pysqlite://",
                               connect_args={'check_same_thread':False},
                               poolclass=StaticPool)
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
        s.inmemory = True
        return s

    @staticmethod
    def connect_sqlite(filename):
        engine = create_engine("sqlite+pysqlite:///" + filename)
        with engine.connect() as conn:
            cursor = conn.connection.cursor()
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
    def connect_postgres(db_user=None, db_name=None, host=None, port=None,
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


    def conn(self):
        if self.inmemory:
            return ConnLock(self)
        return self.engine.connect()

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
        self.tx_blob_table = Table(
            'transactionblobrefs', self.metadata, autoload_with=self.engine)

        self.attempt_table = Table(
            'transactionattempts', self.metadata, autoload_with=self.engine)

        with self.conn() as conn:
            # for the moment, we only evict sessions that the pid no
            # longer exists but as this evolves, we might also
            # periodically check that our own session hasn't been evicted
            proc_self = psutil.Process()
            ins = (insert(self.session_table).values(
                pid = proc_self.pid,
                pid_create = int(proc_self.create_time()))
                   .returning(self.session_table.c.id))
            res = conn.execute(ins)
            self.session_id = res.fetchone()[0]
            conn.commit()

    def recover(self):
        with self.conn() as conn:
            sel = select(self.session_table.c.id,
                         self.session_table.c.pid,
                         self.session_table.c.pid_create)
            res = conn.execute(sel)
            for row in res:
                (db_id, pid, pid_create) = row
                if not Storage.check_pid(pid, pid_create):
                    logging.info('deleting stale session %s %s %s',
                                 db_id, pid, pid_create)
                    dele = (delete(self.session_table)
                            .where(self.session_table.c.id == db_id))
                    conn.execute(dele)

            conn.commit()

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

    def get_transaction_cursor(self) -> TransactionCursor:
        return TransactionCursor(self)

    def load_one(self, min_age=0):
        with self.conn() as conn:
            max_recent = int(time.time()) - min_age
            # TODO this is currently a scan, probably want to bake the
            # next attempt time logic into the previous request completion so
            # you can index on it

            # TODO maybe input_done or last_update_session == our session

            # iow don't recover incomplete after a crash until the
            # client writes to it again so we don't start it upstream
            # if the client went away
            sel = (select(self.tx_table.c.id,
                          self.tx_table.c.version)
                   .where(self.tx_table.c.inflight_session_id.is_(None),
                          self.tx_table.c.attempt_count <
                          self.tx_table.c.max_attempts,
                          self.tx_table.c.output_done.is_not(sa_true()),
                          self.tx_table.c.json.is_not(None))
                   .limit(1))
            res = conn.execute(sel)
            row = res.fetchone()
            if not row:
                return None
            db_id,version = row

            tx = self.get_transaction_cursor()
            tx._start_attempt_db(conn, db_id, version)

            self.tx_versions.update(tx.id, tx.version)

            # TODO: if the last n consecutive actions are all
            # load/recover, this transaction may be crashing the system ->
            # quarantine

            conn.commit()
            return tx

    def _gc_non_durable_one(self, min_age):
        max_recent = int(time.time()) - min_age

        with self.conn() as conn:
            sel = (select(self.tx_table.c.id)
                   .where(self.tx_table.c.input_done.is_not(sa_true()),
                          self.tx_table.c.output_done.is_not(sa_true()),
                          self.tx_table.c.max_attempts == 1,
                          self.tx_table.c.last_update <= max_recent)
                   .limit(1))
            res = conn.execute(sel)
            row = res.fetchone()
            if row is None:
                return False

            tx_cursor = self.get_transaction_cursor()
            tx_cursor._load_db(conn, db_id = row[0])
            tx_cursor._abort(conn)

            conn.commit()

        return True

    def gc_non_durable(self, min_age):
        count = 0
        while True:
            if not self._gc_non_durable_one(min_age):
                break
            count += 1
        return count

    def wait_created(self, db_id, timeout=None):
        with self.created_lock:
            logging.debug('Storage.wait_created %s %s', self.created_id, db_id)
            fn = lambda: self.created_id is not None and (
                db_id is None or self.created_id > db_id)
            return self.created_cv.wait_for(fn, timeout)
