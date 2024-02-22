from typing import Optional, Dict, List, Tuple
import sqlite3
import json
import time
import logging
from threading import Lock, Condition
from hashlib import sha256
from base64 import b64encode

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool
from sqlalchemy import LargeBinary, MetaData, Table, cast, func, insert, or_, select, update


import psutil

from blob import Blob, InlineBlob
from response import Response
from storage_schema import InvalidActionException, VersionConflictException
from filter import TransactionMetadata, WhichJson


class TransactionCursor:
    id : Optional[int] = None
    rest_id : Optional[str] = None
    attempt_id : Optional[int] = None

    version : Optional[int] = None

    creation : Optional[int] = None
    input_done : Optional[bool] = None
    output_done : Optional[bool] = None

    tx : Optional[TransactionMetadata] = None


    def __init__(self, storage):
        self.parent = storage
        self.id = None

    def etag(self) -> str:
        base = '%d.%d.%d' % (self.creation, self.id, self.version)
        return base
        # xxx enable in prod
        #return b64encode(
        #    sha256(base.encode('us-ascii')).digest()).decode('us-ascii')

    # create_body_rest_id
    # XXX maybe this should be refactored into
    # insert; update in the same db tx
    def create(self, rest_id, tx : TransactionMetadata,
               create_body_rest_id : Optional[str] = None):
        parent = self.parent
        self.creation = int(time.time())
        with self.parent.conn() as conn:
            cursor = conn.connection.cursor()
            self.last = False
            self.version = 0

            body_blob_id, body_rest_id, input_done = self._set_body(
                cursor, conn, tx.body, create_body_rest_id)

            max_attempts = tx.max_attempts if tx.max_attempts else 1
            db_json = json.dumps(tx.to_json(WhichJson.DB))
            logging.debug('TxCursor.create %s %s', rest_id, db_json)
            cursor.execute("""
                INSERT INTO Transactions
                (rest_id, creation, last_update, version, last,
                 json, attempt_count, max_attempts, body_blob_id, body_rest_id,
                 input_done)
                VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
                RETURNING id""",
                (rest_id, self.creation, self.creation,
                 self.version, self.last, db_json,
                 max_attempts, body_blob_id, body_rest_id, input_done))
            self.id = cursor.fetchone()[0]
            self.tx = tx
            self.input_done = input_done
            conn.connection.commit()

        with parent.created_lock:
            logging.debug('TransactionCursor.create id %d', self.id)
            if parent.created_id is None or self.id > parent.created_id:
                parent.created_id = self.id
                parent.created_cv.notify_all()

    def _set_body(self, cursor, conn,
                  reuse_body_rest_id=None, create_body_rest_id=None
                  ) -> Tuple[Optional[int], Optional[str], Optional[bool]]:
                  # -> id, rest_id, input_done
        if reuse_body_rest_id:
            cursor.execute(
                'SELECT id,length(content),length FROM Blob WHERE rest_id = ?',
                (reuse_body_rest_id,))
            row = cursor.fetchone()
            # xxx need proper reporting of this
            assert row
            body_blob_id,length,content_length = row
            done = (length == content_length)
            # XXX allow sharing incomplete for exploder but not rest?
            return body_blob_id, reuse_body_rest_id, done
        elif create_body_rest_id:
            blob_writer = self.parent.get_blob_writer()
            body_rest_id = create_body_rest_id
            body_blob_id = blob_writer._create(
                body_rest_id, conn)
            return body_blob_id, create_body_rest_id, False
        return None, None, None

    def write_envelope(self, tx_delta : TransactionMetadata,
                       create_body_rest_id : Optional[str] = None):
        with self.parent.conn() as conn:
            cursor = conn.connection.cursor()
            # TODO this should just load(), trying to save a single
            # point read from the db isn't worth worrying about the
            # state in the cursor getting out of sync with the db.
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
            set_max_attempts = ''
            set_max_attempts_val = ()
            if tx_delta.max_attempts:
                set_max_attempts = ', max_attempts = ?'
                set_max_attempts_val = (tx_delta.max_attempts,)

            set_body = ''
            set_body_val = ()
            # xxx err if this was already set?
            body_blob_id, body_rest_id, input_done = self._set_body(
                cursor, conn, tx_delta.body, create_body_rest_id)

            if body_blob_id:
                set_body = (', body_blob_id = ?, body_rest_id = ?, '
                            'input_done = ?')
                set_body_val = (body_blob_id, body_rest_id, input_done)

            cursor.execute("""
              UPDATE Transactions SET json = ?, version = ?,
              max_attempts = 1, attempt_count = 0, last_update = ?
              """ + set_max_attempts + set_body + """
              WHERE id = ? AND version = ? RETURNING version""",
              (json.dumps(tx_to_db.to_json(WhichJson.DB)), new_version, now) +
              set_max_attempts_val + set_body_val +
              (self.id, self.version))

            if (row := cursor.fetchone()) is None or row[0] != new_version:
                raise VersionConflictException()
            self.version = row[0]
            self.input_done = input_done

            # XXX need to catch exceptions and db.rollback()? (throughout)
            conn.connection.commit()

            # TODO or RETURNING json
            # XXX doesn't include response fields? tx.merge() should
            # handle that?
            self.tx = self.tx.merge(tx_delta)

            logging.info('write_envelope id=%d version=%d',
                         self.id, self.version)

            self.parent.tx_versions.update(self.id, self.version)
        return True

    def _set_response(self, col : str, response : Response):
        assert(self.attempt_id is not None)
        with self.parent.conn() as conn:
            cursor = conn.connection.cursor()
            cursor.execute('UPDATE TransactionAttempts SET ' + col + ' = ? '
                           'WHERE transaction_id = ? AND attempt_id = ?',
                           (json.dumps(response.to_json()),
                            self.id, self.attempt_id))
            new_version = self.version + 1
            cursor.execute('UPDATE Transactions SET version = ? '
                           'WHERE version = ? AND id = ? RETURNING version',
                           (new_version, self.version, self.id))
            if (row := cursor.fetchone()) is None or row[0] != new_version:
                raise VersionConflictException()
            conn.connection.commit()
            self.version = row[0]
            self.parent.tx_versions.update(self.id, new_version)


    def set_mail_response(self, response : Response):
        # XXX enforce that req field is populated
        self._set_response('mail_response', response)

    def add_rcpt_response(self, response : List[Response]):
        assert(self.attempt_id is not None)
        with self.parent.conn() as conn:
            cursor = conn.connection.cursor()
            # TODO load-that's-not-a-load, cf write_envelope()
            cursor.execute(
                'SELECT version,json FROM Transactions WHERE id = ?',
                (self.id,))
            db_version,tx_json_str = cursor.fetchone()
            tx_json = json.loads(tx_json_str)
            if db_version != self.version:
                raise VersionConflictException()
            cursor.execute('SELECT rcpt_response FROM TransactionAttempts '
                           'WHERE transaction_id = ? AND attempt_id = ?',
                           (self.id, self.attempt_id))
            old_resp = []
            row = cursor.fetchone()
            if row and row[0]:
                old_resp = json.loads(row[0])
            old_resp.extend([r.to_json() for r in response])
            if len(old_resp) > len(tx_json['rcpt_to']):
                logging.critical(
                    'add_rcpt_response %d %s %s %s',
                    self.id, self.rest_id, old_resp, tx_json['rcpt_to'])
                assert False

            cursor.execute('UPDATE TransactionAttempts SET rcpt_response = ?'
                           'WHERE transaction_id = ? AND attempt_id = ?',
                           (json.dumps(old_resp),
                            self.id, self.attempt_id))
            new_version = self.version + 1
            cursor.execute('UPDATE Transactions SET version = ? '
                           'WHERE version = ? AND id = ? RETURNING version',
                           (new_version, self.version, self.id))
            if (row := cursor.fetchone()) is None or row[0] != new_version:
                raise VersionConflictException()
            self.version = row[0]
            conn.connection.commit()
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
            cursor = conn.connection.cursor()
            return self._load_db(cursor, db_id, rest_id)

    def _load_db(self, cursor,
                 db_id : Optional[int] = None,
                 rest_id : Optional[str] = None
                 ) -> Optional[TransactionMetadata]:
        where = None
        where_id = None

        if db_id is not None:
            where = 'WHERE id = ?'
            where_id = (db_id,)
        elif rest_id is not None:
            where = 'WHERE rest_id = ?'
            where_id = (rest_id,)

        cursor.execute("""
          SELECT id,rest_id,creation,json,version,
          last,input_done,output_done,body_rest_id
          FROM Transactions """ + where,
          where_id)
        row = cursor.fetchone()
        if not row:
            return None

        (self.id, self.rest_id,
         self.creation, json_str, self.version, self.last,
         self.input_done, self.output_done, self.body_rest_id) = row[0:9]

        trans_json = json.loads(json_str) if json_str else {}
        logging.debug('TransactionCursor._load_db %s %s',
                      self.rest_id, row)
        self.tx = TransactionMetadata.from_json(trans_json, WhichJson.DB)
        self.tx.body = self.body_rest_id

        cursor.execute("""
          SELECT attempt_id,mail_response,rcpt_response,data_response
          FROM TransactionAttempts WHERE transaction_id = ?
          ORDER BY attempt_id DESC LIMIT 1""", (self.id,))
        row = cursor.fetchone()
        if row is not None:
            self.attempt_id,mail_json,rcpt_json,data_json = row
            if mail_json:
                self.tx.mail_response = Response.from_json(json.loads(mail_json))
            if rcpt_json:
                self.tx.rcpt_response = [
                    Response.from_json(r) for r in json.loads(rcpt_json)]
            if data_json:
                self.tx.data_response = Response.from_json(json.loads(data_json))

        return self.tx

    def _start_attempt_db(self, cursor, db_id, version):
        assert self.parent.session_id is not None
        cursor.execute("""
            UPDATE Transactions
            SET version = ?, inflight_session_id = ?,
            attempt_count = (SELECT COUNT(*) + 1 FROM TransactionAttempts
                             WHERE transaction_id = ?)
            WHERE id = ? AND version = ? RETURNING version""",
            (version+1, self.parent.session_id, db_id, db_id,
             version))
        assert cursor.fetchone()
        cursor.execute(
            """INSERT INTO TransactionAttempts (transaction_id, attempt_id)
            VALUES (?, (SELECT iif(i IS NULL, 1, i+1)
            FROM (SELECT max(attempt_id) AS i from TransactionAttempts
            WHERE transaction_id = ?))) RETURNING attempt_id""",
            (db_id, db_id))
        assert (row := cursor.fetchone())
        self.attempt_id = row[0]
        self._load_db(cursor, db_id=db_id)

    def _set_max_attempts(self, max_attempts, cursor):
        new_version = self.version + 1
        cursor.execute("""
          UPDATE Transactions
          SET max_attempts = ?, last_update = ?, version = ?
          WHERE id = ? AND version = ? RETURNING version""",
                       (max_attempts, time.time(), new_version,
                        self.id, self.version))
        if (row := cursor.fetchone()) is None or row[0] != new_version:
            raise VersionConflictException()
        self.version = new_version
        self.parent.tx_versions.update(self.id, self.version)

    def set_max_attempts(self, max_attempts : Optional[int] = None):
        with self.parent.conn() as conn:
            cursor = conn.connection.cursor()
            self._set_max_attempts(max_attempts, cursor)
            conn.connection.commit()

    def finalize_attempt(self, output_done):
        with self.parent.conn() as conn:
            cursor = conn.connection.cursor()
            self._finalize_attempt(cursor, output_done)
            conn.connection.commit()

    def _finalize_attempt(self, cursor, output_done):
        new_version = self.version + 1
        now = int(time.time())
        cursor.execute("""
          UPDATE Transactions
          SET inflight_session_id = NULL,
          output_done = ?,
          last_update = ?,
          version = ?
          WHERE id = ? AND version = ?
          RETURNING version
        """, (output_done, now, new_version, self.id, self.version))

        if (row := cursor.fetchone()) is None or row[0] != new_version:
            raise VersionConflictException()
        self.version = new_version
        self.parent.tx_versions.update(self.id, self.version)

    def wait(self, timeout=None) -> bool:
        old = self.version
        self.load()
        if self.version > old:
            return True

        if not self.parent.tx_versions.wait(self, self.id, old, timeout):
            logging.debug('TransactionCursor.wait timed out')
            return False
        self.load()
        return self.version > old  # xxx assert?

    def _set_input_done(self, cursor):
        cursor.execute("""
            UPDATE Transactions
            SET input_done = TRUE,
            version = (SELECT version + 1
                       FROM Transactions WHERE id = ?)
            WHERE id = ? RETURNING version """, (self.id, self.id))
        self.version = cursor.fetchone()[0]
        logging.debug('_set_input_done id=%d version=%d', self.id, self.version)
        # xxx defer until after commit
        self.parent.tx_versions.update(self.id, self.version)
        self.input_done = True
        return True

    def _abort(self, cursor):
        logging.info('TransactionCursor.abort %d %s', self.id, self.rest_id)
        new_version = self.version + 1
        now = int(time.time())
        cursor.execute("""
          UPDATE Transactions
          SET max_attempts = 0, last_update = ?, version = ?
          WHERE id = ? AND version = ? RETURNING version""",
          (now, new_version, self.id, self.version))
        if (row := cursor.fetchone()) is None or row[0] != new_version:
            raise VersionConflictException()
        self.version = new_version
        self.parent.tx_versions.update(self.id, self.version)


    def abort(self):
        with self.parent.conn() as conn:
            cursor = conn.connection.cursor()
            return self._abort(cursor)


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
        with self.parent.engine.connect() as conn:
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

        with self.parent.engine.connect() as conn:
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

            stmt = (update(self.parent.blob_table)
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

            res = conn.execute(stmt)
            row = res.fetchone()
            logging.debug('append_data %d %d %d', row[0], self.length, len(d))
            assert row[0] == (self.length + len(d))

            conn.commit()
            self.length = row[0]
            self.content_length = content_length
            self.last = (self.length == self.content_length)
            logging.debug('append_data %d %s last=%s',
                          self.length, self.content_length, self.last)
            self.parent.blob_versions.update(self.id, self.length)

        if self.last:
            with self.parent.conn() as conn:
                cursor = conn.connection.cursor()
                cursor.execute(
                    'SELECT id FROM Transactions WHERE body_blob_id = ?',
                    (self.id,))
                for row in cursor:
                    logging.debug('BlobWriter.append_data last %d tx %s',
                                  self.id, row)
                    tx = self.parent.get_transaction_cursor()
                    tx._load_db(cursor, row[0])
                    tx._set_input_done(cursor)
                conn.connection.commit()
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

        stmt = select(
            self.parent.blob_table.c.id,
            self.parent.blob_table.c.rest_id,
            self.parent.blob_table.c.length,
            func.length(self.parent.blob_table.c.content))
        if db_id is not None:
            stmt = stmt.where(self.parent.blob_table.c.id == db_id)
        elif rest_id is not None:
            stmt = stmt.where(self.parent.blob_table.c.rest_id == rest_id)

        with self.parent.conn() as conn: # self.parent.engine.connect() as conn:
            res = conn.execute(stmt)
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

    # wait for self.offsetlength to increase or timeout
    def wait(self, timeout=None):
        # version is max offset+len in BlobContent
        old_len = self.length
        if not self.parent.blob_versions.wait(
                self, self.blob_id, old_len, timeout):
            return False
        self.load(self.blob_id)
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
            return self.cv.wait_for(lambda: self.version > version, timeout)
    def update(self, version):
        with self.lock:
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
        self.id_version.waiters.remove(self.obj)
        if not self.id_version.waiters:
            self.parent.del_id(self.db_id)

class IdVersionMap:
    lock : Lock
    id_version_map : dict[int,IdVersion]  # db-id

    def __init__(self):
        self.id_version_map = {}
        self.lock = Lock()

    def del_id(self, id):
        with self.lock:
            del self.id_version_map[id]

    def update(self, db_id, version):
        logging.debug('IdVersionMap.update id=%d version=%d', db_id, version)
        with self.lock:
            if db_id not in self.id_version_map: return
            waiter = self.id_version_map[db_id]
            waiter.update(version)

    def get_id_version(self, db_id, version, obj):
        with self.lock:
            if db_id not in self.id_version_map:
                self.id_version_map[db_id] = IdVersion(db_id, version)
            waiter = self.id_version_map[db_id]
            waiter.waiters.add(obj)
            return waiter

    def wait(self, obj : object, db_id : int, version : int,
             timeout=None) -> bool:
        logging.debug('_wait id=%d version=%s', db_id, version)
        with Waiter(self, db_id, version, obj) as id_version:
            return id_version.wait(version, timeout)

# sqlalchemy StaticPool used in-memory/tests just returns the same
# connection to everyone but doesn't limit to one at a time as one
# might expect
class ConnLock:
    def __init__(self, parent):
        self.parent = parent
    def __enter__(self):
        self.parent._db_write_lock.acquire()
        self.conn = self.parent.engine.connect()
        return self.conn
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.parent._db_write_lock.release()
    def commit(self):
        self.conn.commit()

class Storage:
    session_id = None
    tx_versions : IdVersionMap
    blob_versions : IdVersionMap
    engine : Optional[Engine] = None
    inmemory = False
    _db_write_lock : Lock

    # TODO IdVersionMap w/single object id 0?
    created_id = None
    created_lock = None
    created_cv = None

    def __init__(self, engine=None):
        self.engine = engine
        self._db_write_lock = Lock()
        self.tx_versions = IdVersionMap()
        self.blob_versions = IdVersionMap()

        self.created_lock = Lock()
        self.created_cv = Condition(self.created_lock)

    @staticmethod
    def get_inmemory_for_test():
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
    def connect(filename):
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

    def conn(self):
        if self.inmemory:
            return ConnLock(self)
        return self.engine.connect()

    def _init_session(self):
        self.metadata = MetaData()
        self.blob_table = Table(
            'Blob', self.metadata, autoload_with=self.engine)

        with self.conn() as conn:
            cursor = conn.connection.cursor()
            # for the moment, we only evict sessions that the pid no
            # longer exists but as this evolves, we might also
            # periodically check that our own session hasn't been evicted
            proc_self = psutil.Process()
            cursor.execute("""
                INSERT INTO Sessions (pid, pid_create) VALUES (?,?)
                RETURNING id""",
                (proc_self.pid, int(proc_self.create_time())))
            self.session_id = cursor.fetchone()[0]
            conn.connection.commit()

    def recover(self):
        with self.conn() as conn:
            cursor = conn.connection.cursor()
            cursor.execute('SELECT id, pid, pid_create FROM Sessions')
            for row in cursor:
                (db_id, pid, pid_create) = row
                if not Storage.check_pid(pid, pid_create):
                    logging.info('deleting stale session %s %s %s',
                                 db_id, pid, pid_create)
                    cursor.execute(
                        'DELETE FROM Sessions WHERE id = ?', (db_id,))
                    conn.connection.commit()

            conn.connection.commit()

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
            cursor = conn.connection.cursor()
            max_recent = int(time.time()) - min_age
            # TODO this is currently a scan, probably want to bake the
            # next attempt time logic into the previous request completion so
            # you can index on it

            # TODO maybe don't load !input_done until some request
            # from the client
            cursor.execute("""
              SELECT id,version from Transactions
              WHERE inflight_session_id is NULL AND
              attempt_count < max_attempts AND
              output_done IS NOT TRUE AND
              json IS NOT NULL
              LIMIT 1""")
            row = cursor.fetchone()
            if not row:
                return None
            db_id,version = row

            tx = self.get_transaction_cursor()
            tx._start_attempt_db(cursor, db_id, version)

            self.tx_versions.update(tx.id, tx.version)

            # TODO: if the last n consecutive actions are all
            # load/recover, this transaction may be crashing the system ->
            # quarantine

            conn.connection.commit()
            return tx


    def _gc_non_durable_one(self, min_age):
        max_recent = int(time.time()) - min_age

        with self.conn() as conn:
            cursor = conn.connection.cursor()

            # TODO the way this is supposed to work is that
            # cursor_to_endpoint() notices that this has been aborted and
            # early returns but it doesn't actually do that. OTOH maybe it
            # should implement the downstream timeout and this should be
            # restricted to inflight_session_id IS NULL
            cursor.execute("""
            SELECT id from Transactions
            WHERE input_done IS NOT TRUE AND
            output_done IS NOT TRUE AND
            (max_attempts = 1 AND last_update <= ?)
            LIMIT 1""", (max_recent,))
            row = cursor.fetchone()
            if row is None:
                return False

            tx_cursor = self.get_transaction_cursor()
            tx_cursor._load_db(cursor, db_id = row[0])
            tx_cursor._abort(cursor)

            conn.connection.commit()

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


# forward path
# set_durable() will typically be concurrent with a transaction?
# have the data in ephemeral blob storage (possibly mem)



