from typing import Optional, Dict, List, Tuple
import sqlite3
import json
import time
import logging
from threading import Lock, Condition
from hashlib import sha256
from base64 import b64encode

import psutil

from blob import Blob, InlineBlob
from response import Response
from storage_schema import Status, Action, transition, InvalidActionException, check_valid_append, VersionConflictException
from filter import TransactionMetadata

class TransactionCursor:
    id : Optional[int] = None
    status : Status = None
    attempt_id : Optional[int] = None

    length = None
    i = 0  # TransactionContent index
    version : Optional[int] = None
    max_i = None
    last = None
    creation : Optional[int] = None
    status : Optional[Status] = None
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

    def create(self, rest_id):
        parent = self.parent
        self.creation = int(time.time())
        with parent.db_write_lock:
            cursor = parent.db.cursor()
            self.status = Status.INSERT
            self.last = False
            self.version = 0
            cursor.execute(
                'INSERT INTO Transactions '
                '  (rest_id, creation, last_update, '
                'version, status, last) '
                'VALUES (?, ?, ?, ?, ?, ?) RETURNING id',
                (rest_id, self.creation, self.creation,
                 self.version, self.status, self.last))
            self.id = cursor.fetchone()[0]
            self.tx = TransactionMetadata()

            cursor.execute(
                'INSERT INTO TransactionActions '
                '(transaction_id, action_id, time, action) '
                'VALUES (?,?,?,?)',
                (self.id, 0, int(time.time()), Action.INSERT))
            parent.db.commit()

        with parent.created_lock:
            logging.debug('TransactionCursor.create id %d', self.id)
            if parent.created_id is None or self.id > parent.created_id:
                parent.created_id = self.id
                parent.created_cv.notify_all()

    def write_envelope(self, tx_delta : TransactionMetadata):
        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()
            # TODO this should just load(), trying to save a single
            # point read rfom the db isn't worth worrying about the
            # state in the cursor getting out of sync with the db.
            tx_to_db = self.tx.merge(tx_delta)
            tx_to_db.mail_response = None
            tx_to_db.rcpt_response = None
            tx_to_db.data_response = None
            # e.g. overwriting an existing field
            # xxx return/throw
            assert tx_to_db is not None
            logging.debug('write_envelope %s', tx_to_db.to_json())
            new_version = self.version + 1
            # TODO all updates '... WHERE inflight_session_id =
            #   self.parent.session' ?
            cursor.execute(
                'UPDATE Transactions SET json = ?, version = ? '
                'WHERE id = ? AND version = ? RETURNING version',
                (json.dumps(tx_to_db.to_json()), new_version, self.id,
                 self.version))

            if (row := cursor.fetchone()) is None:
                raise VersionConflictException()
            self.version = row[0]

            # XXX need to catch exceptions and db.rollback()? (throughout)
            self.parent.db.commit()

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
        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()
            cursor.execute('UPDATE TransactionAttempts SET ' + col + ' = ? '
                           'WHERE transaction_id = ? AND attempt_id = ?',
                           (json.dumps(response.to_json()),
                            self.id, self.attempt_id))
            new_version = self.version + 1
            cursor.execute('UPDATE Transactions SET version = ? '
                           'WHERE version = ? AND id = ? RETURNING version',
                           (new_version, self.version, self.id))
            if (row := cursor.fetchone()) is None:
                raise VersionConflictException()
            self.parent.db.commit()
            self.version = row[0]
            self.parent.tx_versions.update(self.id, new_version)


    def set_mail_response(self, response : Response):
        # XXX enforce that req field is populated
        self._set_response('mail_response', response)

    # xxx this is append responses?
    # enforce that this doesn't add more responses than len(rcpt_to)?
    def add_rcpt_response(self, response : List[Response]):
        assert(self.attempt_id is not None)
        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()
            # TODO load-that's-not-a-load, cf write_envelope()
            cursor.execute('SELECT rcpt_response FROM TransactionAttempts '
                           'WHERE transaction_id = ? AND attempt_id = ?',
                           (self.id, self.attempt_id))
            old_resp = []
            row = cursor.fetchone()
            if row and row[0]:
                old_resp = json.loads(row[0])
            old_resp.extend([r.to_json() for r in response])
            cursor.execute('UPDATE TransactionAttempts SET rcpt_response = ?'
                           'WHERE transaction_id = ? AND attempt_id = ?',
                           (json.dumps(old_resp),
                            self.id, self.attempt_id))
            new_version = self.version + 1
            cursor.execute('UPDATE Transactions SET version = ? '
                           'WHERE version = ? AND id = ? RETURNING version',
                           (new_version, self.version, self.id))
            if (row := cursor.fetchone()) is None:
                raise VersionConflictException()
            self.version = row[0]
            self.parent.db.commit()
            self.parent.tx_versions.update(self.id, new_version)

    def set_data_response(self, response : Response):
        self._set_response('data_response', response)


    # TODO maybe this should be stricter and require the previous blob
    # to at least have its length determined (i.e. PUT with the
    # content-range overall length set) before you can do another append to the
    # message. That would allow the key to be the byte offset
    # (allowing pread) instead of the simple index/counter i we're
    # using now.
    # TODO related, possibly we should require the blob to be
    # finalized before reuse?

    APPEND_BLOB_OK = 0
    APPEND_BLOB_UNKNOWN = 1

    def append_blob(self, d : Optional[bytes] = None,
                    blob_rest_id : Optional[str] = None,
                    last : bool = False) -> int:
        finalized = False
        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()
            # TODO load-that's-not-a-load, cf write_envelope()
            cursor.execute(
                'SELECT status,version,last From Transactions WHERE id = ?',
                (self.id,))
            row = cursor.fetchone()
            status = Status(row[0])
            version,db_last = row[1:]
            check_valid_append(status)

            logging.debug('Storage.append_blob version id=%d db %d new %d '
                          'status %s db last %s op last %s',
                          self.id, row[1], self.version, status.name, db_last,
                          last)
            assert(not db_last)

            if blob_rest_id is not None:
                cursor.execute(
                    'SELECT id,length FROM Blob WHERE rest_id = ?',
                    (blob_rest_id,))
                row = cursor.fetchone()
                if not row:
                    return TransactionCursor.APPEND_BLOB_UNKNOWN
                blob_id,blob_len = row

            col = None
            val = None
            if d is not None:
                col = 'inline'
                val = (d,)
            else:
                col = 'blob_id'
                val = (blob_id,)

            new_max_i = self.max_i + 1 if self.max_i is not None else 0
            cursor.execute(
                'INSERT INTO TransactionContent '
                '(transaction_id, i, ' + col + ') '
                'VALUES (?, ?, ?)',
                (self.id, new_max_i) + val)
            new_version = version + 1
            cursor.execute(
                'UPDATE Transactions SET version = ?, last = ? '
                'WHERE id = ? AND version = ? RETURNING version',
                (new_version, last, self.id, version))
            if (row := cursor.fetchone()) is None:
                raise VersionConflictException()
            self.version = row[0]
            self.last = last
            finalized = self._maybe_finalize(cursor)
            self.parent.db.commit()
        if not finalized:
            self.parent.tx_versions.update(self.id, self.version)
        self.max_i = new_max_i
        return TransactionCursor.APPEND_BLOB_OK

    def load(self, db_id : Optional[int] = None,
             rest_id : Optional[str] = None) -> Optional[TransactionMetadata]:
        if self.id is not None:
            assert(db_id is None and rest_id is None)
            db_id = self.id
        assert(db_id is not None or rest_id is not None)
        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()
            return self._load_db_locked(cursor, db_id, rest_id)

    def _load_db_locked(self, cursor,
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

        cursor.execute('SELECT id,rest_id,creation,json,version,'
                       'last,input_done,output_done,status '
                       'FROM Transactions ' + where,
                       where_id)
        row = cursor.fetchone()
        if not row:
            return None

        (self.id, self.rest_id,
         self.creation, json_str, self.version, self.last,
         self.input_done, self.output_done) = row[0:8]
        self.status = Status(row[8])

        trans_json = json.loads(json_str) if json_str else {}
        logging.debug('TransactionCursor._load_db_locked %s %s',
                      self.rest_id, trans_json)
        self.tx = TransactionMetadata.from_json(trans_json)

        cursor.execute('SELECT max(i) '
                       'FROM TransactionContent WHERE transaction_id = ?',
                       (self.id,))
        row = cursor.fetchone()
        self.max_i = row[0]

        cursor.execute(
            'SELECT attempt_id,mail_response,rcpt_response,data_response '
            'FROM TransactionAttempts WHERE transaction_id = ? '
            'ORDER BY attempt_id DESC LIMIT 1', (self.id,))
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

    def _start_attempt_db(self, cursor, db_id, status, version):
        # xxx keep this in a consistent state during the transition
        if Action.LOAD not in transition[status]:
            logging.critical('_start_attempt_db bad status id %d %s',
                             db_id, str(status))
            assert not '_start_attempt_db precondition failure'
        new_status = transition[status][Action.LOAD]
        cursor.execute(
            'UPDATE Transactions '
            'SET status = ?, version = ?, inflight_session_id = ? '
            'WHERE id = ? AND version = ? RETURNING version',
            (new_status, version+1, self.parent.session_id, db_id, version))
        assert cursor.fetchone()
        cursor.execute(
            'INSERT INTO TransactionAttempts (transaction_id, attempt_id) '
            'VALUES (?, (SELECT iif(i IS NULL, 1, i+1) '
            'FROM (SELECT max(attempt_id) AS i from TransactionAttempts '
            'WHERE transaction_id = ?))) RETURNING attempt_id',
            (db_id, db_id))
        self._load_db_locked(cursor, db_id=db_id)


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

    def wait_for(self, fn, timeout=None):
        if timeout is not None:
            start = time.monotonic()
        timeout_left = timeout
        while True:
            if fn(): return True
            if timeout is not None and timeout_left <= 0:
                return False
            if not self.wait(timeout_left):
                return False
            if timeout is not None:
                timeout_left = timeout - (time.monotonic() - start)
        asset(not 'unreachable')  #return False

    # -> (time, action, optional[response])
    def load_last_action(self, num_actions
                         ) -> List[Tuple[int, int, Optional[Response]]] :
        cursor = self.parent.db.cursor()
        cursor.execute('SELECT version FROM Transactions WHERE id = ?',
                       (self.id,))
        row = cursor.fetchone()
        self.version = row[0]
        cursor.execute('SELECT time, action, response_json '
                       'FROM TransactionActions '
                       'WHERE transaction_id = ? '
                       'ORDER BY action_id DESC LIMIT ?',
                       (self.id, num_actions))
        out = []
        for row in cursor:
            resp = Response.from_json(json.loads(row[2])) if row[2] else None
            out.append((row[0], row[1], resp))
        return out

    def read_content(self, i) -> Optional[Blob]:
        cursor = self.parent.db.cursor()
        cursor.execute('SELECT inline,blob_id FROM TransactionContent '
                       'WHERE transaction_id = ? AND i = ?',
                       (self.id, i))
        row = cursor.fetchone()
        if not row:
            return None
        (inline, blob_id) = row
        if inline:
            return InlineBlob(inline)
        r = self.parent.get_blob_reader()
        r.load(db_id=blob_id)
        return r

    def _check_payload_done(self, cursor) -> bool:
        # XXX this also needs to check Transaction.last?
        cursor.execute('SELECT COUNT(*) '
                       'FROM TransactionContent JOIN Blob '
                       'ON TransactionContent.blob_id = Blob.id '
                       'WHERE TransactionContent.transaction_id = ? '
                       '  AND Blob.last != TRUE',
                       (self.id,))
        row = cursor.fetchone()
        assert(row is not None)
        return row[0] == 0

    def _maybe_finalize(self, cursor):
        if self.last and self._check_payload_done(cursor):
            cursor.execute(
                'UPDATE Transactions '
                'SET input_done = TRUE, '
                'version = (SELECT MAX(version)+1 '
                '           FROM Transactions WHERE id = ?)'
                'WHERE id = ? RETURNING version',
            (self.id, self.id))
            self.version = cursor.fetchone()[0]
            self.parent.tx_versions.update(self.id, self.version)
            return True
        return False

    # next: break up append_action into
    # unload / complete attempt
    # abort

    def append_action(self, action : Action,
                       response : Optional[Response] = None) -> bool:
        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()
            self._append_action_db(cursor, action, response)
            self.parent.db.commit()
        self.parent.tx_versions.update(self.id, self.version)
        return True

    # appends a TransactionAttempts record and updates status
    def _append_action_db(self, cursor, action : Action,
                         response : Optional[Response] = None) -> bool:
        assert action != Action.LOAD
        now = int(time.time())

        # TODO load() here? cf write_envelope()
        logging.debug('TransactionCursor.append_action (pre) id=%d '
                      'my version %d '
                      'status=%s action=%s',
                      self.id, self.version, self.status.name,
                      action.name)

        if (self.status not in transition or
            action not in transition[self.status]):
            raise InvalidActionException(self.status, action)
        status = transition[self.status][action]

        if action == Action.SET_DURABLE:
            assert(self._check_payload_done(cursor))  # precondition

        logging.info('TransactionCursor.append_action (post) id=%d '
                     'now=%d action=%s i=%s length=%s status=%s',
                     self.id, now, action.name, self.i, self.length,
                     self.status.name)
        new_version = self.version + 1
        cursor.execute(
            'UPDATE Transactions '
            'SET status = ?, last_update = ?, version = ?, '
            'output_done = ?, '
            'inflight_session_id = ? '
            'WHERE id = ? AND version = ? RETURNING version',
            (status, now, new_version,
             status in [Status.DONE, Status.ONESHOT_TEMP],
             (self.parent.session_id if action == Action.LOAD else None),
             self.id, self.version))
        if (row := cursor.fetchone()) is None:
            raise VersionConflictException()
        db_new_version = row[0]

        attempt_id = None
        if action in [Action.ABORT, Action.SET_DURABLE]:
            if status == Status.ONESHOT_INFLIGHT:
                cursor.execute(
                    'SELECT MAX(attempt_id) FROM TransactionAttempts '
                    'WHERE transaction_id = ?', (self.id,))
                attempt_id = cursor.fetchone()[0]
        elif action != Action.INSERT:
            assert(self.attempt_id is not None)
            attempt_id = self.attempt_id

        resp_col = ''
        resp_val = ()
        resp_bind = ''
        if response is not None:
            resp_col = ', response_json'
            resp_val = (json.dumps(response.to_json()),)
            resp_bind = ',?'
        cursor.execute(
            'INSERT INTO TransactionActions '
            '(transaction_id, action_id, time, action' + resp_col + ') '
            'VALUES ('
            '  ?,'
            '  (SELECT MAX(action_id) FROM TransactionActions '
            '   WHERE transaction_id = ?) + 1,'
            '  ?,?' + resp_bind + ')',
            (self.id, self.id, now, action) + resp_val)


        # TODO gc payload on status DONE
        # (or ttl, keep for ~1d after DONE?)
        self.status = status
        self.version = db_new_version
        self.attempt_id = attempt_id
        return True

class BlobWriter:
    id = None
    offset = 0
    length = None  # overall length from content-range
    rest_id = None
    last = False

    def __init__(self, storage):
        self.parent = storage

    def create(self, rest_id):
        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()
            cursor.execute(
                'INSERT INTO Blob (last_update, rest_id, last) '
                'VALUES (?, ?, false) RETURNING id',
                (int(time.time()), rest_id))
            self.id = cursor.fetchone()[0]
            self.parent.db.commit()
            self.rest_id = rest_id
        return self.id

    def load(self, rest_id):
        cursor = self.parent.db.cursor()
        cursor.execute('SELECT id, length, last_update FROM Blob WHERE '
                       'rest_id = ?', (rest_id,))
        row = cursor.fetchone()
        if not row:
            return None
        self.id, self.length, self.last_update = row
        self.rest_id = rest_id

        cursor.execute('SELECT offset + LENGTH(content) FROM BlobContent '
                       'WHERE id = ? ORDER BY offset DESC LIMIT 1', (self.id,))
        row = cursor.fetchone()
        if row:
            self.offset = row[0]

        return self.id

    CHUNK_SIZE = 1048576

    # This verifies that noone else wrote to BlobContent since load()
    # Note: as of python 3.11 sqlite.Connection.blobopen() returns a
    # file-like object blob reader handle so striping out the data may
    # be less necessary
    def append_data(self, d : bytes, length=None):
        logging.info('BlobWriter.append_data %d %s off=%d d.len=%d length=%s '
                     'new length=%s',
                     self.id, self.rest_id, self.offset, len(d), self.length,
                     length)
        assert(self.length is None or (self.length == length))
        assert(not self.last)
        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()
            dd = d
            while dd:
                chunk = dd[0:self.CHUNK_SIZE]
                cursor.execute('SELECT offset + LENGTH(content) '
                               'FROM BlobContent WHERE id = ? '
                               'ORDER BY offset DESC LIMIT 1',
                               (self.id,))
                row = cursor.fetchone()
                if self.offset == 0:
                    assert(row is None)
                else:
                    assert(row[0] == self.offset)

                cursor.execute(
                    'INSERT INTO BlobContent (id, offset, content) '
                    'VALUES (?,?,?)',
                    (self.id, self.offset, chunk))
                self.parent.db.commit()  # XXX

                dd = dd[self.CHUNK_SIZE:]
                self.offset += len(chunk)
            last = length is not None and self.offset == length
            logging.info('BlobWriter.append_data id=%d length=%s last=%s',
                         self.id, length, last)
            cursor.execute(
                'UPDATE Blob SET length = ?, last_update = ?, last = ? '
                'WHERE id = ? AND ((length IS NULL) OR (length = ?))',
                (length, int(time.time()), last,
                 self.id, length))
            assert(cursor.rowcount == 1)
            self.length = length
            self.last = last

            if self.last:
                cursor = self.parent.db.cursor()
                cursor.execute(
                    'SELECT DISTINCT transaction_id '
                    'FROM TransactionContent WHERE blob_id = ?', (self.id,))
                for row in cursor:
                    logging.debug('BlobWriter.append_data last %d tx %s',
                                  self.id, row)
                    tx = self.parent.get_transaction_cursor()
                    tx._load_db_locked(cursor, row[0])
                    tx._maybe_finalize(cursor)

            self.parent.db.commit()

        self.parent.blob_versions.update(self.id, self.offset)
        return True


class BlobReader(Blob):
    blob_id : Optional[int]
    last = False
    blob_id = None
    length = None
    rest_id = None
    offset : Optional[int] = None  # max offset in BlobContent

    def __init__(self, storage):
        self.parent = storage

    def len(self):
        return self.length

    def id(self):
        return 'storage_%s' % self.blob_id

    def load(self, db_id = None, rest_id = None):
        self.blob_id = db_id
        cursor = self.parent.db.cursor()
        where = ''
        where_val = ()
        if db_id is not None:
            where = 'id = ?'
            where_val = (db_id,)
        elif rest_id is not None:
            where = 'rest_id = ?'
            where_val = (rest_id,)
        cursor.execute('SELECT id,rest_id,length,last FROM Blob WHERE ' + where,
                       where_val)
        row = cursor.fetchone()
        if not row:
            return None

        self.blob_id = row[0]
        self.rest_id = row[1]
        self.length = row[2]
        self.last = row[3]

        cursor.execute('SELECT offset '
                       'FROM BlobContent WHERE id = ? '
                       'ORDER BY offset DESC LIMIT 1',
                       (self.blob_id,))
        row = cursor.fetchone()
        self.offset = row[0] if row is not None else None

        return self.length

    # xxx clean this up, should probably just read sequentially per
    # self.offset?
    def read_content(self, offset) -> Optional[bytes]:
        if offset is None: return None

        # TODO inflight waiter list thing
        cursor = self.parent.db.cursor()
        cursor.execute('SELECT content FROM BlobContent '
                       'WHERE id = ? AND offset = ?',
                       (self.blob_id, offset))
        row = cursor.fetchone()
        if not row: return None
        return row[0]

    def contents(self):
        dd = bytes()
        while len(dd) < self.length:
            dd += self.read_content(len(dd))
        assert(len(dd) == self.length)
        return dd

    def wait_length(self, timeout):
        old_len = self.offset if self.offset is not None else 0
        if not self.parent.blob_versions.wait(
                self, self.blob_id, old_len, timeout):
            return False
        self.load(self.blob_id)
        return old_len is None or (self.length > old_len)

    # wait until fully written
    def wait(self, timeout=None):
        while not self.last:
            self.wait_length(timeout)

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
            if version <= self.version:
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

class Storage:
    session_id = None
    tx_versions : IdVersionMap
    blob_versions : IdVersionMap

    # TODO IdVersionMap w/single object id 0?
    created_id = None
    created_lock = None
    created_cv = None

    def __init__(self):
        self.db = None
        self.db_write_lock = Lock()
        self.tx_versions = IdVersionMap()
        self.blob_versions = IdVersionMap()

        self.created_lock = Lock()
        self.created_cv = Condition(self.created_lock)

    def get_inmemory_for_test():
        with open("init_storage.sql", "r") as f:
            db = Storage.open_db(":memory:")
            db.cursor().executescript(f.read())
            return db

    @staticmethod
    def open_db(filename : str):
        return sqlite3.connect(filename, check_same_thread=False)

    def connect(self, filename=None, db=None):
        if db:
            self.db = db
        else:
            self.db = Storage.open_db(filename)

        cursor = self.db.cursor()
        # should be sticky from schema but set it here anyway
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        # https://www.sqlite.org/pragma.html#pragma_synchronous
        # FULL=2, flush WAL on every write,
        # NORMAL=1 not durable after power loss
        cursor.execute("PRAGMA synchronous=2")
        cursor.execute("PRAGMA auto_vacuum=2")

        # for the moment, we only evict sessions that the pid no
        # longer exists but as this evolves, we might also
        # periodically check that our own session hasn't been evicted
        proc_self = psutil.Process()
        cursor.execute('INSERT INTO Sessions (pid, pid_create) VALUES (?,?) '
                       'RETURNING id',
                       (proc_self.pid, int(proc_self.create_time())))
        self.session_id = cursor.fetchone()[0]
        self.db.commit()
        self.recover()

    def recover(self):
        cursor = self.db.cursor()
        cursor.execute('SELECT id, pid, pid_create FROM Sessions')
        for row in cursor:
            (db_id, pid, pid_create) = row
            if not Storage.check_pid(pid, pid_create):
                logging.info('deleting stale session %s %s %s',
                             db_id, pid, pid_create)
                cursor.execute('DELETE FROM Sessions WHERE id = ?', (db_id,))
                self.db.commit()

        logging.info('recover transactions')
        cursor.execute('SELECT id FROM Transactions '
                       'WHERE status = ? AND inflight_session_id is NULL',
                       (Status.INFLIGHT,))

        recovered = 0
        for row in cursor:
            (db_id,) = row
            # ick this should use append_action?
            logging.info('Storage.recover orphaned transaction %d', db_id)
            cursor.execute('UPDATE Transactions SET status = ? WHERE id = ?',
                           (Status.WAITING, db_id))
            cursor.execute(
                'INSERT INTO TransactionActions '
                '(transaction_id, action_id, time, action) '
                'VALUES ('
                '  ?,'
                '  (SELECT MAX(action_id) FROM TransactionActions '
                '   WHERE transaction_id = ?) + 1,'
                '  ?,?)',
                (db_id, db_id, int(time.time()), Action.RECOVER))
            recovered += 1
        logging.info('Storage.recover recovered %s transactions', recovered)
        self.db.commit()

        # could probably DELETE FROM Transactions WHERE status = Status.INSERT

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
        with self.db_write_lock:
            cursor = self.db.cursor()
            max_recent = int(time.time()) - min_age
            # TODO this is currently a scan, probably want to bake the
            # next attempt time logic into the previous request completion so
            # you can index on it

            # TODO maybe don't load !input_done until some request
            # from the client
            cursor.execute('SELECT id,status,version from Transactions '
                           'WHERE inflight_session_id is NULL AND '
                           'output_done IS NOT TRUE AND'
                           '(json_extract(json, "$.host") IS NOT NULL) '
                           'LIMIT 1')
            row = cursor.fetchone()
            if not row:
                return None
            db_id,status,version = row

            tx = self.get_transaction_cursor()
            tx._start_attempt_db(cursor, db_id, status, version)

            self.tx_versions.update(tx.id, tx.version)

            # TODO: if the last n consecutive actions are all
            # load/recover, this transaction may be crashing the system ->
            # quarantine

            self.db.commit()
            return tx


    # abort non-durable transactions
    def _gc_non_durable_one(self, min_age):
        max_recent = int(time.time()) - min_age
        cursor = self.db.cursor()

        cursor.execute('SELECT id from Transactions '
                       'WHERE last_update <= ? AND '
                       'input_done IS NOT TRUE AND '
                       'output_done IS NOT TRUE '
                       'LIMIT 1',
                       (max_recent,))
        row = cursor.fetchone()
        if row is None: return False

        tx_cursor = self.get_transaction_cursor()
        tx_cursor._load_db_locked(cursor, db_id = row[0])
        tx_cursor._append_action_db(cursor, Action.ABORT)

        self.db.commit()

        return True

    def gc_non_durable(self, min_age):
        count = 0
        while True:
            if not self._gc_non_durable_one(min_age): break
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



