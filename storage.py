from typing import Optional, Dict

from blob import Blob, InlineBlob
from threading import Lock, Condition

import psutil

import sqlite3
import json
import time

import logging

from response import Response

class Status:
    INSERT = 0  # uncommitted
    WAITING = 1
    INFLIGHT = 2
    DONE = 3
    ONESHOT_DONE = 4  # set_durable noop
    ONESHOT_INFLIGHT = 5
    ONESHOT_TEMP = 6  # set_durable -> WAITING

# TODO it seems like these fine-grained codes delivered/temp/perm are
# redundant with the resp, this is a vestige of not originally storing
# the resp? does anything directly reading the db distinguish those?
# or do you want it to capture things like upstream temp + max retry
# -> perm? or maybe that should be an additional action? conceptually
# that is a separate attempt LOAD, TEMP, LOAD, PERM though in practice
# the completion logic writes that?
class Action:
    INSERT = 0
    LOAD = 1  # WAITING -> INFLIGHT
              # INSERT -> ONESHOT_INFLIGHT
    RECOVER = 5  # INFLIGHT w/stale session -> WAITING
    DELIVERED = 2
    TEMP_FAIL = 3
    PERM_FAIL = 4
    ABORT = 6
    START = 7  # inflight -> inflight (no change)
    SET_DURABLE = 8  # ONESHOT_DONE -> WAITING or
                     # ONESHOT_INFLIGHT -> INFLIGHT

# TransactionCursor:
# sync success/permfail (single mx temp) -> oneshot
#   create
#   write envelope
#   append action(resp)
#     status ONESHOT_DONE
#     action DELIVERED | PERM_FAIL

# durable (msa tempfail, multi-mx mixed)
#   create
#   write envelope
#   append blob
#   append action
#     status DONE | WAITING
#     action DELIVERED | TEMP_FAIL | PERM_FAIL

# retry
#   load
#   read blobs
#   append action

class TransactionCursor:
    # XXX need a transaction/request object or something
    FIELDS = ['local_host', 'remote_host', 'mail_from', 'transaction_esmtp',
              'rcpt_to', 'rcpt_esmtp', 'host']

    # json fields
    local_host = None
    remote_host = None
    mail_from = None
    transaction_esmtp = None
    rcpt_to = None
    rcpt_esmtp = None
    host = None

    id : Optional[int] = None
    status = None
    length = None
    i = 0  # TransactionContent index
    version : Optional[int] = None
    max_i = None

    def __init__(self, storage):
        self.parent = storage
        self.id = None

    def create(self, rest_id):
        parent = self.parent
        now = int(time.time())
        with parent.db_write_lock:
            cursor = parent.db.cursor()
            cursor.execute(
                'INSERT INTO Transactions '
                '  (rest_id, inflight_session_id, creation, last_update, version, status, '
                'last) '
                'VALUES (?, ?, ?, ?, ?, ?, ?)',
                (rest_id, parent.session_id, now, now, 0,
                 Status.INSERT, False))
            parent.db.commit()
            self.id = cursor.lastrowid
            self.version = 0

            cursor.execute(
                'INSERT INTO TransactionActions '
                '(transaction_id, action_id, time, action) '
                'VALUES (?,?,?,?)',
                (self.id, 0, int(time.time()), Action.INSERT))

        with parent.created_lock:
            assert(parent.created_id is None or
                   self.id > parent.created_id)
            parent.created_id = self.id
            parent.created_cv.notify_all()

    def write_envelope(
            self,
            local_host : Optional[str] = None,
            remote_host : Optional[str] = None,
            mail_from : Optional[str] = None,
            transaction_esmtp : Optional[Dict[str,str]] = None,
            rcpt_to : Optional[str] = None,
            rcpt_esmtp  : Optional[Dict[str,str]] = None,
            host : Optional[str] = None):
        trans_json = {
            'local_host': local_host,
            'remote_host': remote_host,
            'mail_from': mail_from,
            'transaction_esmtp': transaction_esmtp,
            'rcpt_to': rcpt_to,
            'rcpt_esmtp': rcpt_esmtp,
            'host': host,
        }
        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()
            cursor.execute('SELECT version from Transactions WHERE id = ?',
                           (self.id,))
            row = cursor.fetchone()
            assert(row is not None and (row[0] == self.version))

            cursor.execute(
                'UPDATE Transactions SET json = ?, version = ? '
                'WHERE id = ? AND version = ?',
                (json.dumps(trans_json), self.version + 1, self.id,
                 self.version))

            # XXX need to catch exceptions and db.rollback()? (throughout)
            self.parent.db.commit()
            assert(cursor.rowcount == 1)

            self.version += 1
            logging.info('write_envelope id=%d version=%d',
                         self.id, self.version)

            self.parent.versions.update(self.id, self.version)
        return True

    # TODO maybe this should be stricter and require the previous blob
    # to at least have its length determined (i.e. PUT with the
    # content-range overall length set) before you can do another append to the
    # message. That would allow the key to be the byte offset
    # (allowing pread) instead of the simple index/counter i we're
    # using now.
    def append_data(self, d : bytes, last : bool):
        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()
            cursor.execute(
                'SELECT status,version,last From Transactions WHERE id = ?',
                (self.id,))
            row = cursor.fetchone()
            status,version,db_last = row
            logging.info('append_data status=%d', status)
            assert(status in [Status.INSERT,
                              Status.INFLIGHT,  # xxx
                              Status.ONESHOT_INFLIGHT,
                              Status.ONESHOT_DONE,
                              Status.ONESHOT_TEMP])  # e.g. start tempfailed upstream
            assert(version == self.version)
            assert(not db_last)
            cursor.execute(
                'INSERT INTO TransactionContent '
                '(transaction_id, i, length, inline) '
                'VALUES (?, ?, ?, ?)',
                (self.id, self.i, len(d), d))
            new_version = self.version + 1
            cursor.execute(
                'UPDATE Transactions SET version = ?, last = ?'
                'WHERE id = ? AND version = ?',
                (new_version, last, self.id, self.version))
            self.parent.db.commit()
            assert(cursor.rowcount == 1)
            self.version = new_version
            self.parent.versions.update(self.id, self.version)

        self.i += 1

    APPEND_BLOB_OK = 0
    APPEND_BLOB_UNKNOWN = 1

    def append_blob(self, blob_rest_id : str, last : bool) -> int:
        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()
            cursor.execute(
                'SELECT status,version,last From Transactions WHERE id = ?',
                (self.id,))
            row = cursor.fetchone()
            status,version,db_last = row
            assert(status in [Status.INSERT,
                              Status.ONESHOT_DONE,
                              Status.INFLIGHT,    # xxx
                              Status.ONESHOT_INFLIGHT ])
            logging.debug('append_blob version id=%d db %d new %d status %d '
                          'db last %s op last %s',
                          self.id, row[1], self.version, status, db_last, last)
            assert(version == self.version)
            assert(not db_last)
            # XXX we may want to append this before the blob is
            # finalized? in which case this isn't known yet.
            cursor.execute(
                'SELECT id,length FROM Blob WHERE rest_id = ?',
                (blob_rest_id,))
            row = cursor.fetchone()
            if not row:
                return TransactionCursor.APPEND_BLOB_UNKNOWN
            blob_id,blob_len = row

            cursor.execute(
                'INSERT INTO TransactionContent '
                '(transaction_id, i, blob_id) '
                'VALUES (?, ?, ?)',
                (self.id, self.i, blob_id))
            new_version = self.version + 1
            cursor.execute(
                'UPDATE Transactions SET version = ?, last = ? '
                'WHERE id = ? AND version = ?',
                (new_version, last, self.id, self.version))
            self.parent.db.commit()
            assert(cursor.rowcount == 1)
        self.version = new_version
        self.parent.versions.update(self.id, self.version)
        self.i += 1
        return TransactionCursor.APPEND_BLOB_OK

    def load(self, db_id : Optional[int] = None,
             rest_id : Optional[str] = None):
        assert(db_id is not None or rest_id is not None)

        cursor = self.parent.db.cursor()
        where = None
        where_id = None

        if db_id is not None:
            where = 'WHERE id = ?'
            where_id = (db_id,)
        elif rest_id is not None:
            where = 'WHERE rest_id = ?'
            where_id = (rest_id,)

        cursor.execute('SELECT id,rest_id,creation,json,status,version,'
                       'last '
                       'FROM Transactions ' + where,
                       where_id)
        row = cursor.fetchone()
        if not row:
            return False

        (self.id, self.rest_id,
         self.creation, json_str, self.status, self.version,
         self.last) = row

        trans_json = json.loads(json_str) if json_str else {}
        for a in TransactionCursor.FIELDS:
            self.__setattr__(a, trans_json.get(a, None))

        cursor.execute('SELECT max(i) '
                       'FROM TransactionContent WHERE transaction_id = ?',
                       (self.id,))
        row = cursor.fetchone()
        self.max_i = row[0]

        return True

    def wait(self, timeout=None) -> bool:
        old = self.version
        self.load(self.id)
        if self.version > old:
            return True

        if not self.parent.versions.wait(self, timeout):
            logging.debug('TransactionCursor.wait timed out')
            return False
        self.load(self.id)
        return self.version > old  # xxx assert?

    def wait_for(self, fn, timeout=None):
        start = time.monotonic() if timeout is not None else None
        timeout_left = timeout
        while True:
            if fn(): return True
            if timeout is not None and timeout_left <= 0: return False
            if not self.wait(timeout_left): return False
            timeout_left = (timeout - (time.monotonic() - start)
                            if timeout is not None else None)
        asset(not 'unreachable')  #return False

    def wait_attr_not_none(self, attr, timeout=None):
        return self.wait_for(
            lambda: hasattr(self, attr) and getattr(self, attr) is not None,
            timeout)

    # -> (time, action, optional[response])
    def load_last_action(self, num_actions):
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
        r.start(db_id=blob_id)
        return r

    def _check_payload_done(self, cursor) -> bool:
        cursor.execute('SELECT COUNT(*) '
                       'FROM TransactionContent JOIN Blob '
                       'ON TransactionContent.blob_id = Blob.id '
                       'WHERE TransactionContent.transaction_id = ? '
                       '  AND Blob.last != True',
                       (self.id,))
        row = cursor.fetchone()
        assert(row is not None)
        return row[0] == 0

    # appends a TransactionAttempts record and marks Transaction done
    def append_action(self, action,
                      response : Optional[Response] = None) -> bool:
        now = int(time.time())
        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()

            cursor.execute(
                'SELECT status,version from Transactions WHERE id = ?',
                (self.id,))
            row = cursor.fetchone()
            status,version = row
            logging.debug('append_action db version %d my version %d '
                          'status=%d action=%d',
                          version, self.version, status, action)
            # XXX this is too conservative e.g.
            # writer may SET_DURABLE while reader is inflight
            #assert(version == self.version)

            if status == Status.INSERT:
                if action == Action.SET_DURABLE:
                    status = Status.WAITING
                elif action == Action.LOAD:
                    status = Status.ONESHOT_INFLIGHT
                elif action == Action.ABORT:
                    status = Status.DONE
                else:
                    assert(not "unexpected status")
            elif status == Status.ONESHOT_INFLIGHT:
                if action in [Action.DELIVERED,
                              Action.PERM_FAIL,
                              Action.ABORT]:
                    status = Status.ONESHOT_DONE
                elif action == Action.TEMP_FAIL:
                    status = Status.ONESHOT_TEMP
                elif action == Action.START:
                    # no change
                    assert(status in [Status.INFLIGHT, Status.ONESHOT_INFLIGHT])
                elif action == Action.SET_DURABLE:
                    status = Status.INFLIGHT
                else:
                    assert(not "unexpected status")
            elif status == Status.ONESHOT_TEMP:
                if action == Action.SET_DURABLE:
                    status = Status.WAITING
                elif action == Action.ABORT:
                    status = Status.DONE
                else:
                    assert(not "unexpected status")
            elif status == Status.WAITING:
                assert(action == Action.LOAD)
            elif status == Status.DONE:
                # XXX ambiguous, at least 2 possibilities:
                # 1: tx was aborted
                # 2: previous set_durable succeeded but resp timed out
                #    and this is a retry
                # for the sake of the tests for the time being,
                # assume #1, probably we need a separate
                # Status.ABORTED or this needs to read the last action
                # and error out if it was Action.ABORT
                return False
            elif status == Status.ONESHOT_DONE:
                assert(action == Action.SET_DURABLE)
            elif status == Status.INFLIGHT:
                if action in [Action.DELIVERED,
                              Action.PERM_FAIL,
                              Action.ABORT]:
                    status = Status.DONE
                elif action == Action.START:
                    pass  # no change
                elif action == Action.TEMP_FAIL:
                    status = Status.WAITING
                else:
                    assert(not "unexpected status")

            if action == Action.SET_DURABLE:
                assert(self._check_payload_done(cursor))  # precondition

            logging.info('TransactionCursor.append_action '
                         'now=%d id=%d action=%d i=%s length=%s status=%d',
                         now, self.id, action, self.i, self.length, status)
            new_version = version + 1
            cursor.execute(
                'UPDATE Transactions '
                'SET status = ?, last_update = ?, version = ?, '
                'inflight_session_id = NULL '
                'WHERE id = ? AND version = ?',
                (status, now, new_version, self.id, version))
            assert(cursor.rowcount == 1)

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

            # TODO gc payload on status DONE (still need ONESHOT_DONE?)
            # (or ttl, keep for ~1d after DONE?)
            self.parent.db.commit()
            self.version = new_version
            self.parent.versions.update(self.id, self.version)
            return True

class BlobWriter:
    id = None
    offset = 0
    length = None  # overall length from content-range
    rest_id = None
    last = False

    def __init__(self, storage):
        self.parent = storage

    def start(self, rest_id):  # xxx create()
        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()
            cursor.execute(
                'INSERT INTO Blob (last_update, rest_id, last) '
                'VALUES (?, ?, false)', (int(time.time()), rest_id))
            self.parent.db.commit()
            self.id = cursor.lastrowid
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
            logging.info('BlobWriter.append_data id=%d length=%s',
                         self.id, length)
            last = length is not None and self.offset == length
            cursor.execute(
                'UPDATE Blob SET length = ?, last_update = ?, last = ? '
                'WHERE id = ? AND ((length IS NULL) OR (length = ?))',
                (length, int(time.time()), last,
                 self.id, length))
            assert(cursor.rowcount == 1)
            self.parent.db.commit()
            self.length = length
            self.last = last

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

    def start(self, db_id = None, rest_id = None):  # xxx load()
        self.blob_id = db_id
        cursor = self.parent.db.cursor()
        where = ''
        where_val = ()
        if id is not None:
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
        old_len = self.offset
        if not self.parent.blob_versions._wait(
                self, self.blob_id, old_len, timeout):
            return False
        self.start(self.blob_id)
        return old_len is None or (self.length > old_len)

    # wait until fully written
    def wait(self, timeout=None):
        while not self.last:
            self.wait_length(timeout)

class Waiter:
    version : int
    waiters : set[object]
    def __init__(self, version):
        self.waiters = set()
        self.version = version

class VersionWaiter:
    lock : Lock
    cv : Condition
    version : dict[int,Waiter] = None  # db id

    def __init__(self):
        self.version = {}
        self.lock = Lock()
        self.cv = Condition(self.lock)

    def update(self, id, version):
        logging.debug('VersionWaiter.update id=%d version=%d', id, version)
        with self.lock:
            if id not in self.version: return
            waiter = self.version[id]
            assert(waiter.version is None or (waiter.version <= version))
            waiter.version = version
            self.cv.notify_all()


    def wait(self, cursor : TransactionCursor, timeout=None) -> bool:
        assert(cursor.id is not None)
        return self._wait(cursor, cursor.id, cursor.version, timeout)

    def _wait(self, obj : object, id : int, version : int,
              timeout=None) -> bool:
        logging.debug('_wait id=%d version=%s', id, version)
        with self.lock:
            if id not in self.version:
                self.version[id] = Waiter(version)
            waiter = self.version[id]
            assert(waiter.version is None or (waiter.version >= version))
            if waiter.version is not None and (waiter.version > version):
                return True
            waiter.waiters.add(obj)
            self.cv.wait_for(
                lambda: waiter.version is not None and (version is None or (waiter.version > version)),
                timeout=timeout)
            rv = waiter.version is not None and (version is None or (waiter.version > version))
            waiter.waiters.remove(obj)
            if not waiter.waiters:
                del self.version[id]
            return rv

class Storage:
    session_id = None
    tx_versions : VersionWaiter
    blob_versions : VersionWaiter

    # TODO VersionWaiter w/single object id 0?
    created_id = None
    created_lock = None
    created_cv = None

    def __init__(self):
        self.db = None
        self.db_write_lock = Lock()
        self.versions = VersionWaiter()
        self.blob_versions = VersionWaiter()

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
        cursor.execute('INSERT INTO Sessions (pid, pid_create) VALUES (?,?)',
                       (proc_self.pid, int(proc_self.create_time())))
        self.db.commit()
        self.session_id = cursor.lastrowid
        self.recover()

    def recover(self):
        cursor = self.db.cursor()
        cursor.execute('SELECT id, pid, pid_create FROM Sessions')
        for row in cursor:
            (id, pid, pid_create) = row
            if not Storage.check_pid(pid, pid_create):
                logging.info('deleting stale session %s %s %s',
                             id, pid, pid_create)
                cursor.execute('DELETE FROM Sessions WHERE id = ?', (id,))
                self.db.commit()

        logging.info('recover transactions')
        cursor.execute('SELECT id FROM Transactions '
                       'WHERE status = ? AND inflight_session_id is NULL',
                       (Status.INFLIGHT,))

        recovered = 0
        for row in cursor:
            (id,) = row
            logging.info('Storage.recover orphaned transaction %d', id)
            cursor.execute('UPDATE Transactions SET status = ? WHERE id = ?',
                           (Status.WAITING, id))
            cursor.execute(
                'INSERT INTO TransactionActions '
                '(transaction_id, action_id, time, action) '
                'VALUES ('
                '  ?,'
                '  (SELECT MAX(action_id) FROM TransactionActions '
                '   WHERE transaction_id = ?) + 1,'
                '  ?,?)',
                (id, id, int(time.time()), Action.RECOVER))
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
            cursor.execute('SELECT id,status from Transactions '
                           'WHERE status = ? OR '
                           '(status = ? AND last_update <= ?) '
                           'LIMIT 1',
                           (Status.INSERT, Status.WAITING, max_recent))
            row = cursor.fetchone()
            if not row:
                return None
            db_id,status = row

            if status == Status.INSERT:
                new_status = Status.ONESHOT_INFLIGHT
            elif status == Status.WAITING:
                new_status = Status.INFLIGHT

            # TODO: if the last n consecutive actions are all
            # load/recover, this transaction may be crashing the system ->
            # quarantine
            # TODO: move to inner TransactionCursor.append_action in db tx
            # with above select?
            cursor.execute(
                'UPDATE Transactions SET inflight_session_id = ?, status = ? '
                'WHERE id = ?',
                (self.session_id, new_status, db_id))
            cursor.execute(
                'INSERT INTO TransactionActions '
                '(transaction_id, action_id, time, action) '
                'VALUES ('
                '  ?,'
                '  (SELECT MAX(action_id) FROM TransactionActions'
                '   WHERE transaction_id = ?) + 1,'
                '  ?,?)',
                (db_id, db_id, int(time.time()), Action.LOAD))
            self.db.commit()
            tx = self.get_transaction_cursor()
            assert(tx.load(db_id=db_id))
            return tx


    # abort non-durable transactions
    def _gc_non_durable_one(self, min_age):
        max_recent = int(time.time()) - min_age
        cursor = self.db.cursor()
        # XXX refactor with TransactionCursor.append_action so this
        # can be a single db transaction
        cursor.execute('SELECT id from Transactions '
                       'WHERE last_update <= ? AND '
                       'status IN (?, ?) LIMIT 1',
                       (max_recent, Status.INSERT, Status.ONESHOT_TEMP))
        row = cursor.fetchone()
        if row is None: return False

        tx_cursor = self.get_transaction_cursor()
        tx_cursor.load(db_id = row[0])
        tx_cursor.append_action(Action.ABORT)

        return True

    def gc_non_durable(self, min_age):
        count = 0
        while True:
            if not self._gc_non_durable_one(min_age): break
            count += 1
        return count

    def wait_created(self, db_id, timeout=None):
        with self.created_lock:
            fn = lambda: self.created_id is not None and (
                db_id is None or db_id > self.created_id)
            return self.created_cv.wait_for(fn, timeout)


# forward path
# set_durable() will typically be concurrent with a transaction?
# have the data in ephemeral blob storage (possibly mem)



