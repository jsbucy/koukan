from typing import Optional

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
    ONESHOT_DONE = 4

class Action:
    INSERT = 0
    LOAD = 1  # WAITING -> INFLIGHT
    RECOVER = 5  # INFLIGHT w/stale session -> WAITING
    DELIVERED = 2
    TEMP_FAIL = 3
    PERM_FAIL = 4
    ABORT = 6
    START = 7

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
        with parent.db_write_lock:
            cursor = parent.db.cursor()
            cursor.execute(
                'INSERT INTO Transactions '
                '  (rest_id, inflight_session_id, creation, version, status, '
                'length, last) '
                'VALUES (?, ?, ?, ?, ?, ?, ?)',
                (rest_id, parent.session_id, int(time.time()), 0,
                 Status.INSERT, 0, False))
            parent.db.commit()
            self.id = cursor.lastrowid
            self.version = 0

        with parent.created_lock:
            assert(parent.created_id is None or
                   self.id > parent.created_id)
            parent.created_id = self.id
            parent.created_cv.notify_all()

    def write_envelope(self,
              local_host : str, remote_host : str,
              mail_from : str, transaction_esmtp,
              rcpt_to : str, rcpt_esmtp,
              host : str):
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
            cursor.execute(
                'INSERT INTO TransactionActions '
                '(transaction_id, action_id, time, action) '
                'VALUES (?,?,?,?)',
                (self.id, 0, int(time.time()), Action.INSERT))
            # XXX need to catch exceptions and db.rollback()? (throughout)
            self.parent.db.commit()
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
    def append_data(self, d : bytes):
        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()
            cursor.execute(
                'SELECT status,version From Transactions WHERE id = ?',
                (self.id,))
            row = cursor.fetchone()
            assert(row is not None and
                   (row[0] == Status.INSERT or
                    row[0] == Status.INFLIGHT or
                    row[0] == Status.ONESHOT_DONE))
            assert(row[1] == self.version)

            cursor.execute(
                'INSERT INTO TransactionContent '
                '(transaction_id, i, length, inline) '
                'VALUES (?, ?, ?, ?)',
                (self.id, self.i, len(d), d))
            cursor.execute(
                'UPDATE Transactions SET length = ?, version = ? '
                'WHERE id = ? AND version = ?',
                (self.i + 1, self.version + 1, self.id, self.version))

            self.parent.db.commit()
            self.version += 1
            self.parent.versions.update(self.id, self.version)

        self.i += 1

    APPEND_BLOB_OK = 0
    APPEND_BLOB_UNKNOWN = 1

    def append_blob(self, blob_id : str, length : int) -> int:
        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()
            cursor.execute(
                'SELECT status,version From Transactions WHERE id = ?',
                (self.id,))
            row = cursor.fetchone()
            assert(row is not None and
                   (row[0] in [Status.INSERT, Status.ONESHOT_DONE,
                               Status.INFLIGHT]))
            logging.debug('append_blob version id=%d db %d new %d',
                          self.id, row[1], self.version)
            assert(row[1] == self.version)
            # XXX we may want to append this before the blob is
            # finalized? in which case this isn't known yet.
            cursor.execute(
                'SELECT length FROM Blob WHERE id = ?',
                (blob_id,))
            row = cursor.fetchone()
            if not row:
                return TransactionCursor.APPEND_BLOB_UNKNOWN
            (blob_len,) = row

            cursor.execute(
                'INSERT INTO TransactionContent '
                '(transaction_id, i, length, blob_id) '
                'VALUES (?, ?, ?, ?)',
                (self.id, self.i, length, blob_id))
            cursor.execute(
                'UPDATE Transactions SET length = ?, version = ? '
                'WHERE id = ? AND version = ?',
                (self.i + 1, self.version + 1, self.id, self.version))

            self.parent.db.commit()
            self.version += 1
            self.parent.versions.update(self.id, self.version)


        self.i += 1
        return TransactionCursor.APPEND_BLOB_OK

    def load(self, id : Optional[int] = None, rest_id : Optional[str] = None):
        self.id = id
        cursor = self.parent.db.cursor()
        where = None
        where_id = None
        assert(id is not None or rest_id is not None)
        if id is not None:
            where = 'WHERE id = ?'
            where_id = (id,)
        elif rest_id is not None:
            where = 'WHERE rest_id = ?'
            where_id = (rest_id,)

        cursor.execute('SELECT id,rest_id,creation,json,length,status,version,'
                       'last '
                       'FROM Transactions ' + where,
                       where_id)
        row = cursor.fetchone()
        if not row:
            return False
        #if length is None:
            # select sum(length) from (
            # select if(Tx.inline is not null, length(tx.inline),
            #    if(tx.blob_id is not null, blob.length, null)) as length
            # from TransactionContent left join Blob on TransactionContent.blob_id = Blob.Id)

        (self.id, self.rest_id,
         self.creation, json_str, self.length, self.status, self.version,
         self.last) = row
        assert(id is None or self.id == id)
        assert(rest_id is None or self.rest_id == rest_id)
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
        cursor = self.parent.db.cursor()
        cursor.execute('SELECT version from Transactions WHERE id = ?',
                       (self.id,))
        row = cursor.fetchone()
        assert(row is not None and row[0] >= old)
        if row[0] > old:
            return self.load(self.id)

        if not self.parent.versions.wait(self, timeout): return False
        return self.load(self.id)

    def wait_for(self, fn, timeout=None):
        start = time.monotonic() if timeout is not None else None
        timeout_left = timeout
        while True:
            if fn(): return True
            if timeout is not None and timeout_left <= 0: return False
            if not self.wait(timeout_left): return False
            timeout_left = (timeout - (time.monotonic() - start)
                            if timeout is not None else None)
        return False

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
        r.start(id=blob_id)
        return r

    def finalize_payload(self, status=None):
        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()
            cursor.execute('SELECT version from Transactions WHERE id = ?',
                           (self.id,))
            row = cursor.fetchone()
            assert(row[0] == self.version)
            status_set = ''
            status_val = ()
            if status is not None:
                status_set = ', status = ?'
                status_val = (status,)
            cursor.execute(
                'UPDATE Transactions '
                'SET last_update = ?, last = ?, version = ? ' + status_set +
                'WHERE id = ? AND version = ?',
                (int(time.time()), True, self.version + 1) + status_val +
                (self.id, self.version))
            self.parent.db.commit()
            self.version += 1
            self.parent.versions.update(self.id, self.version)

        return True

    # appends a TransactionAttempts record and marks Transaction done
    def append_action(self, action, response : Response):
        now = int(time.time())
        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()
            status = None

#            if self.offset is None and self.length is None:
#                status = Status.ONESHOT_DONE
            if (action == Action.DELIVERED or
                action == Action.PERM_FAIL or
                action == Action.ABORT):
                status = Status.DONE
            elif action == Action.TEMP_FAIL:
                status = Status.WAITING
            elif action == Action.START:
                status = Status.INFLIGHT
            logging.info('TransactionCursor.append_action '
                         'now=%d id=%d action=%d i=%s length=%s status=%d',
                         now, self.id, action, self.i, self.length, status)
            cursor.execute(
                'UPDATE Transactions '
                'SET status = ?, last_update = ?, version = ?, '
                'inflight_session_id = NULL '
                'WHERE id = ? AND version = ?',
                (status, now, self.version + 1, self.id, self.version))
            cursor.execute(
                'INSERT INTO TransactionActions '
                '(transaction_id, action_id, time, action, response_json) '
                'VALUES ('
                '  ?,'
                '  (SELECT MAX(action_id) FROM TransactionActions '
                '   WHERE transaction_id = ?) + 1,'
                '  ?,?,?)',
                (self.id, self.id, now, action, json.dumps(response.to_json())))

            # TODO gc payload on status DONE (still need ONESHOT_DONE?)
            # (or ttl, keep for ~1d after DONE?)
            self.parent.db.commit()
            self.version += 1

class BlobWriter:
    def __init__(self, storage):
        self.parent = storage
        self.id = None
        self.offset = 0
        self.length = None  # overall length from content-range
        self.rest_id = None

    def start(self, rest_id):  # xxx create()
        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()
            cursor.execute(
                'INSERT INTO Blob (last_update, rest_id) '
                'VALUES (?, ?)', (int(time.time()), rest_id))
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

    # Note: as of python 3.11 sqlite.Connection.blobopen() returns a
    # file-like object blob reader handle so this may be less necessary
    def append_data(self, d : bytes, length=None):
        logging.info('BlobWriter.append %d %s off=%d d.len=%d length=%s '
                     'new length=%s',
                     self.id, self.rest_id, self.offset, len(d), self.length,
                     length)
        assert(self.length is None or (self.length == length))
        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()
            dd = d
            while dd:
                chunk = dd[0:self.CHUNK_SIZE]
                cursor.execute(
                    'INSERT INTO BlobContent (id, offset, content) '
                    'VALUES (?,?,?)',
                    (self.id, self.offset, chunk))
                self.parent.db.commit()  # XXX

                dd = dd[self.CHUNK_SIZE:]
                self.offset += len(chunk)
            logging.info('BlobWriter.append_data id=%d length=%s',
                         self.id, length)
            cursor.execute(
                'UPDATE Blob SET length = ?, last_update = ? '
                'WHERE id = ? AND ((length IS NULL) OR (length = ?))',
                (length, int(time.time()), self.id, length))
            assert(cursor.rowcount == 1)
            self.parent.db.commit()
            self.length = length

        self.parent.blob_versions.update(self.id, self.offset)
        return True


class BlobReader(Blob):
    blob_id : Optional[int]

    def __init__(self, storage):
        self.parent = storage
        self.blob_id = None
        self.length = None
        self.rest_id = None

    def len(self):
        return self.length

    def id(self):
        return 'storage_%s' % self.blob_id

    def start(self, id = None, rest_id = None):  # xxx load()
        self.blob_id = id
        cursor = self.parent.db.cursor()
        where = ''
        where_val = ()
        if id is not None:
            where = 'id = ?'
            where_val = (id,)
        elif rest_id is not None:
            where = 'rest_id = ?'
            where_val = (rest_id,)
        cursor.execute('SELECT id,rest_id,length FROM Blob WHERE ' + where,
                       where_val)
        row = cursor.fetchone()
        if not row:
            return None
        self.blob_id = row[0]
        self.rest_id = row[1]
        self.length = row[2]
        return self.length

    def read_content(self, offset) -> Optional[bytes]:
        # xxx precondition offset/self.length?

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
        old_len = self.length
        if not self.parent.blob_versions._wait(
                self, self.blob_id, old_len, timeout):
            return False
        self.start(self.blob_id)
        return self.length > old_len

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
        logging.debug('update id=%d version=%d', id, version)
        with self.lock:
            if id not in self.version: return
            waiter = self.version[id]
            assert(waiter.version <= version)
            waiter.version = version
            self.cv.notify_all()


    def wait(self, cursor : TransactionCursor, timeout=None) -> bool:
        assert(cursor.id is not None)
        return self._wait(cursor, cursor.id, cursor.version, timeout)

    def _wait(self, obj : object, id : int, version : int,
              timeout=None) -> bool:
        logging.debug('_wait id=%d version=%d', id, version)
        with self.lock:
            if id not in self.version:
                self.version[id] = Waiter(version)
            waiter = self.version[id]
            assert(waiter.version >= version)
            if waiter.version > version:
                return True
            waiter.waiters.add(obj)
            self.cv.wait_for(lambda: waiter.version > version,
                             timeout=timeout)
            rv = waiter.version > version
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
            cursor.execute('SELECT id from Transactions '
                           'WHERE status = ? OR '
                           '(status = ? AND last_update <= ?) '
                           'LIMIT 1',
                           (Status.INSERT, Status.WAITING, max_recent))
            row = cursor.fetchone()
            if not row:
                return None
            id = row[0]

            # TODO: if the last n consecutive actions are all
            # load/recover, this transaction may be crashing the system ->
            # quarantine
            cursor.execute(
                'UPDATE Transactions SET inflight_session_id = ?, status = ? '
                'WHERE id = ?',
                (self.session_id, Status.INFLIGHT, id))
            cursor.execute(
                'INSERT INTO TransactionActions '
                '(transaction_id, action_id, time, action) '
                'VALUES ('
                '  ?,'
                '  (SELECT MAX(action_id) FROM TransactionActions'
                '   WHERE transaction_id = ?) + 1,'
                '  ?,?)',
                (id, id, int(time.time()), Action.LOAD))
            self.db.commit()
            tx = self.get_transaction_cursor()
            assert(tx.load(id=id))
            return tx

    def wait_created(self, id, timeout=None):
        with self.created_lock:
            fn = lambda: self.created_id is not None and (
                id is None or id > self.created_id)
            return self.created_cv.wait_for(fn, timeout)


# forward path
# set_durable() will typically be concurrent with a transaction?
# have the data in ephemeral blob storage (possibly mem)



