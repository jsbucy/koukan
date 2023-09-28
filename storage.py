from typing import Optional

from blob import Blob, InlineBlob
from threading import Lock

import psutil

import sqlite3
import json
import time

import logging

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
    id = None
    status = None
    length = None
    offset = None

    def __init__(self, storage):
        self.parent = storage
        self.id = None

    def create(self, rest_id):
        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()
            cursor.execute(
                'INSERT INTO Transactions '
                '  (rest_id, inflight_session_id, creation, status) '
                'VALUES (?, ?, ?, ?)',
                (rest_id, self.parent.session_id, int(time.time()),
                 Status.INSERT))
            self.parent.db.commit()
            self.id = cursor.lastrowid

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
            cursor.execute(
                'UPDATE Transactions SET json = ? WHERE id = ?',
                (json.dumps(trans_json), self.id))
            cursor.execute(
                'INSERT INTO TransactionActions '
                '(transaction_id, action_id, time, action) '
                'VALUES (?,?,?,?)',
                (self.id, 0, int(time.time()), Action.INSERT))
            # XXX need to catch exceptions and db.rollback()? (throughout)
            self.parent.db.commit()
        return True

    def append_data(self, d : bytes):
        if self.offset is None:
            self.offset = 0

        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()
            # TODO: check max(offset) == self.offset?
            cursor.execute(
                'INSERT INTO TransactionContent '
                '(transaction_id, offset, inline) '
                'VALUES (?, ?, ?)',
                (self.id, self.offset, d))
            self.parent.db.commit()
        self.offset += len(d)

    APPEND_BLOB_OK = 0
    APPEND_BLOB_UNKNOWN = 1

    def append_blob(self, blob_id : str) -> int:
        if self.offset is None:
            self.offset = 0

        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()
            cursor.execute(
                'SELECT status From Transactions WHERE id = ?',
                (self.id,))
            row = cursor.fetchone()
            assert(row is not None and
                   (row[0] == Status.INSERT or row[0] == Status.ONESHOT_DONE))

            cursor.execute(
                'SELECT length FROM Blob WHERE id = ? AND length IS NOT NULL',
                (blob_id,))
            row = cursor.fetchone()
            if not row: return TransactionCursor.APPEND_BLOB_UNKNOWN
            (blob_len,) = row

            # TODO: check max(offset) == self.offset?
            cursor.execute(
                'INSERT INTO TransactionContent '
                '(transaction_id, offset, blob_id) '
                'VALUES (?, ?, ?)',
                (self.id, self.offset, blob_id))
            self.parent.db.commit()
        self.offset += blob_len
        return TransactionCursor.APPEND_BLOB_OK



    def load(self, id : int):
        self.id = id
        cursor = self.parent.db.cursor()
        cursor.execute('SELECT creation,json,length,status FROM Transactions '
                       'WHERE id = ?', (id,))
        row = cursor.fetchone()
        if not row: return False
        (self.creation, json_str, self.length, self.status) = row
        trans_json = json.loads(json_str)
        for a in TransactionCursor.FIELDS:
            self.__setattr__(a, trans_json.get(a, None))

        return True

    def read_content(self, offset) -> Optional[Blob]:
        cursor = self.parent.db.cursor()
        cursor.execute('SELECT inline,blob_id FROM TransactionContent '
                       'WHERE transaction_id = ? AND offset = ?',
                       (self.id, offset))
        row = cursor.fetchone()
        if not row: return None
        (inline, blob_id) = row
        if inline:
            return InlineBlob(inline)
        r = self.parent.get_blob_reader()
        r.start(blob_id)
        return r

    def finalize_payload(self, status):
        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()
            cursor.execute(
                'UPDATE Transactions '
                'SET last_update = ?, status = ?, length = ? '
                'WHERE id = ?',
                (int(time.time()), status, self.offset, self.id))
            self.parent.db.commit()
        return True


    # appends a TransactionAttempts record and marks Transaction done
    def append_action(self, action):
        now = int(time.time())
        logging.info('TransactionCursor.append_action %d %d %d %s %s',
                     now, self.id, action, self.offset, self.length)
        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()
            status = None

            if self.offset is None and self.length is None:
                status = Status.ONESHOT_DONE
            elif action == Action.DELIVERED or action == Action.PERM_FAIL:
                status = Status.DONE
            elif action == Action.TEMP_FAIL:
                status = Status.WAITING
            cursor.execute(
                'UPDATE Transactions '
                'SET status = ?, last_update = ?, '
                '  inflight_session_id = NULL '
                'WHERE id = ?',
                (status, now, self.id))
            cursor.execute(
                'INSERT INTO TransactionActions '
                '(transaction_id, action_id, time, action) '
                'VALUES ('
                '  ?,'
                '  (SELECT MAX(action_id) FROM TransactionActions '
                '   WHERE transaction_id = ?) + 1,'
                '  ?,?)',
                (self.id, self.id, now, action))
            self.parent.db.commit()


class BlobWriter:
    def __init__(self, storage):
        self.parent = storage
        self.id = None
        self.offset = 0

    def start(self):
        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()
            cursor.execute('INSERT INTO Blob (length) VALUES (NULL)')
            self.parent.db.commit()
            self.id = cursor.lastrowid
        return self.id

    CHUNK_SIZE = 1048576

    # Note: as of python 3.11 sqlite.Connection.blobopen() returns a
    # file-like object blob reader handle so this may be less necessary
    def append_data(self, d : bytes):
        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()
            dd = d
            while dd:
                # TODO: check max(offset) == self.offset?
                cursor.execute(
                    'INSERT INTO BlobContent (id, offset, content) '
                    'VALUES (?,?,?)',
                    (self.id, self.offset, dd[0:self.CHUNK_SIZE]))
                dd = dd[self.CHUNK_SIZE:]
            self.parent.db.commit()
            self.offset += len(d)

    def finalize(self):
        with self.parent.db_write_lock:
            cursor = self.parent.db.cursor()
            cursor.execute(
                'UPDATE BLOB SET length = ?, last_update = ? WHERE ID = ?',
                (self.offset, int(time.time()), self.id))
            self.parent.db.commit()
        return True


class BlobReader(Blob):
    def __init__(self, storage):
        self.parent = storage
        self.blob_id = None
        self.length = None

    def len(self): return self.length

    def id(self):
        return 'storage_%s' % self.blob_id

    def start(self, id):
        self.blob_id = id
        cursor = self.parent.db.cursor()
        cursor.execute('SELECT length FROM Blob '
                       'WHERE id = ? AND last_update IS NOT NULL',
                       (self.blob_id,))
        row = cursor.fetchone()
        if not row:
            return None
        self.length = row[0]
        return self.length

    def read_content(self, offset) -> Optional[bytes]:
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

class Storage:
    session_id = None

    def __init__(self):
        self.db = None
        self.db_write_lock = Lock()

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
            cursor.execute('SELECT id from Transactions WHERE status = ?'
                           ' AND last_update <= ? LIMIT 1',
                           (Status.WAITING, max_recent))
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
            assert(tx.load(id))
            return tx


# forward path
# set_durable() will typically be concurrent with a transaction?
# have the data in ephemeral blob storage (possibly mem)



