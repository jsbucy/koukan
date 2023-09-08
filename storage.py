from typing import Optional

import sqlite3
import json
import time

class Status:
    WAITING = 0
    INFLIGHT = 1
    DONE = 2

class Action:
    INSERT = 0
    LOAD = 1
    DELIVERED = 2
    TEMP_FAIL = 3
    PERM_FAIL = 4

class TransactionWriter:
    def __init__(self, storage):
        self.parent = storage
        self.id = None
        self.offset = 0

    def start(self, local_host, remote_host,
              mail_from, transaction_esmtp,
              rcpt_to, rcpt_esmtp,
              host, inflight : bool):
        trans_json = {
            'local_host': local_host,
            'remote_host': remote_host,
            'mail_from': mail_from,
            'transaction_esmtp': transaction_esmtp,
            'rcpt_to': rcpt_to,
            'rcpt_esmtp': rcpt_esmtp,
            'host': host,
        }
        # TODO:
        # not sure if Transaction/BlobWriter should be a single db transaction
        # sqlite3 doesn't support concurrent writes?
        # need a belt+suspenders check that someone else isn't
        # interleaving writes in Transaction/BlobContent with the same
        # id?
        cursor = self.parent.db.cursor()
        status = Status.INFLIGHT if inflight else Status.WAITING
        cursor.execute('INSERT INTO Transactions (json, creation, status) '
                       'VALUES (?, ?, ?)',
                       (json.dumps(trans_json), int(time.time()), status))
        cursor.execute(
            'INSERT INTO TransactionActions '
            '(transaction_id, action_id, time, action) '
            'VALUES (?,?,?,?)',
            (cursor.lastrowid, 0, int(time.time()), Action.INSERT))
        self.parent.db.commit()
        self.id = cursor.lastrowid

    def append_data(self, d : bytes):
        cursor = self.parent.db.cursor()

        cursor.execute(
            'INSERT INTO TransactionContent (transaction_id, offset, inline) '
            'VALUES (?, ?, ?)',
            (self.id, self.offset, d))
        self.parent.db.commit()
        self.offset += len(d)

    APPEND_BLOB_OK = 0
    APPEND_BLOB_UNKNOWN = 1

    def append_blob(self, blob_id : str) -> int:
        cursor = self.parent.db.cursor()

        cursor.execute(
            'SELECT length FROM Blob WHERE id = ? AND length IS NOT NULL',
            (blob_id,))
        row = cursor.fetchone()
        if not row: return TransactionWriter.APPEND_BLOB_UNKNOWN
        (blob_len,) = row

        cursor.execute(
            'INSERT INTO TransactionContent (transaction_id, offset, blob_id) '
            'VALUES (?, ?, ?)',
            (self.id, self.offset, blob_id))
        self.parent.db.commit()

        self.offset += blob_len
        return TransactionWriter.APPEND_BLOB_OK

    def finalize(self):
        cursor = self.parent.db.cursor()
        cursor.execute('UPDATE Transactions SET last_update = ? WHERE id = ?',
                       (int(time.time()), self.id))
        self.parent.db.commit()


class TransactionReader:
    def __init__(self, storage):
        self.parent = storage
        self.offset = 0
        self.blob_reader = None

    def start(self, id):
        self.id = id
        cursor = self.parent.db.cursor()
        cursor.execute('SELECT creation,json FROM Transactions '
                       'WHERE id = ? AND last_update IS NOT NULL',
                       (id, ))
        row = cursor.fetchone()
        if not row: return False
        (self.creation, json_str) = row
        trans_json = json.loads(json_str)
        self.local_host = trans_json['local_host']
        self.remote_host = trans_json['remote_host']
        self.mail_from = trans_json['mail_from']
        self.transaction_esmtp = trans_json['transaction_esmtp']
        self.rcpt_to = trans_json['rcpt_to']
        self.rcpt_esmtp = trans_json['rcpt_esmtp']
        self.host = trans_json['host']
        return True

    def read_content(self) -> Optional[bytes]:
        while True:
            if self.blob_reader:
                content = self.blob_reader.read_content()
                if content:
                    self.offset += len(content)
                    return content

            cursor = self.parent.db.cursor()
            cursor.execute('SELECT inline,blob_id FROM TransactionContent '
                           'WHERE transaction_id = ? AND offset = ?',
                           (self.id, self.offset))
            row = cursor.fetchone()
            if not row: return None
            (inline, blob_id) = row
            if inline:
                self.offset += len(inline)
                return inline
            if blob_id:
                self.blob_reader = self.parent.get_blob_reader()
                # XXX err
                assert(self.blob_reader.start(blob_id) is not None)


class BlobWriter:
    def __init__(self, storage):
        self.parent = storage
        self.id = None
        self.offset = 0

    def start(self):
        cursor = self.parent.db.cursor()
        cursor.execute('INSERT INTO Blob (length) VALUES (NULL)')
        self.parent.db.commit()
        self.id = cursor.lastrowid
        return self.id

    CHUNK_SIZE = 1048576

    # Note: as of python 3.11 sqlite.Connection.blobopen() returns a
    # file-like object blob reader handle so this may be less necessary
    def append_data(self, d : bytes):
        cursor = self.parent.db.cursor()
        dd = d
        while dd:
            cursor.execute(
                'INSERT INTO BlobContent (id, offset, content) VALUES (?,?,?)',
                (self.id, self.offset, dd[0:self.CHUNK_SIZE]))
            dd = dd[self.CHUNK_SIZE:]
        self.parent.db.commit()
        self.offset += len(d)

    def finalize(self):
        cursor = self.parent.db.cursor()
        cursor.execute(
            'UPDATE BLOB SET length = ?, last_update = ? WHERE ID = ?',
            (self.offset, int(time.time()), self.id))
        self.parent.db.commit()


class BlobReader:
    def __init__(self, storage):
        self.parent = storage
        self.blob_id = None
        self.offset = 0

    def start(self, id):
        self.blob_id = id
        cursor = self.parent.db.cursor()
        cursor.execute('SELECT length FROM Blob '
                       'WHERE id = ? AND last_update IS NOT NULL',
                       (self.blob_id,))
        row = cursor.fetchone()
        if not row:
            return None
        return row[0]


    def read_content(self) -> Optional[bytes]:
        cursor = self.parent.db.cursor()
        cursor.execute('SELECT content FROM BlobContent '
                       'WHERE id = ? AND offset = ?',
                       (self.blob_id, self.offset))
        row = cursor.fetchone()
        if not row:
            self.blob_id = None
            return None
        (content,) = row
        self.offset += len(content)
        return content


class Storage:
    def __init__(self):
        pass

    def get_inmemory_for_test():
        with open("init_storage.sql", "r") as f:
            db = Storage.open_db(":memory:")
            db.cursor().executescript(f.read())
            return db

    def open_db(filename):
        return sqlite3.connect(filename, check_same_thread=False)

    # TODO lease breaking
    # set inflight back to waiting if pid invalid, etc.

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

    def get_transaction_writer(self) -> TransactionWriter:
        return TransactionWriter(self)

    def get_blob_writer(self) -> BlobWriter:
        return BlobWriter(self)
    def get_blob_reader(self) -> BlobReader:
        return BlobReader(self)

    def get_transaction_reader(self) -> TransactionReader:
        return TransactionReader(self)

    def load_one(self):
        cursor = self.db.cursor()
        cursor.execute('SELECT id from Transactions WHERE status = ? LIMIT 1',
                       (Status.WAITING,))
        row = cursor.fetchone()
        if not row: return None
        id = row[0]

        cursor.execute('SELECT action_id,action from TransactionActions '
                       'WHERE transaction_id = ? '
                       'ORDER BY action_id DESC LIMIT 3',
                       (id,))
        action_id = None
        loads = 0
        for row in cursor:
            if action_id is None:
                action_id = row[0] + 1
            if row[1] == Action.LOAD:
                loads += 1
        if action_id is None: action_id = 0
        # TODO: if the last n consecutive actions are all loads, this may
        # be crashing the system -> quarantine
        cursor.execute('UPDATE Transactions SET status = ? WHERE id = ?',
                       (Status.INFLIGHT, id))
        cursor.execute(
            'INSERT INTO TransactionActions '
            '(transaction_id, action_id, time, action) '
            'VALUES (?,?,?,?)',
            (id, action_id, int(time.time()), Action.LOAD))
        self.db.commit()
        reader = self.get_transaction_reader()
        assert(reader.start(id))
        return reader

    # appends a TransactionAttempts record and marks Transaction done
    def append_transaction_actions(self, id, action):
        cursor = self.db.cursor()
        cursor.execute('SELECT MAX(action_id) FROM TransactionActions '
                       'WHERE transaction_id = ?', (id,))
        row = cursor.fetchone()
        action_id = 0
        if row is not None and row[0] is not None:
            action_id = row[0] + 1
        now = int(time.time())
        status = None
        if action == Action.DELIVERED or action == Action.PERM_FAIL:
            status = Status.DONE
        elif action == Action.TEMP_FAIL:
            status = Status.WAITING
        cursor.execute(
            'UPDATE Transactions SET status = ?, last_update = ? '
            'WHERE id = ?',
            (status, now, id))
        cursor.execute(
            'INSERT INTO TransactionActions '
            '(transaction_id, action_id, time, action) '
            'VALUES (?,?,?,?)',
            (id, action_id, now, action))
        self.db.commit()

# forward path
# set_durable() will typically be concurrent with a transaction?
# have the data in ephemeral blob storage (possibly mem)



