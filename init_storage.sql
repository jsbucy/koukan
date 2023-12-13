PRAGMA foreign_keys = ON;  -- XXX on every connection
PRAGMA journal_mode=WAL;
PRAGMA auto_vacuum=2;  -- incremental

CREATE TABLE Sessions (
  id INTEGER PRIMARY KEY,
  pid INTEGER,
  pid_create INTEGER,
  UNIQUE(pid, pid_create)
);

-- XXX this should probably be named something like SendRequests instead?
CREATE TABLE Transactions (
  id INTEGER PRIMARY KEY,  -- autoincrement?
  rest_id TEXT UNIQUE,

  -- tag/queue/service/host
  json TEXT,

  status INTEGER,
  -- length INTEGER NOT NULL,

  -- append(last=True) has been called, guarantees that TransactionContent
  -- not growing but not that all blobs are finalized
  last bool,

  inflight_session_id INTEGER,

  creation INTEGER,
  last_update INTEGER,
  version INTEGER NOT NULL,

  FOREIGN KEY(inflight_session_id) REFERENCES Sessions(id)
    ON UPDATE CASCADE  -- xxx moot?
    ON DELETE SET NULL
);

CREATE INDEX TxRestId on Transactions (rest_id);

CREATE TABLE TransactionAttempts (
  transaction_id INTEGER,
  attempt_id INTEGER NOT NULL,

  mail_response JSON,
  rcpt_response JSON,
  data_response JSON,

  PRIMARY KEY(transaction_id, attempt_id),

  FOREIGN KEY(transaction_id) REFERENCES Transactions(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE
);

CREATE TABLE TransactionContent (
  transaction_id INTEGER,
  i INTEGER NOT NULL,  -- 0,1,2,3

  inline BLOB,
  blob_id TEXT,
  length INTEGER,  -- xxx never read?

  FOREIGN KEY(transaction_id) REFERENCES Transactions(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,

  FOREIGN KEY(blob_id) REFERENCES Blob(id),

  PRIMARY KEY(transaction_id, i)
);

CREATE TABLE TransactionActions (
  transaction_id INTEGER,
  action_id INTEGER NOT NULL,
  attempt_id INTEGER,
  time INTEGER,
  action INTEGER,

  response_json TEXT,

  FOREIGN KEY(transaction_id) REFERENCES Transactions(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,

  FOREIGN KEY(transaction_id, attempt_id)
    REFERENCES TransactionAttempts(transaction_id, attempt_id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,

  PRIMARY KEY(transaction_id, action_id)
);

CREATE TABLE Blob (
  id INTEGER PRIMARY KEY,
  rest_id TEXT UNIQUE,
  length INTEGER,  -- null until we know the overall length
  last bool NOT NULL,  -- True if length is not NULL and max BlobContent ==
  last_update INTEGER NOT NULL
);

CREATE INDEX BlobRestId on Blob (rest_id);

-- this should be striped out at the granularity you want to read back
-- into memory later ~1MiB
CREATE TABLE BlobContent (
  id TEXT,
  offset INTEGER,
  content BLOB,
  FOREIGN KEY (id) REFERENCES Blob(id) ON DELETE CASCADE,
  PRIMARY KEY(id, offset)
);

/*
gc:
step 1:  delete expired transactions
DELETE FROM Transactions WHERE now - last_update > ttl
step 2: delete blobs
DELETE FROM Blob WHERE id in (
SELECT id from Blob LEFT JOIN (SELECT DISTINCT blob_id as trans_blob_id FROM TransactionContent)
WHERE trans_blob_id = NULL);

also drop old INSERT status, etc.

PRAGMA incremental_vacuum;
*/
