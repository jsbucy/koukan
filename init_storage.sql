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
  json JSON,

  status INTEGER,

  -- bool, basically payload is completely written
  input_done int,
  -- bool, max TransactionAttempts was final
  output_done int,

  -- append(last=True) has been called, guarantees that TransactionContent
  -- not growing but not that all blobs are finalized
  last bool,

  inflight_session_id INTEGER,

  creation INTEGER,
  last_update INTEGER,
  version INTEGER NOT NULL,

  attempt_count INTEGER,
  max_attempts INTEGER,

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
  blob_id INTEGER,
  length INTEGER,  -- xxx never read?

  FOREIGN KEY(transaction_id) REFERENCES Transactions(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,

  FOREIGN KEY(blob_id) REFERENCES Blob(id),

  PRIMARY KEY(transaction_id, i)
);

CREATE INDEX TxContentBlob on TransactionContent (blob_id);

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
