PRAGMA foreign_keys = ON;  -- XXX on every connection
PRAGMA journal_mode=WAL;
PRAGMA auto_vacuum=2;  -- incremental

CREATE TABLE Sessions (
  id INTEGER PRIMARY KEY,
  pid int,
  pid_create int,
  UNIQUE(pid, pid_create)
);

-- XXX this should probably be named something like SendRequests instead?
CREATE TABLE Transactions (
  id INTEGER PRIMARY KEY,  -- autoincrement?
  rest_id text UNIQUE,

  -- tag/queue/service/host
  json text,

  status int,
  length int,

  inflight_session_id int,

  creation int,
  last_update int,  -- unix secs, null until finalized

  FOREIGN KEY(inflight_session_id) REFERENCES Sessions(id)
    ON UPDATE CASCADE  -- xxx moot?
    ON DELETE SET NULL
);

CREATE INDEX TxRestId on Transactions (rest_id);

CREATE TABLE TransactionContent (
  transaction_id int,
  offset int,
  inline BLOB,
  blob_id text,

  FOREIGN KEY(transaction_id) REFERENCES Transactions(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,

  FOREIGN KEY(blob_id) REFERENCES Blob(id),

  PRIMARY KEY(transaction_id, offset)
);

CREATE TABLE TransactionActions (
  transaction_id int,
  action_id int NOT NULL,
  time int,
  action int,

  response_json text,

  FOREIGN KEY(transaction_id) REFERENCES Transactions(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,

  PRIMARY KEY(transaction_id, action_id)
);


CREATE TABLE Blob (
  id INTEGER PRIMARY KEY,
  length int,  -- null until finalized
  last_update int  -- unix secs, null until finalized
);

-- this should be striped out at the granularity you want to read back
-- into memory later ~1MiB
CREATE TABLE BlobContent (
  id TEXT,
  offset int,
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
