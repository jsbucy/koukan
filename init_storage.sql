PRAGMA foreign_keys = ON;  -- XXX on every connection
PRAGMA journal_mode=WAL;
PRAGMA auto_vacuum=2;  -- incremental

-- XXX this should probably be named something like SendRequests instead?
CREATE TABLE Transactions (
  id INTEGER PRIMARY KEY,  -- autoincrement?
  -- tag/queue/service/host
  json text,

  status int,  -- 0: waiting, 1: inflight, 2: done
  pid int,  -- if status == inflight
  length int,

  creation int,
  last_update int  -- unix secs, null until finalized
);

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
DELETE FROM BlobFiles WHERE id in (SELECT id from Blob LEFT JOIN (SELECT DISTINCT blob_id as trans_blob_id FROM TransactionContent)
WHERE trans_blob_id = NULL);
PRAGMA incremental_vacuum;
*/
