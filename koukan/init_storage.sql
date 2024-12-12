PRAGMA foreign_keys = ON;  -- XXX on every connection
PRAGMA journal_mode=WAL;
PRAGMA auto_vacuum=2;  -- incremental

CREATE TABLE Sessions (
  id INTEGER PRIMARY KEY,
  creation INTEGER NOT NULL,
  live BOOL,
  last_update INTEGER NOT NULL,
  uri TEXT NOT NULL,
  UNIQUE(id, live)
);

CREATE TABLE Transactions (
  id INTEGER PRIMARY KEY,  -- autoincrement?
  rest_id TEXT UNIQUE,

  -- tag/queue/service/host
  json JSON,

  -- bool, basically payload is completely written
  input_done int,

  -- human-readable explanation of why we aren't retrying this message further
  -- add an int/enum if we need to handle programmatically
  -- the presence of this field gates recovery
  final_attempt_reason TEXT,

  -- XXX dead?
  -- append(last=True) has been called, guarantees that TransactionContent
  -- not growing but not that all blobs are finalized
  last bool,

  -- session that created this
  creation_session_id INTEGER,
  -- session that output attempt is currently active in
  inflight_session_id INTEGER,
  inflight_session_live BOOL,

  creation INTEGER NOT NULL,
  last_update INTEGER NOT NULL,
  version INTEGER NOT NULL,

  next_attempt_time INTEGER,  -- unix secs

  message_builder JSON,

  -- json.notification is present and non-empty
  notification BOOL,

  -- notification was null when final_attempt_reason was set
  -- the exploder may enable notifications on an upstream transaction
  -- after it has reached a final status; this is to facilitate recovering these
  no_final_notification BOOL,

  FOREIGN KEY(inflight_session_id, inflight_session_live) REFERENCES Sessions(id, live)
    ON UPDATE SET NULL
    ON DELETE SET NULL
);

CREATE INDEX TxRestId on Transactions (rest_id);

CREATE TABLE TransactionBlobRefs (
  transaction_id INTEGER,
  tx_rest_id TEXT,
  blob_id INTEGER,
  rest_id TEXT,

  FOREIGN KEY(transaction_id) REFERENCES Transactions(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,

  FOREIGN KEY(tx_rest_id) REFERENCES Transactions(rest_id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,

  FOREIGN KEY(blob_id) REFERENCES Blob(id)
    ON UPDATE CASCADE
    ON DELETE SET NULL,

  PRIMARY KEY(transaction_id, blob_id)
);

-- TODO add timestamps
CREATE TABLE TransactionAttempts (
  transaction_id INTEGER,
  attempt_id INTEGER NOT NULL,

  responses JSON,

  creation INTEGER NOT NULL,
  last_update INTEGER NOT NULL,

  PRIMARY KEY(transaction_id, attempt_id),

  FOREIGN KEY(transaction_id) REFERENCES Transactions(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE
);

CREATE TABLE Blob (
  id INTEGER PRIMARY KEY,
  -- final length declared by client in content-length header
  length INTEGER,
  creation INTEGER NOT NULL,
  last_update INTEGER NOT NULL,
  content BLOB
);
