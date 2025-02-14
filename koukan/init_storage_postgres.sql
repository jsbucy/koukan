CREATE TABLE Sessions (
  id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  creation INTEGER NOT NULL,
  live bool NOT NULL,
  last_update INTEGER NOT NULL,
  uri TEXT NOT NULL,
  UNIQUE(id, live)
);

CREATE TABLE Blob (
  id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  -- final length declared by client in content-length header
  length INTEGER,
  creation INTEGER NOT NULL,
  last_update INTEGER NOT NULL,
  content BYTEA
);

CREATE TABLE Transactions (
  id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  rest_id TEXT UNIQUE,

  -- tag/queue/service/host
  json JSONB,

  -- bool, basically payload is completely written
  input_done boolean,
  final_attempt_reason TEXT,

  -- session that created this
  creation_session_id INTEGER,
  -- session that output attempt is currently active in
  inflight_session_id INTEGER,
  inflight_session_live bool,

  creation INTEGER,
  last_update INTEGER,
  version INTEGER NOT NULL,

  next_attempt_time INTEGER,  -- unix secs

  message_builder JSONB,

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

CREATE TABLE TransactionAttempts (
  transaction_id INTEGER,
  attempt_id INTEGER NOT NULL,

  responses JSONB,

  creation INTEGER NOT NULL,
  last_update INTEGER NOT NULL,

  PRIMARY KEY(transaction_id, attempt_id),

  FOREIGN KEY(transaction_id) REFERENCES Transactions(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE
);

