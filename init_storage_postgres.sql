CREATE TABLE Sessions (
  id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  pid INTEGER,
  pid_create INTEGER,
  UNIQUE(pid, pid_create)
);

CREATE TABLE Blob (
  id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  rest_id TEXT UNIQUE,
  -- final length declared by client in content-length header
  length INTEGER,
  last_update INTEGER NOT NULL,
  content BYTEA
);

CREATE INDEX BlobRestId on Blob (rest_id);

CREATE TABLE Transactions (
  id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  rest_id TEXT UNIQUE,

  -- tag/queue/service/host
  json JSON,

  -- bool, basically payload is completely written
  input_done boolean,
  -- bool, max TransactionAttempts was final
  output_done boolean,

  -- append(last=True) has been called, guarantees that TransactionContent
  -- not growing but not that all blobs are finalized
  last bool,

  inflight_session_id INTEGER,

  creation INTEGER,
  last_update INTEGER,
  version INTEGER NOT NULL,

  attempt_count INTEGER,
  max_attempts INTEGER,

  body_blob_id INTEGER,
  body_rest_id TEXT,

  FOREIGN KEY(body_blob_id) REFERENCES Blob(id)
    ON UPDATE CASCADE
    ON DELETE SET NULL,

  FOREIGN KEY(body_rest_id) REFERENCES Blob(rest_id)
    ON UPDATE CASCADE
    ON DELETE SET NULL,

  FOREIGN KEY(inflight_session_id) REFERENCES Sessions(id)
    ON UPDATE CASCADE  -- xxx moot?
    ON DELETE SET NULL
);

CREATE INDEX TxRestId on Transactions (rest_id);
CREATE INDEX TxBodyBlobId on Transactions (body_blob_id);

-- TODO add timestamps
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

