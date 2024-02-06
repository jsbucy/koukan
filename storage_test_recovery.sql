INSERT INTO Sessions (id, pid, pid_create) VALUES (100,2,3);

INSERT INTO Blob (id, rest_id, length, last, last_update)
 VALUES (234, "body_rest_id", 4, TRUE, 1707248591);
INSERT INTO BlobContent (id,offset,content) VALUES (234, 0, "body");

INSERT INTO Transactions (id, rest_id, json, status, inflight_session_id, creation, last_update, last, version, attempt_count, max_attempts, body_blob_id, body_rest_id) VALUES (12345, "xyz", '{"mail_from": {"m":"alice@example.com"}, "host": "outbound-gw"}', 1, 100, 1, 1, true, 0, 0, 100, 234, "body_rest_id");

