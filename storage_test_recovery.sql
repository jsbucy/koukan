INSERT INTO Sessions (id, pid, pid_create) VALUES (100,2,3);

INSERT INTO Transactions (id, rest_id, json, status, inflight_session_id, creation, last_update, last, version, attempt_count, max_attempts) VALUES (12345, "xyz", '{"mail_from": {"m":"alice@example.com"}, "host": "outbound-gw"}', 1, 100, 1, 1, true, 0, 0, 100);

INSERT INTO TransactionContent (transaction_id, i, inline, length) VALUES (12345, 0, "hello, world", 5);

