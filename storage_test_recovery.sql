INSERT INTO Sessions (id, pid, pid_create) VALUES (100,2,3);

INSERT INTO Transactions (id, rest_id, json, status, inflight_session_id, creation, last_update, length, last, version) VALUES (12345, "xyz", '{"mail_from": "alice@example.com"}', 2, 100, 1, 1, 12, true, 0);

INSERT INTO TransactionContent (transaction_id, i, inline, length) VALUES (12345, 0, "hello, world", 5);

INSERT INTO TransactionActions (transaction_id, action_id, time, action)
VALUES (12345, 1, 1, 1);
