INSERT INTO Sessions (id, pid, pid_create) VALUES (100,2,3);

INSERT INTO Transactions (id, json, status, inflight_session_id, creation, last_update) VALUES (1, "{}", 2, 100, 1, 1);

INSERT INTO TransactionActions (transaction_id, action_id, time, action)
VALUES (1, 1, 1, 1);
