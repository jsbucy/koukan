INSERT INTO Sessions (pid, pid_create) VALUES (2,1707248500);

INSERT INTO Blob (rest_id, length, last_update, content)
 VALUES ('body_rest_id', 4, 1707248591, 'body');

INSERT INTO Transactions (rest_id, json, creation_session_id, inflight_session_id, creation, last_update, last, version, body_blob_id, body_rest_id) VALUES ('xyz', '{"mail_from": {"m":"alice@example.com"}, "host": "outbound-gw"}', (select min(id) from sessions), (select id from sessions where pid_create = 1707248500 limit 1), 1707248590, 1707248592, true, 0, (select id from blob limit 1), 'body_rest_id');
