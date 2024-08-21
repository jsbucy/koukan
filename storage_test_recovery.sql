INSERT INTO Sessions (creation, last_update, live)
 VALUES (1234567890, 1234567890, 1);

INSERT INTO Blob (length, last_update, content)
 VALUES (4, 1707248591, 'body');

INSERT INTO Transactions (rest_id, json, creation_session_id, inflight_session_id, inflight_session_live, creation, last_update, last, version ) VALUES ('xyz', '{"mail_from": {"m":"alice@example.com"}, "host": "outbound-gw"}', (select min(id) from sessions), (select id from sessions where creation = 1234567890 limit 1), 1, 1707248590, 1707248592, true, 0);
