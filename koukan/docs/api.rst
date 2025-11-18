===============
Koukan REST API
===============

Sending
=======

Koukan provides a rich http/json rest api to send and receive email.

The api has 1 type of resource: the transaction, a request to send one
message to one recipient. The transaction object contains fields
corresponding to smtp parameters.

Transaction fields
------------------
mail_from: Mailbox

mail_response: Response

rcpt_to: Array[Mailbox]

rcpt_response: Array[Response]

first-class rest api users will only use 1
rcpt_to/rcpt_response. (only the smtp gateway uses multiple)

data_response: Response

Transaction ``body:`` field
---------------------------

In creation requests:

- inline rfc822 ``{"inline": "Subject: hello\r\n\r\n"}``
- a request to reuse a blob ``{"reuse_uri": "/transactions/xyz/body"}``
- a message builder specification `message_builder.json <https://github.com/jsbucy/koukan/blob/a9e58dfee15a4bf26e723f97dc5d7a6052b15fe6/koukan/message_builder.json>`__
  ::

    {"message_builder": {
       "headers": [["subject", "hello"]],
           "text_body": [{
             "content_type": "text/plain",
             "content": {"inline": "hello, world!"}}],
    }}

Returned from GET

- blob status::

    {"blob_status": {"finalized": true}}

- blob url to PUT::

    {"blob_status": {"uri": "http://router.local/transactions/xyz/body"}}

- either of the above for each blob in MessageBuilder Spec::

    {"message_builder": {"blob_status": {
     "my_plain_body": {"finalized": true},
     "my_html_body": {"uri": "http://router.local/transactions/xyz/blob/my_html_body"}}}}


HostPort
--------
host: string

port: int

Mailbox
-------
m: rfc5321 mailbox without <>

e: Array[EsmtpParam]

EsmtpParam
----------
keyword: string

value: string (optional)

Response
--------
code: int

message: string

Simplest case
-------------

If the message only contains a moderate amount of plain text, we can
send a message with a single POST::

    POST /senders/submission/transactions HTTP/1.1
    Content-type: application/json
    {"mail_from": {"m": "alice@example.com"},
     "rcpt_to": {"m": "bob@example.com"},
     "body": {"message_builder": {
         "headers": [["subject", "hello"]],
         "text_body": [{
           "content_type": "text/plain",
           "content": {"inline": "hello, world!"}}],
    }}}

    201 created
    Location: /transactions/xyz
    Content-type: application/json

    {"mail_from": {}, "rcpt_to": {}, "body": {}}

Koukan transactions are write-once; the ``{}`` is a placeholder
indicating the field is populated. Transactions are long-running
operations (lro) that track the status of the message delivery. With a
request-timeout header, Koukan will do a hanging GET::

    GET /transactions/xyz  HTTP/1.1
    request-timeout: 10

    (some time elapses)
    200 ok
    Content-type: application/json

    {"mail_from": {}, "rcpt_to": {}, "body": {},
     "mail_response": {"code": 250 },
     "rcpt_response": {"code": 250 },
     "data_response": {"code": 250 },
     "attempt_count": 1,
     "final_attempt_reason": "upstream response success"
    }

Response fields are per the most recent attempt.

``final_attempt_reason`` is a human-readable string that if non-null
indicates that Koukan is done with this transaction. Completed
transactions are garbage-collected after a configured interval
(e.g. 1h) from the time they were completed.

Large/Binary Attachments
------------------------

If the body is not suitable to inline in JSON, specify an id within
the message_builder spec. Koukan returns a url to PUT the blob to::

    POST /senders/submission/transactions  HTTP/1.1
    Content-type: application/json

    {"mail_from": {"m": "alice@example.com"},
     "rcpt_to": {"m": "bob@example.com"},
     "body": {"message_builder": {
         "headers": [["subject", "hello"]],
         "text_body": [{
           "content_type": "text/html",
           "content": {"create_id": "my_body"}}]
    }}}

    201 created
    Location: /transactions/xyz
    
    {"body": {"message_builder": { "blob_status": {
     "my_body": { "uri": "http://router.local/transactions/xyz/blob/my_body"}}}}}

    PUT /transactions/xyz/blob/my_body  HTTP/1.1
    content-length: 12345678

    200 ok

    GET /transactions/xyz  HTTP/1.1
    
    200 ok
    {"body": {"message_builder": { "blob_status": {
     "my_body": { "finalized": true}}}}}

Blob Reuse
----------

A transaction can reuse an attachment from a previous transaction. If
the reuse succeeded, this will be reflected in ``blob_status``. Note:
the id returned in blob_status will be the same as from the reused blob.
::

    POST /senders/submission/transactions HTTP/1.1
    Content-type: application/json

    {"mail_from": {"m": "alice@example.com"},
     "rcpt_to": {"m": "bob@example.com"},
     "body": {"message_builder": {
         "headers": [["subject", "hello"]],
         "text_body": [{
           "content_type": "text/plain",
           "content": { "reuse_uri": "/transactions/xyz/blob/my_body"}
         }]
    }}}

    201 created
    Location: /transactions/xyz

    {"mail_from": {},
     "rcpt_to": {},
     "body": {"message_builder": { "blob_status": {
     "my_body": {"finalized": true}}}}}

Pre-serialized rfc822/mime message
----------------------------------

If you already have a serialized rfc822/mime message you want to send,
create the transaction without the ``body`` field. Similar to above,
Koukan will return a URL to PUT the blob to::

    POST /senders/submission/transactions HTTP/1.1
    Content-type: application/json
    {"mail_from": {"m": "alice@example.com"},
     "rcpt_to": {"m": "bob@example.com"}}}}

    201 created
    Location: /transactions/xyz
    Content-type: application/json

    {"mail_from": {}, "rcpt_to": {}, "body": {"blob_status": {
     "uri": "http://router.local/transactions/xyz/body"}}}

    PUT /transactions/xyz/body HTTP/1.1

    200 ok

    GET /transactions/xyz HTTP/1.1

    200 ok

    {"mail_from": {}, "rcpt_to": {}, "body": {"blob_status": {
     "finalized": "true"}}}

Body reuse
----------

Similarly, you can reuse an rfc822 body from a previous transaction::

    POST /senders/submission/transactions HTTP/1.1
    Content-type: application/json

    {"mail_from": {"m": "alice@example.com"},
     "rcpt_to": {"m": "bob@example.com"},
     "body": {"reuse_uri": "/transactions/xyz/body"}
    }

    GET /transactions/xyz HTTP/1.1

    200 ok

    {"mail_from": {}, "rcpt_to": {}, "body": {"blob_status": {
     "finalized": "true"}}}

Cancellation
------------

To cancel a transaction, simply::

    POST /transactions/123/cancel HTTP/1.1

with an empty entity. This is a no-op if the transaction already has
``final_attempt_reason``. This will manifest as ``cancelled: true`` in
the transaction json. Note: cancellation will not abort an inflight
OutputHandler that is waiting on the upstream but will abort on the
next iteration of ``OutputHandler.handle()``.

Receiving
=========

Receiving is a little more complicated due to the need to be
compatible with gatewaying from interactive (non-pipelined) SMTP.

cf examples/receiver

Your application must expose the following routes/endpoints::

    POST /senders/router/transactions HTTP/1.1

    201 created
    Location: /transactions/123

create a new transaction and return the path in location::

    GET /transactions/<tx id> HTTP/1.1


upload the rfc822 message::

    PUT /transactions/<tx id>/body HTTP/1.1


additionally, if you enable message parsing in the output chain::

    PUT /transactions/<tx id>/message_builder HTTP/1.1
    PUT /transactions<tx id>/blob/<blob id> HTTP/1.1

for each blob in the message builder spec json

