===============
Koukan REST API
===============

Sending
=======

Koukan provides a rich http/json rest api to send and receive email.

RestMTP has 1 type of resource: the transaction, a request to send one
message to one recipient. The transaction object contains fields
corresponding to smtp parameters.

mail_from: Mailbox

mail_response: Response

rcpt_to: Array[Mailbox]

rcpt_response: Array[Response]

first-class rest api users will only use 1
rcpt_to/rcpt_response. (only the smtp gateway uses multiple)

body: MessageBuilder request

data_response: Response

remote_host: HostPort

local_host: HostPort

HostPort:

host: string

port: int

Mailbox:

m: rfc5321 mailbox without <>

e: Array[EsmtpParam]

EsmtpParam:

keyword: string

value: string (optional)

Response:

code: int

message: string

In the simplest case of a message containing
only text, we can send a message with a single POST::

    POST /transactions
    Content-type: application/json
    {"mail_from": {"m": "alice@example.com"},
     "rcpt_to": {"m": "bob@example.com"},
     "body": {"message_builder: {
         "headers": [["subject", "hello"]],
         "text_body": [{
           "content_type": "text/plain", "content": {"inline": "hello, world!"}}],
    }}}

    201 created
    Location: /transactions/xyz
    Content-type: application/json
    {"mail_from": {}, "rcpt_to": {}, "body": {}}

RestMTP transactions are write-once; the ``{}`` is a placeholder
indicating the field is populated. RestMTP transactions are
long-running operations (lro) that track the status of the message
delivery. With a request-timeout header, Koukan will do a hanging GET::

    GET /transactions/xyz
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

If you need to send a large or binary attachment, that is done by
specifying an id within the message_builder spec and then PUTting the blob
to that id::

    POST /transactions
    Content-type: application/json
    {"mail_from": {"m": "alice@example.com"},
     "rcpt_to": {"m": "bob@example.com"},
     "body": {"message_builder: {
         "headers": [["subject", "hello"]],
         "text_body": [{
           "content_type": "text/html", "content": {"create_id": "my_body"}}]
    }}}

    201 created
    Location: /transactions/xyz

    PUT /transactions/xyz/blob/my_body
    content-type: text/html
    content-length: 12345678

TODO: the api doesn't really expose whether all attachments have been received?

A transaction can reuse an attachment from a previous transaction::

    POST /transactions
    Content-type: application/json
    {"mail_from": {"m": "alice@example.com"},
     "rcpt_to": {"m": "bob@example.com"},
     "body": {"message_builder: {
         "headers": [["subject", "hello"]],
         "text_body": [{
           "content_type": "text/plain",
           "content": { "reuse_uri": "/transactions/xyz/blob/my_body"}
         }]
    }}}

If you already have a serialized rfc822 payload you want
to send, simply PUT that to /transactions/xyz/body. Similarly, you
can reuse an rfc822 body from a previous transaction::

    POST /transactions
    Content-type: application/json
    {"mail_from": {"m": "alice@example.com"},
     "rcpt_to": {"m": "bob@example.com"},
     "body": {"reuse_uri": "/transactions/xyz/body"}
    }

To abort an inflight transaction, simply::

    POST /transactions/123/cancel

with an empty entity. This will manifest as ``cancelled`` in the
transaction json.

Receiving
=========

Receiving is a little more complicated due to the need to be
compatible with gatewaying from interactive (non-pipelined) SMTP.

cf examples/receiver

Your application must expose the following routes/endpoints::

    POST /transactions

    201 created
    Location: /transactions/123

create a new transaction and return the path in location::

    GET /transactions/<tx id>


upload the rfc822 message::

    PUT /transactions/<tx id>/body


additionally, if you enable receive parsing::

    PUT /transactions/<tx id>/message_builder
    PUT /transactions<tx id>/blob/<blob id>

for each blob in the message builder spec json

