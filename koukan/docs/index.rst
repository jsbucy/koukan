.. Koukan documentation master file, created by
   sphinx-quickstart on Fri Apr 18 10:43:58 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Koukan documentation
====================

Overview

Email is ubiquitous but also difficult to get right. Within any
computing facility, there is a collection of often highly
miscellaneous things that send email. 



Installation

RestMTP for Applications

Sending

Koukan provides a rich http/json rest api to send and receive email.

RestMTP has 1 type of resource: the transaction, a request to send one
message to one recipient. In the simplest case of a message containing
only text, we can send a message with a single POST:

POST /transactions
Content-type: application/json
{"mail_from": "alice",
 "rcpt_to": "bob",
 "body": {"message_builder: {
     "headers": [["subject", "hello"]],
     "text_body": [{
       "content_type": "text/plain", "content": {"inline": "hello, world!"}}]
}}}

201 created
Location: /transactions/xyz
Content-type: application/json
{"mail_from": {}, "rcpt_to": {}, "body": {}}

RestMTP transactions are write-once; the {} is a placeholder
indicating the field is populated. RestMTP transactions are
long-running operations that track the status of the message
delivery. With a request-timeout header, Koukan will do a hanging GET:

GET /transactions/xyz
request-timeout: 10

(some time elapses)
200 ok
Content-type: application/json
{"mail_from": {}, "rcpt_to": {}, "body": {},
 "mail_response": 250, "rcpt_response": 250, "data_response": 250
}

If you need to send a large or binary attachment, that is done by
specifying an id within the message_builder and then PUTting the blob
to that id:

POST /transactions
Content-type: application/json
{"mail_from": "alice",
 "rcpt_to": "bob",
 "body": {"message_builder: {
     "headers": [["subject", "hello"]],
     "text_body": [{
       "content_type": "text/plain", "content": {"create_id": "my_body"}}]
}}}

201 created
Location: /transactions/xyz

PUT /transactions/xyz/blob/my_body
content-type: text/html
content-length: 12345678

TODO: the api doesn't really expose whether all attachments have been received?

A transaction can reuse an attachment from a previous transaction:

POST /transactions
Content-type: application/json
{"mail_from": "alice",
 "rcpt_to": "bob",
 "body": {"message_builder: {
     "headers": [["subject", "hello"]],
     "text_body": [{
       "content_type": "text/plain", "content": {"reuse_uri": "/transactions/xyz/blob/my_body"}}]
}}}



Finally, suppose you already have a serialized rfc822 payload you want
to send. Simply POST that to /transactions/xyz/body. Similarly, you
can reuse an rfc822 body from a previous transaction:
POST /transactions
Content-type: application/json
{"mail_from": "alice",
 "rcpt_to": "bob",
 "body": {"reuse_uri": "/transactions/xyz/body"}
}



Receiving

Receiving is a little more complicated due to the need to be
compatible with gatewaying from interactive (non-pipelined) SMTP.

cf examples/receiver

Your application must expose the following routes/endpoints:
POST /transactions
create a new transaction and return the path in location:
GET /transactions/<tx id>

POST /transactions/<tx id>/body
upload the rfc822 message

additionally, if you enable receive parsing:
POST /transactions/<tx id>/message_builder
PUT /transactions<tx id>/blob/<blob id>
for each blob in the message builder spec json


Koukan Implementation

Overview

Koukan has 2 main components:
- SMTP gateway
  smtp <-> http/json rest protocol
  stateless protocol proxy, no business logic
- router
  http/json rest protocol only
  stateful/store-and-forward

The router has 2 main control flows
RestHandler: http route -> storage
OutputHandler: storage -> RestEndpoint

The OutputHandler passes the transaction through a filter chain on the
way to RestEndpoint. Filters can make arbitrary transformations of the
transaction including modifying the message, routing on destination
address, etc.

In order to be compatible with interactive/non-pipelined SMTP,
gateway->router uses a specialized dialect of the rest protocol to
support building up a transaction incrementally.



Extending Koukan with Filters


.. toctree::
   :maxdepth: 2
   :caption: Contents:

