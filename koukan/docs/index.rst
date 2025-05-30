.. Koukan documentation master file, created by
   sphinx-quickstart on Fri Apr 18 10:43:58 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Koukan documentation
====================

Overview

Koukan is an email<->application gateway. Koukan provides a rich
http/json rest api for new-build applications to send and receive
email which includes rfc822/MIME handling, DKIM signing, etc. Koukan acts as
a SMTP MTA/MSA for existing applications.


Installation

Configuration

Endpoints/Output Chains
you will typically have 1 endpoint for each smtp vip + port e.g. mx and msa
any endpoint that terminates smtp will end with the exploder
there will be an accompanying "exploder upstream" endpoint/chain
with "per_request" retry/notify settings
there will also be an endpoint for direct rest clients
so a simple config might consist of
endpoint:
- name: smtp-mx
  chain:
  # ...
  - filter: exploder
    msa: false
    output-chain: smtp-mx-upstream
- name: smtp-mx-upstream
  output_handler:
    notification:
      host: submission
    retry_params:
  chain:
  # ...
  - filter: rest_output

msa is similar to mx but enables store&forward on the exploder in more
cases with msa: true

rest clients that take full advantage of RestMTP LROs don't need notifications:

- name: submission
  output_handler:
    retry_params:
  chain:
  # ...
  - filter: rest_output

Note that the output chain is linear. Koukan routes on recipient by
setting fields in the transaction to influence the next hop that
rest_output sends to and if that is the smtp gateway, what destination
the gateway sends to after that. A typical ingress config would route
known domains and reject everything else. Whereas an egress config
might special-case internal domains and then send everything else to
the RHS of the address.


cluster/k8s


RestMTP for Applications

Sending

Koukan provides a rich http/json rest api to send and receive email.

RestMTP has 1 type of resource: the transaction, a request to send one
message to one recipient. In the simplest case of a message containing
only text, we can send a message with a single POST:

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

RestMTP transactions are write-once; the {} is a placeholder
indicating the field is populated. RestMTP transactions are
long-running operations (lro) that track the status of the message
delivery. With a request-timeout header, Koukan will do a hanging GET:

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

final_attempt_reason is a human-readable string that if non-null
indicates that Koukan is done with this transaction. Completed
transactions are garbage-collected after a configured interval
(e.g. 1h) from the time they were completed.

If you need to send a large or binary attachment, that is done by
specifying an id within the message_builder spec and then PUTting the blob
to that id:

POST /transactions
Content-type: application/json
{"mail_from": {"m": "alice@example.com"},
 "rcpt_to": {"m": "bob@example.com"},
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
{"mail_from": {"m": "alice@example.com"},
 "rcpt_to": {"m": "bob@example.com"},
 "body": {"message_builder: {
     "headers": [["subject", "hello"]],
     "text_body": [{
       "content_type": "text/plain",
       "content": { "reuse_uri": "/transactions/xyz/blob/my_body"}
     }]
}}}



Finally, suppose you already have a serialized rfc822 payload you want
to send. Simply POST that to /transactions/xyz/body. Similarly, you
can reuse an rfc822 body from a previous transaction:
POST /transactions
Content-type: application/json
{"mail_from": {"m": "alice@example.com"},
 "rcpt_to": {"m": "bob@example.com"},
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

PUT /transactions/<tx id>/body
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

