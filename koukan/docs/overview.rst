========
Overview
========

Koukan is an email<->application gateway. Koukan provides a rich
http/json rest api for new-build applications to send and receive
email which includes rfc822/MIME handling, DKIM signing, etc. Koukan
is a clean-sheet full SMTP MTA/MSA implementation for robust
interoperability with as-built internet email and applications.

What distinguishes Koukan from conventional forking Unix MTAs is that
it is modeled on Envoy to proxy to the destination and return an
authoritative upstream response synchronously in the common
case. Koukan minimizes use of store-and-forward. This reduces the
number of situations where a message bounces after the fact, a major
source of mysterious flakiness.

Koukan consists of two components: the router and the SMTP
gateway. The router implements the rest api, durable storage, stateful
retries, etc. The SMTP gateway is a stateless adapter to interconnect
SMTP client and server connections with the rest api.

The Koukan router and gateway are single-process/non-forking Python3
programs.

For scaling and availability, Koukan supports clustering multiple
router and gateway processes sharing the same underlying storage via a
cluster scheduler such as Kubernetes (k8s). In particular the router
stores all durable data in a database and does not assume a
durable/strongly consistent posix filesystem.

Rest API
--------

Koukan's rest api is called RestMTP. It has a single type of resource:
a transaction, a request to send a message to one recipient. A RestMTP
transaction is a long-running operation (LRO) that tracks the delivery
status of the message until it has been delivered or fails. This way,
a RestMTP sender application only needs to save the transaction id to
reliably obtain failure/diagnostic information rather than having to
route bounce messages back to the application.

The message contents can be specified as an abstract json "message
builder" representation or pre-serialized rfc822. File attachments in
the message builder specification and rfc822 messages are treated as
blobs. RestMTP transaction creation requests can reuse blobs by
referencing previous transactions.

Message Flows
-------------

-> denotes http/json rest api "restmtp"

=> denotes smtp

Rest Sender Application

application -> router -> smtp gw => internet

Rest Receiver Application

internet => smtp gw -> router -> application

SMTP Sendeer Application

application => smtp gw -> router -> smtp gw => internet

SMTP Receiver Application

internet => smtp gw -> router -> smtp gw => application

Endpoints
---------

A key concept in Koukan is the endpoint which is passed in the http
host: header and used to select between multiple configured processing
flows. The gateway uses a private dialect of the RestMTP for
compatibility with smtp. So typically requests from the smtp gw will
use different endpoints vs those from first-class rest clients. Also,
initial submission will usually use distinct endpoints from internet
interchange. So each of the above examples would use distinct router
endpoints.





