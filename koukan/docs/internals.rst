=====================
Koukan Implementation
=====================

Overview
========

Koukan has 2 main components:

* SMTP gateway
  smtp <-> http/json rest protocol
  stateless protocol proxy, no business logic
* router
  http/json rest protocol only
  stateful/store-and-forward

Router
======

Within the router, there are two main flows. RestHandler terminates
http endpoints/fastapi routes and is a thin adapter to
storage. OutputHandler consumes data from storage to drive the output
filter chain which always terminates in RestEndpoint to send the
message to the destination.

The OutputHandler passes the transaction through a filter chain on the
way to RestEndpoint. Filters can make arbitrary transformations of the
transaction including modifying the message, routing on destination
address, etc.

Storage
-------

The Koukan router currently stores all durable data in a SQL database
which it accesses via SQLAlchemy Core. Blob data is stored in a single
field in Blob.content. This can be referenced from multiple
transactions via TransactionBlobRefs.

Koukan uses lightweight in-process synchronization to coordinate
multiple readers/writers of a given transaction:
VersionCache. OutputHandler waits for new downstream data, feeds it to
the upstream chain, and writes upstream responses. RestHandler writes
new downstream data and then waits for upstream responses.




Gateway
=======

In order to be compatible with interactive/non-pipelined SMTP,
gateway->router uses a specialized dialect of the rest protocol to
support building up a transaction incrementally.



Extending Koukan with Filters
=============================
