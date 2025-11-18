=====================
Koukan Implementation
=====================

Overview
========

Koukan is designed around the premise that in 2025, network
availability is pretty good most of the time. So in the common case,
we should be able to get a final upstream response quickly and can
return that to the sender immediately. This is known as cut-through
delivery. Contrast with the classical store&forward model where you
spool to disk and deal with output "later," sometimes much
later. Store&forward entails sending a bounce message after the fact if the
message couldn't be delivered upstream which may never get back to the
sender.

Koukan consists of two components: the router and the SMTP
gateway. The router implements the rest api, durable storage, stateful
retries, etc. The SMTP gateway is a stateless adapter to proxy
SMTP client and server connections with the rest api.

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

Transaction Model
-----------------

The in-process representation of the Transaction resource is
``TransactionMetadata`` which is usually abbreviated tx in the code.

Many operations are defined in terms of taking a difference or delta
between a previous snapshot of the transaction and the current state
or applying a delta to a transaction.

Filter Chain
------------

Filters transform the transaction. In the simplest case, they
conservatively extend the transaction by adding fields. In more
complex cases, they may implement an arbitrary transformation by
proxying between the upstream and downstream side (ProxyFilter).

When the router starts an OutputHandler, it constructs a sequence of
filter subclasses per the yaml. FilterChain is the execution engine
to propagate the transaction through the sequence of filters.

The chain is always terminated by a filter that sends the transaction
"somewhere else". This is typically either

*  RestEndpoint to send it over http
*  Exploder to write it to a new upstream transaction via StorageWriterFilter.


Storage
-------

The Koukan router stores all durable data in a SQL database which it
accesses via SQLAlchemy Core. Blob data is stored in a single field in
Blob.content. This can be referenced from multiple transactions via
TransactionBlobRefs.

Koukan uses lightweight in-process synchronization to coordinate
multiple readers/writers of a given transaction:
VersionCache. OutputHandler waits for new downstream data, feeds it to
the upstream chain, and writes upstream responses. RestHandler writes
new downstream data and then waits for upstream responses.

Exploder
--------

The router output flow generally assumes single-recipient. SMTP has
always supported multi-recipient transactions and some old MTAs may
not gracefully handle the server rejecting additional recipients. To
support this in Koukan, the smtp gateway uses a private dialect of the
rest api to build up the transaction incrementally. To bridge the gap
with the single-recipient output flow, smtp-facing endpoints are
terminated by the Exploder. The Exploder starts a separate upstream
transaction for each recipient of the smtp transaction. The body
storage is refcounted in the database.

Whereas in the native rest case we have::

  application -> RestHandler -> StorageWriterFilter -> db
  db -> OutputHandler -> ... -> RestEndpoint -> gateway

with SMTP we have::

  gateway -> RestHandler -> StorageWriterFilter -> db
  db -> OutputHandler -> Exploder -> StorageWriterFilter -> db
  db -> OutputHandler -> ... -> RestEndpoint...

While SMTP supports multi-recipient transactions, it does not support
returning a different final response for each recipient.  Koukan's
goal here is to return an authoritative upstream response
synchronously and avoid accept-and-bounce to the greatest extent
possible.

The Exploder accomplishes this by attempting *opportunistic
cut-through* and falling back to store&forward if this is not
possible. It waits for a short time (relative to an smtp command
timeout, say 10-30s) for an upstream response. Then if all upstream
data responses are the same, that is returned directly. Otherwise,
retry/bounce is enabled on the upstream transactions that didn't
already succeed. At this point, the downstream smtp transaction is
durable and it's safe to return a 250 data/final response.

There is a further distinction between submission vs
ingress. Submission stores-and-forwards temporary upstream errors
since the downstream is a client that usually expects us "the server"
to retry. Whereas ingress returns temporary errors downstream directly
since the client is another MTA that is prepared to retry.

.. list-table::
  :header-rows: 1


  * -
    - submission
    - ingress
  * - recipient
    - timeout: upgrade to 250

      temp/4xx: upgrade to 250

      perm/5xx: verbatim
    - timeout: 450

      temp/4xx: verbatim

      perm/5xx: verbatim

  * - data/final
    - timeout: upgrade to 250

      temp/4xx: upgrade to 250

      perm/5xx: verbatim
    - all errors: verbatim



Gateway
=======

There is no intrinsic connection between the smtp server and client
sides of the gateway; you can run separate instances for each function
for isolation, etc.

Smtp Server
-----------

aiosmtpd → SmtpHandler → RestEndpoint

Smtp Client
-----------

RestHandler → SyncFilterAdapter → SmtpEndpoint

``SyncFilterAdapter`` implements the same ``AsyncFilter`` abc as
``StorageWriterFilter``.



