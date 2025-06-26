=====================
Koukan Implementation
=====================

Overview
========

Koukan has 2 main components:
- SMTP gateway
  smtp <-> http/json rest protocol
  stateless protocol proxy, no business logic
- router
  http/json rest protocol only
  stateful/store-and-forward

Within the router, there are two main flows. RestHandler terminates
http endpoints/fastapi routes and is a thin adapter to
storage. OutputHandler consumes data from storage to drive the output
filter chain which always terminates in RestEndpoint to send the
message to the destination.

The OutputHandler passes the transaction through a filter chain on the
way to RestEndpoint. Filters can make arbitrary transformations of the
transaction including modifying the message, routing on destination
address, etc.

In order to be compatible with interactive/non-pipelined SMTP,
gateway->router uses a specialized dialect of the rest protocol to
support building up a transaction incrementally.



Extending Koukan with Filters
=============================
