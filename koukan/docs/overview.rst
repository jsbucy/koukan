========
Overview
========

Koukan is an email<->application gateway. Koukan provides a rich
http/json rest api for new-build applications to send and receive
email which includes rfc822/MIME handling, DKIM signing, etc. Koukan
is a clean-sheet full SMTP MTA/MSA implementation for robust
interoperability with as-built internet email and applications.

What distinguishes Koukan from conventional forking Unix MTAs is that
it is modeled on Envoy to proxy to the destination and return an authoritative
upstream response synchronously in the common case. Store-and-forward
is only used as a fallback. This reduces the number of situations
where a message bounces after the fact, a major source of mysterious
flakiness.

Koukan consists of two main components: the router and the SMTP
gateway. The router implements the rest api, durable storage, stateful
retries, etc. The SMTP gateway is a stateless adapter to couple SMTP
client and server connections with the rest api.

The Koukan router and gateway are single-process/non-forking Python3
programs.

For scaling and availability, Koukan supports clustering multiple
router and gateway processes sharing the same underlying storage via a
cluster scheduler such as Kubernetes (k8s). In particular the router
stores all durable data in a database and does not assume a
durable/strongly consistent posix filesystem.
