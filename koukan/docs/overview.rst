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




Message Flows
-------------

-> denotes http/json rest api "restmtp"

=> denotes smtp

Rest Application

application -> router -> smtp gw => internet

Rest Receiver

internet => smtp gw -> router -> application

SMTP Sender

application => smtp gw -> router -> smtp gw => internet

SMTP Receiver

internet => smtp gw -> router -> smtp gw => application

A key concept in Koukan is the endpoint which is passed in http host:
headers and used to select between multiple configured processing
flows. In general, REST requests from the smtp gw will use different
endpoints vs those from first-class rest clients. Initial submission
will usually use distinct endpoints from internet interchange. So each
of the above examples would use distinct router endpoints.




