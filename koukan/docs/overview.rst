========
Overview
========

Koukan is an email<->application gateway. Koukan provides a rich
http/json rest api for new-build applications to send and receive
email which includes rfc822/MIME handling, DKIM signing, etc. Koukan acts as
a SMTP MTA/MSA for existing applications.

Koukan consists of two main components: the router and the SMTP
gateway. The router implements the rest api, durable storage, stateful
retries, etc. The SMTP gateway is a stateless adapter to couple SMTP
client and server connections with the rest api.

The Koukan router and gateway are single-processes, multi-threaded
Python3 programs. Koukan utilizes a limited amount of asyncio where it
makes sense.

For scaling and availability, Koukan supports clustering multiple
router and gateway processes sharing the same underlying storage via a
cluster scheduler such as Kubernetes (k8s). In particular the router
stores all durable data in a database and does not assume a
durable/strongly consistent posix filesystem. However the router does
buffer some inflight data through the filesystem but this does not need
to be durable; EmptyDir is fine. The smtp gateway should not require a
writable filesystem at all.

