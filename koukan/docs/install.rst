======================
Installation/Operation
======================

Koukan supports reasonably recent versions of Python3 >=3.9 for aiosmtpd.

The router and gateway components are configured entirely through yaml
config files. The only command-line parameter they accept is the path
to the config file.

The router buffers some inflight data through the filesystem but
this does not need to be durable; EmptyDir is fine. The smtp gateway
should not require a writable filesystem at all.


SMTP Gateway
============

If you want the Koukan smtp gateway to terminate port 25/587 connections
directly from the public internet, you will need to arrange for it to
be able to bind privileged ports. You can do this with capsh on Linux.

Alternatively, you use a front proxy such as Envoy to terminate port
25 and let the gateway listen on an unprivilged port. Note that Envoy
cannot currently terminate SMTP STARTTLS but there has been some work
in this area `bug`_.

.. _bug: https://github.com/envoyproxy/envoy/issues/19765
