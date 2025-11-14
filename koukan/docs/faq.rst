===
FAQ
===

General
=======

Why did you call it Koukan?
---------------------------

Definition at `jisho.org <https://jisho.org/word/交換>`__,
think “interchange”

Why wouldn’t I just use Sendgrid/SaaS?
--------------------------------------

Email Service Providers (ESPs) offer tremendous value for many important
use cases. If you are running a public-facing application, e-commerce
site, etc that needs to ensure deliverability of e-mail to every mailbox
provider in the world, that is what you should be using.

If you are sending mail within your own organization/site, SaaS may make
less sense. You may have a collection of back-of-house applications
that send a small amount of traffic to a small number of destinations,
maybe just your own domain. You might be delivering operational
mail/alerts to a mailbox server within the same cluster. In these cases,
you want to deploy something locally and forget about it.

Am I the target audience for Koukan?
------------------------------------

Yes
~~~

- You have an application that sends/receives email and don't like your current
  solution such that you're interested in porting it to a new email api.
- You have have experience running an MTA on the public internet.
- You have some experience with http/rest apis.
- You're willing to get your hands dirty with Python.

No
~~

-  You want to deliver mail to many local users on a timesharing system.
-  You need extreme performance/scalability.
-  You need robust integrated spam/phishing/antivirus out of the box.

How mature is Koukan?
---------------------

I have been running Koukan on the public internet since 2024. It
sees much more spam probing or `internet background radiation
<https://en.wikipedia.org/wiki/Internet_background_noise>`__ than
actual traffic. I do not see crashes or uncaught exceptions.

All of the nitty-gritty protocol codec work is done in mature
libraries: uvicorn/httpx for http, aiosmtpd/smtplib for smtp,
email.message for rfc822/mime.


How does Koukan relate to Unix mailers like Exim and Postfix?
-------------------------------------------------------------

To use an analogy, if Exim and Postfix are Apache httpd, Koukan is
Envoy.  Exim and Postfix both started life in the 1990s as
replacements for Sendmail for handling email for interactive users of
unix timesharing systems.

Koukan does not deliver messages by spawning other programs or writing
to files in the filesystem, it only connects to a socket. So you need
something to listen on a socket to receive messages from Koukan.

Implementation
==============

Why did you use Python?
-----------------------

* Python has the most complete and up-to-date suite of mature
  implementations of core email standards, in particular the
  rfc822/MIME codec, SMTP and domainkeys that saved a ton of time not
  to write from scratch.
* It remains to be seen if the (perceived) performance limitations of
  Python will be a factor in practice for use cases that are a good fit
  for Koukan. It is possible by being smart about memory and forking and
  with horizontal scaling on k8s that the current Python
  implementation can be “scalable enough” for many use cases.

Why don’t you use <my favorite framework/middleware/…>?
-------------------------------------------------------

* We see Koukan as existing near the bottom of the cluster tech
  stack. Koukan/email "is a" stateful middleware thing rather than an
  application built on top of something like Kafka or Pulsar. Email
  has fairly specific semantics and I'm simply not familiar enough with
  any of those technologies to be confident that they're a good
  fit. You might want to use Koukan to send production monitoring
  alerts and having a bunch of backend dependencies undercuts this.

* We have designed Koukan to be extensible so many of these
  integrations could be done through plugins:

  * routing to recipients via LDAP or another database
  * delivering messages or decoded attachments to Swift or another
    cloud storage api

Deployment
==========

Can I run Koukan on Kubernetes or other multi-node/cluster environment?
-----------------------------------------------------------------------

YES!

All replicas share the same underlying database. The current
implementation buffers data through the local filesystem but this does
not need to be durable across restarts; ``emptyDir`` is fine. This is
local to each router process; the router and gateway do not share data
through the filesystem.

Koukan may return http redirects in response to requests to endpoints
with rest_lro enabled; native rest clients must be prepared to follow
these.

Can I use Envoy as a front proxy for the Koukan SMTP Gateway?
-------------------------------------------------------------

Great idea! STARTTLS is a blocker for most use cases, upvote `this
bug <https://github.com/envoyproxy/envoy/issues/19765>`__.

Features/Roadmap
================

What about spam filtering?
--------------------------

We plan to work on this in the near future. Design sketch:

- filters in the chain add "signals" to the transaction e.g.

  - spf/dkim results
  - rfc822/mime parse defects

- at the end of the chain, a "decision script" applies a policy as a
  function of the signals e.g.

  - if the message failed dkim and isn't from an allowlist of my
    trusted partners, then serve an smtp 5xx response


Gateway/Edge
------------

Long-term, the smtp gateway could probably be replaced with an
`Envoy filter <https://github.com/envoyproxy/envoy/issues/9133>`__
