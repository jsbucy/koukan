========
Overview
========

Koukan is an Cloud-Native email transport stack. Koukan provides a
rich http/json rest api for new-build applications to send and receive
email. The rest api includes rfc822/MIME message formatting, DKIM
signing, etc. The rest api also reports many errors immediately rather
than your application having to receive and handle bounce/NDR messages
after the fact.  Koukan is a clean-sheet full SMTP Mail Transfer Agent
(MTA) for high-fidelity interoperability with as-built internet email
and applications.

Koukan processes messages through a filter chain inspired by `Envoy
<https://envoyproxy.io>`__. Koukan is extensible via a plugin api to
load custom filters. Koukan tries to contain opinionated business
logic within filter modules that can be easily swapped out.

For scaling and availability, Koukan supports clustering multiple
instances sharing the same underlying storage via a cluster scheduler
such as Kubernetes (k8s).


Rest API
--------

Koukan's rest api has a single type of resource: a transaction, a
request to send an email message to one recipient. A transaction is a
long-running operation (LRO) that tracks the delivery status of the
message until it has been delivered or permanently fails. A sender
application can reliably obtain common-case failure/diagnostic
information by watching the LRO rather than having to route bounce
messages back to the application.

The message contents can be specified as an abstract json "message
builder" representation or pre-serialized rfc822. Transactions
can reference previous transactions to reuse file attachments.

Senders
-------

Senders are principals/roles for sending messages and the unit of
configuration and access control. For example you might have a sender
for each sending application plus a few "system" senders such as
"ingress" for incoming messages from the public internet. Message
processing is configured on a sender basis in particular which output
flow/filter chain to use.

There is an additional per-sender tag to customize parameters for
different message flows within an application.

Koukan does not have any built-in client authentication. However the
sender is part of the url path so you can use a front proxy to control
access to senders by filtering by path prefix like any other rest api.


Output Flows/Filter Chains
--------------------------

Koukan is configured with one or more output flows which determines
how a message is processed. Most of the action in the output flow
happens in the filter chain. The filter chain is a sequence of filter
invocations that are applied to each transaction. The filters are
actions such as "add received header". User-provided filter plugins
can be loaded in the ``modules.sync_filter`` stanza.

Koukan Software Stack
---------------------

Koukan consists of two components: the router and the SMTP
gateway. The router implements the rest api, durable storage, stateful
retries, etc. The SMTP gateway is a stateless protocol proxy to bridge
SMTP client and server connections with the rest api.

The router and gateway are self-contained, long-running/non-forking
python3 programs that embed uvicorn and aiosmtpd to receive smtp and
http, respectively. The router stores durable data in a database
accessed via SQLAlchemy2 Core. Koukan uses vanilla SQL and does not
depend on non-portable features such as change
notifications. PostgreSQL and SQLite are actively tested, others
should be straightforward to add. The router buffers some data through
the local filesystem but this does not need to be durable.

There is no intrinsic connection between the smtp server and client
sides of the gateway; you can run separate instances for each function
for isolation, etc.

Message Flows
-------------

→ denotes http/json rest api

🠊 denotes smtp

Rest Sender Application
=======================

application → router → smtp gw 🠊 internet

Rest Receiver Application
=========================

internet 🠊 smtp gw → router → application

SMTP Sender Application
=======================

application 🠊 smtp gw → router → smtp gw 🠊 internet

SMTP Receiver Application
=========================

internet 🠊 smtp gw → router → smtp gw 🠊 application

Drilling down into the Router
-----------------------------

within the router, there are 2 flows:

Input/Downstream
================

fastapi route → RestHandler → StorageWriterFilter → Storage (sqlalchemy)

Output/Upstream
===============

Storage → OutputHandler → FilterChain → ... → http/json rest output (RestEndpoint)

Exploder
========

However we need to accommodate multi-rcpt transactions from the smtp
gateway so we add an internal hop through the Exploder to fan these
out:

RestHandler → ...Storage

Storage → OutputHandler → FilterChain → ...Exploder → Storage

Storage → OutputHandler → ...

where the Exploder fans out a separate upstream transaction for reach
rcpt of the downstream transaction and fans the upstream responses
back in.

Config Walkthrough
------------------

Let's walk through the example configs.

Terminology
===========

* ingress: receiving messages that originated "somewhere else"
* submission: sending messages "for the first time" that originated
  within your site/organization. Commonly referred to as msa for
  Mail/Message Submission Agent.
* downstream: the direction of the client
* upstream: the direction of the server

Gateway
=======

`gateway.yaml <https://github.com/jsbucy/koukan/blob/63138439e666dc6cd12e6dcb6c2497bc29bc2112/config/local-test/gateway.yaml>`__

Starting with the gateway, we need to configure 2 flows: smtp →
router/rest and vice versa.

``smtp_listener`` configures the gateway to listen on separate tcp ports for
smtp ingress (port 25) and submission (port 587). The endpoint selects
from the following rest_output stanza and sender/tag control the rest
requests sent to the router. The ``rest_output`` stanza contains the
urls/endpoints for each sender on the router.

In the opposite direction, ``rest_listener`` sets up a port to receive
rest/http from the router. **update** the gateway currently accepts
any rest sender and uses the tag to select the smtp_output stanza.

Router
======

`router.yaml <https://github.com/jsbucy/koukan/blob/63138439e666dc6cd12e6dcb6c2497bc29bc2112/config/local-test/router.yaml>`__

There is a little more going on in the router.

As before, ``rest_listener`` sets up a port to receive rest/http from the
gateway and first-class rest senders.

At a minimum, you will have a ``sender`` for ingress and submission. Depending
on your environment, you will probably create a separate sender for
each rest sending application. The sender/tag selects the output_chain
which is the key into the following endpoint stanza.

The output flow (``endpoint`` stanza) configures the set of steps to
process a message. Transactions originating as smtp must be handled by
a chain that ends with ``exploder`` to fan-out multiple smtp
recipients. This in turn selects another (single-recipient) output
flow that ends with ``rest_output`` to send rest/http to the gateway or
other receiving application.

A common operation is to route messages by recipient address. This is
done by RecipientRouterFilter called simply ``router`` in the output chain
yaml. Ultimately, this is going to mutate the in-process transaction
state to influence where rest_output sends the transaction.

Each instance of RecipientRouterFilter is configured with a
RoutingPolicy. There are 2 basic policies. ``address_list`` selects a
set of addresses to send to a particular destination and will
typically be used in the ingress chain. ``dest_domain`` sends all
messages to the domain part of the destination address in conjunction
with dns/mx resolution and is typically used in the submission chain.

Like Filters, user-provided RoutingPolicies can be loaded in the
``modules.recipient_router_policy`` stanza.

Multiple instances of recipient router may be chained. The first
filter to match the message determines the destination and subsequent
filters no-op.

A few other things to point out in the example config:

In the ingress chains, there is a final "fallthrough" recipient router
to reject addresses that didn't match any previous filter. There is a
second instance of the recipient router filters in the
ingress_exploder chain with ``dry_run: true``. This is to reject
addresses prior to starting the upstream chain.

In the submission chain, the first recipient router filter matches our
own domains to short-circuit directly to our ingress so we have
router → router instead of router → gateway 🠊 gateway → router.

Finally, recipient router filter yaml ``destination`` selects within the
``rest_endpoint`` stanza.
