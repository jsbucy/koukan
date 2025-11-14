=============
Configuration
=============

Gateway
=======

Example: `gateway.yaml <https://github.com/jsbucy/koukan/blob/a9e58dfee15a4bf26e723f97dc5d7a6052b15fe6/config/local-test/gateway.yaml>`__

global: executor/thread limits

rest_listener: host/port/certs for http

rest_output: http endpoint of router and host for output chain

smtp_output: rest host endpoints for the router to send to

smtp_listener: host/port/certs, specify which rest_output host to use

logging: ``logging.dictConfig`` 

Router
======

Example: `router.yaml <https://github.com/jsbucy/koukan/blob/a9e58dfee15a4bf26e723f97dc5d7a6052b15fe6/config/local-test/router.yaml>`__

global: executor/thread limits

rest_listener: host/port/certs for http

sender: principals/roles for sending messages, cf below

endpoint: output chains, cf below

rest_endpoint: referenced from recipient routing destination

modules: user plugins: output chain filters, recipient routing policies

storage: sqlalchemy db url

logging: ``logging.dictConfig`` 

Senders
-------

name: rest url path ``/senders/<name>/transactions``

output_chain: how to handle messages for this sender

retry: ``output_chain`` to use the parameters from the output chain or ``null`` to disable

notification: cf retry

upstream_sender/upstream_tag: exploder rewrites sender/tag to this

tag: per-tag parameter overrides


Output Chains
-------------

name: referenced by senders

rest_lro: false if this ends with exploder

msa: true if this is terminating smtp msa port 587 from clients
injecting messages for the first time, enables store&forward in more
circumstances, further details in internals/exploder

output_handler:
  retry_params:

    max_attempts: maximum number of retries

    min_attempt_time: minimum time from completion of previous attempt to start of next attempt, seconds

    max_attempt_time: maximum time from completion of previous attempt to start of next attempt, seconds

    backoff_factor: exponential backoff factor

    deadline: maximum wall clock time to retry message

  notification:
    sender/tag to inject notifications/DSNs

chain: list of filters

The output chain is linear. ``recipient_router_filter`` routes on
recipient by setting fields in the transaction to influence the http
endpoint that ``rest_output`` sends to and if that is the smtp
gateway, what destination the gateway sends to after that.

A typical ingress config would accept local domains and reject
everything else. Whereas an egress config might short-circuit local
domains back to ingress and then send everything else to the RHS of
the address.

Output Chain Filters
--------------------

remote_host: resolves tx.remote_host ip to name

message_builder: renders/serializes tx.body message builder request
json to rfc822 (for sending)

message_parser: parses rfc822 tx.body to message builder json (for receiving)

received_header: prepends Received: header to tx.body

dkim: signing

dns_resolution: replaces tx.resolution containing a hostname with one
containing a list of IP addresses for the gateway to attempt in order

router: RecipientRouterFilter populates tx.rest_upstream_sender which controls
where RestEndpoint sends it and if that is the gateway, tx.resolution
controls where the gateway sends it

relay_auth: fails the tx if it doesn't contain smtp auth info in
smtp_meta or remote_host in allowlist

exploder: Exploder, output_chain configures the upstream chain, ``msa:
true`` allows store&forward in a few more situations that clients
expect vs ingress where there is a previous hop to retry.

rest_output: RestEndpoint actually sends the message somewhere via
http/rest


Cluster/k8s
===========

All replicas share the same underlying database.

The router implementation currently buffers data through the local
filesystem but this does not need to be durable across restarts;
emptyDir is fine. This is local to each router process; the router and
gateway do not share data through the filesystem.

Koukan may return http redirects in response to requests to endpoints
with rest_lro enabled; native rest clients must be prepared to follow
these.

For both router and gateway, configure ``rest_listener.session_uri`` to
point to the dns alias or ip of the individual pod/replica. For
router, configure ``rest_listener.service_uri`` to the router service dns
alias. Set ``endpoint.rest_lro`` to false for endpoints that the gateway
injects into and true to endpoints that native rest clients use.
