=============
Configuration
=============

Router
======

global: executor/thread limits

rest_listener: host/port/certs

sender:

endpoint: output chains, cf below

rest_endpoint:

modules: user plugins: output chain filters, recipient routing policies

storage: sqlalchemy db url

logging: ``logging.dictConfig`` 

Gateway
=======

global: cf router

rest_listener: cf router

rest_output: http endpoint of router and host for output chain

smtp_output: rest host endpoints for the router to send to

smtp_listener: host/port/certs, specify which rest_output host to use

logging: ``logging.dictConfig`` 



Router Endpoints/Output Chains
==============================

name: client selects this in http host: header

rest_lro: false if this ends with exploder

msa: true if this is terminating smtp msa port 587 from clients
injecting messages for the first time, enables store&forward in more
circumstances, further details in internals/exploder

output_handler:
  retry_params:
    mode: per_request if exploder
  notification:
    mode: per_request if exploder

    host: endpoint to inject dsn/bounce messages into

chain: list of filters

Output Chain Filters
--------------------

remote_host: resolves tx.remote_host ip to name

message_builder: renders/serializes tx.body message builder request json to rfc822 (for sending)

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


You will typically have 1 endpoint for each smtp vip + port e.g. mx and msa.
Any endpoint that terminates smtp will end with the exploder.
There will be an accompanying "exploder upstream" endpoint/chain
with "per_request" retry/notify settings.
There will also be an endpoint for direct rest clients.
So a simple config might consist of::

    endpoint:
    - name: smtp-mx
      chain:
      # ...
      - filter: exploder
        msa: false
        output-chain: smtp-mx-upstream
    - name: smtp-mx-upstream
      output_handler:
        notification:
          host: submission
        retry_params:
      chain:
      # ...
      - filter: rest_output

msa is similar to mx but enables store&forward on the exploder in more
cases with ``msa: true``

rest clients that take full advantage of RestMTP LROs don't need notifications::

    - name: submission
      output_handler:
        retry_params:
      chain:
      # ...
      - filter: rest_output


Note that the output chain is linear. ``recipient_router_filter``
routes on recipient by setting fields in the transaction to influence
the http endpoint that ``rest_output`` sends to and if that is the smtp
gateway, what destination the gateway sends to after that. A typical
ingress config would route known domains and reject everything
else. Whereas an egress config might special-case internal domains and
then send everything else to the RHS of the address.


cluster/k8s
===========

All replicas share the same underlying database.

The router implementation currently buffers data through the local
filesystem but this does not need to be durable across restarts;
emptyDir is fine. This is local to each router process; the router and
gateway do not share data through the filesystem.

Koukan may return http redirects in response to requests to endpoints
with rest_lro enabled; native rest clients must be prepared to follow
these.

For both router and gateway, configure rest_listener.session_uri to
point to the dns alias or ip of the individual pod/replica. For
router, configure rest_listener.service_uri to the router service dns
alias. Set endpoint.rest_lro to false for endpoints that the gateway
injects into and true to endpoints that native rest clients use.
