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

rest_endpoint: list of endpoints referenced by recipient router filter
destination (cf below)

endpoint.name: referenced by recipient routing destination endpoint

endpoint.endpoint: url

endpoint.sender: must match endpoint url path

endpoint.tag: sender tag to send upstream

endpoint.options: cf recipient router filter (below)

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

.. _remote_host_filter:

remote_host
^^^^^^^^^^^
resolves tx.remote_host ip to name

RemoteHostFilterOutput.match() yaml

fcrdns: bool  forward-confirmed reverse dns for remote_host

ehlo_alignment: bool  ehlo aligns to rdns of remote_host

message_builder
^^^^^^^^^^^^^^^
renders/serializes tx.body message builder request
json to rfc822 (for sending)

message_parser
^^^^^^^^^^^^^^
parses rfc822 tx.body to message builder json (for receiving)

received_header
^^^^^^^^^^^^^^^
prepends Received: header to tx.body

dkim_sign
^^^^^^^^^
domain-keys identified mail message signing

filter yaml:

key: private key

domain: domain to sign as d=

selector: selector to sign as s=

dns_resolution
^^^^^^^^^^^^^^
replaces tx.resolution containing a hostname with one
containing a list of IP addresses for the gateway to attempt in order

router
^^^^^^
RecipientRouterFilter populates tx.rest_upstream_sender which controls
where RestEndpoint sends it and if that is the gateway, tx.resolution
controls where the gateway sends it

filter yaml:

policy: routing policy options

policy.name: dest_domain | address_list or pluggable via
modules.recipient_router_policy

dest_domain is used for submission/egress to send to the rhs of the address

address_list is used for ingress to enumerate endpoints for local addresses

policy.endpoint: keys into top-level rest_endpoint

policy.options: updates rest_endpoint options.

policy.options.receive_parsing: required for message_parser to parse
the message for rest receivers

policy.options.send_filter_output: enables sending tx.filter_output
(cf :ref:`signals` below) to receivers. Default false. Rest
receivers may or may not want this but it should be disabled for the
smtp gateway and short-circuiting.

Note: address_list_policy lists are also available with a
``TransactionMatcher`` interface ``matcher: address_list`` for use
with policy_action.

exploder
^^^^^^^^
filter_yaml:

output_chain configures the upstream chain

msa: if true allows store&forward in a few more situations that clients
expect vs ingress where there is a previous hop to retry.

rest_output
^^^^^^^^^^^
RestEndpoint actually sends the message somewhere via http/rest

policy_action
^^^^^^^^^^^^^
cf :ref:`signals` below

.. _spf_check_filter:

spf_check
^^^^^^^^^
sender policy framework host verification

filter yaml:

domains: list of additional domains to check. Some inbound gateway
setups use spf to publish/discover egress IPs.

SpfCheckFilterOutput.match() yaml

mail_from_result: temperror | spf_pass | permerror | fail | softfail | none | neutral

extra_domain: domain from filter yaml domains

extra_domain_result: same values as mail_from_result

.. _dkim_check_filter:

dkim_check
^^^^^^^^^^
domain-keys identified mail signature verification

DkimCheckFilterOutput.results is a list of
DkimCheckFilterOutput.Result which contains details on each dkim
signature in the message in order

DkimCheckFilterOutput.match() matches if *any* signature matches the
given criteria

matcher yaml:

status: temp_err | dkim_pass (default) | fail | unknown_algo

alignment: domain | same_sld (default) | other
applied inclusively i.e. same_sld also matches domain

domains: mutually exclusive with alignment. List of specific domains to check.


.. _message_validation_filter:

message_validation
^^^^^^^^^^^^^^^^^^
parses the message and reports rfc822/mime problems in FilterOutput

MessageValidationFilterResult.match() yaml

validity_threshold : NONE | BASIC | MEDIUM | HIGH

matches if the status is at least as good as this. MEDIUM is the
suggested default for ingress and HIGH for submission. MEDIUM requires
that the headers are reasonably well-formed and contain exactly 1
from, date, message-id. HIGH requires no defects reported by
``email.parser``.

max_received_headers: matches if the message has more than this many
received headers


.. _signals:

Signals&Policies
----------------

Koukan provides a simple yet powerful system for taking exceptional
actions on transactions matching specific criteria.

A signal is a piece of information such as: "the smtp client EHLO
matched their reverse dns." A policy consists of a match expression to
identify messages which is a logical combination of signals and an
action to take on messages that match the expression. For example:
"for transactions where the EHLO didn't match, return an smtp 550
response."

This separation between signals and policies allows uniform
implementation of actions in one place rather than scattering them
across every signal. It also allows policies to include arbitrary
logical combinations of signals often for allowlisting: "reject tx
where the EHLO didn't match unless it is from the ip subnet of my
known inbound gateway thing."

Output chain filters emit signals to ``tx.filter_output``. This is
typically a filter-specific subclass of
``FilterOutput``. ``FilterOutput.match(yaml)`` returns whether the
output matches criteria specified in yaml. ``MatcherResult`` includes
``PRECONDITION_UNMET`` if e.g. the matcher is being run at
``tx.mail_from`` but the signal depends on the body, etc. This no-ops
all policy_action invocations with the same tag for the remainder of
the current ``FilterChain.update()`` cycle.

Simple matchers can also be loaded via ``modules.transaction_matcher``
similar to ``recipient_router_filter.RoutingPolicy``. A simple matcher
is just ``Callable[[yaml, TransactionMetadata], MatcherResult]``

Policies are specified with an invocation of ``policy_action``
filter. Filter yaml:

match: match expression, can be an individual signal ``matcher``
  invocation or a logical expression all/any/not.  An empty/unpopulated
  match expression always matches for use as the catchall/fallthrough at
  the end of a group.

match expression examples::

  match:
    matcher: koukan.remote_host_filter.RemoteHostFilter
    # ...

   match:  # (not x) or (y and z)
     any:
     - not:
       matcher: x
     - all:
       - matcher: y
         y_matcher_arg: 1
       - matcher: z
         z_matcher_arg: "q"

tag: group/tag name

name: rule name (defaults to tag if not present)

action: REJECT | LOG | MATCH
or list of [weight, action] for percent experiments. The denominator
is the sum of the weights.

code: smtp response code for REJECT (default 550)

message: smtp response message for REJECT (default: '5.6.0 message rejected'
along with the matching rule name)

action examples::

  action:
  - [0.9: LOG]
  - [0.1: REJECT]

Full policy_action example::

  endpoint:
  - name: ingress
    # ...
    chain:
    - filter: remote_host
    - filter: policy_action
      match:
        any:
        - matcher: koukan.remote_host_filter.RemoteHostFilter
          fcrdns: false
        - matcher: koukan.remote_host_filter.RemoteHostFilter
          ehlo_alignment: false
      action:
      - [9, LOG]
      - [1, REJECT]
      tag: remote_host

This means: if RemoteHostFilter either returned false for fcrdns or
ehlo alignment, LOG 90% of the time and REJECT 10% of the time.

The policy action filter has its own filter_output object which contains 2
sets: matched_tags and matched_rules.

PolicyActionFilter first checks the rule's tag and rule name against
PolicyActionFilterOutput; if either is present, the rule is treated as
a no-op. Then it evaluates the match expression. If it returns
``PRECONDITION_UNMET``, all policy invocations with the same tag are
no-op'd for the rest of the current ``FilterChain.update()``
invocation.

If the expression matches, PolicyActionFilter applies the action:

* REJECT: sets the responses to code from yaml (default: 550)
* MATCH: retires rule and tag
* LOG: retires rule only. This is useful to dark-launch multiple
  reject rules to see all the ones that match rather than stopping
  after the first.

PolicyActionFilter's output object is itself a matcher! This means you
can match on the results of previous policy_action invocations so you
can, for example, write a single rule to match an allowlist signal to
reuse in multiple reject rules::

  chain:
  - filter: policy_action
    match:
      matcher: network_address
      cidr: 192.168.1.0/24
    name: my_inbound_gw
  - filter: message_validation
  - filter: policy_action
    match:
      all:
        - not:
            matcher: koukan.policy_action_filter.PolicyActionFilter
            rule: my_inbound_gw
        - matcher: koukan.message_validation_filter.MessageValidationFilter
          validity_threshold: HIGH
     tag: validation
     action: REJECT


Signals
^^^^^^^

The following built-in filters populate filter_output, cf individual
filter descriptions (above):

* :ref:`message_validation_filter`
* :ref:`remote_host_filter`
* :ref:`dkim_check_filter`
* :ref:`spf_check_filter`
* :ref:`policy_action <signals>`

In addition several simple matchers are available in ``koukan.transaction_matchers``:

network_address: transaction_matchers.match_network_address remote_host cidr::

  match:
    matcher: network_address
    cidr: 192.168.1.0/24

tls: transaction_matchers.match_tls was the transaction received via
smtp with tls?

address_list: matches lists of email addresses using the same
specification as the routing policy AddressListPolicy

smtp_auth: matches if the message was received with smtp
authentication. Replaces ``relay_auth`` filter

Cluster/k8s
===========

All replicas share the same underlying database.

The router implementation currently buffers data through the local
filesystem but this does not need to be durable across restarts;
emptyDir is fine. This is local to each router process; the router and
gateway do not share data through the filesystem.

Koukan may return http redirects if the transaction is leased/active
on a different replica; native rest clients must be prepared to follow
these.

For both router and gateway, configure ``rest_listener.session_uri`` to
point to the dns alias or ip of the individual pod/replica. For
router, configure ``rest_listener.service_uri`` to the router service dns
alias.
