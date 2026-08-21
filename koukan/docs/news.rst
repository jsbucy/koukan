====
NEWS
====

.. _news_exploder:

``exploder`` branch
===================

(merged ~2026/08)

Reimplement :ref:`internals_exploder` functionality in
StorageWriterFilter. Backwards-compatible with existing configs but we
expect to deprecate Exploder in the future.

:ref:`output_chain`

StorageWriterFilter now directly orchestrates fan-out of rcpts from a
downstream smtp tx to multiple/per-rcpt upstream/output tx and then
fans-in the upstream responses.

Exploder adds a lot of overhead (almost 2x rest request time, db
writes, etc) due to adding a second trip through most of the stack whereas
most transactions only have 1 recipient.

This creates some additional complexity in that exploder gives you a
single point to reject the message whereas now there must be some
synchronization across upstream tx :ref:`transaction_group`. We think
this is a good tradeoff vs the large performance benefit.

Migration Guide
---------------

Look at the diffs to ``router.yaml`` ``1b7256b9`` for an example. In
the sample configs, smtp submission/ingress sender/tags inject into a
_exploder chain. These ends with an exploder filter instance.

1. remove exploder filter from the end of the _exploder chain.

2. Merge the exploder instance's ``output_chain`` into the _exploder
   chain including the final ``rest_output``

3. ``msa: true`` -> ``sf_mode: upstream_unavailability``
   ``msa: false`` -> ``sf_mode: mixed_data_response``

4. add ``mail_ok`` filter before first ``router``

``dns_policy`` branch
=====================

(merged 2026/3/17)

Add DnsResolutionFilterOutput :ref:`dns_resolution_filter`

drive-by improvements:

* set smtp server ident
* smtp client logging


``signals_rcpt`` branch
=======================

(merged 2026/3/6)

Add per_rcpt policy_action mode to allow writing rules to reject
individual recipients. :ref:`signals`

Matcher updates:

* add num_rcpts matcher
* add per_rcpt support to address_list matcher
* add invalid_mail_from/rcpt_to matchers

bugfixes/improvements
---------------------

receivers handle heartbeats, validate content-type

aiosmtpd passes null-reverse-path to handler as <> but the koukan
stack expects empty string

domain_from_address() catch exceptions parsing the address

recipient router support no policy = route everything

This is a better way to write the catchall since ``address_list`` doesn’t
match invalid address.

before::

  - filter: router
    policy:
      name: address_list
      # no domains/dest -> reject all

after::

  - filter: router
    # no policy: match everything
    # no dest: reject

RestEndpoint fail the tx if we got that far without
endpoint/base_url. This can happen with a bad routing config.
Previously this would result in an uncaught exception.


Signals branch
==============

(merged 2026/2/26)

This introduces machinery to take exceptional actions on messages
matching specified criteria. :ref:`signals`

This includes the following new filters:

* :ref:`policy_action <signals>`
* :ref:`dkim_check_filter`
* :ref:`spf_check_filter`
* :ref:`message_validation_filter`


Incompatible changes
--------------------

``relay_auth`` filter replaced with simple matcher
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

before::

  endpoint:
  - name: submission
    chain:
    - filter: relay_auth

after::

  endpoint:
  - name: submission
    chain:
    - filter: policy_action
      match:
        not:
          matcher: smtp_auth
      tag: smtp_auth
      action: REJECT
      code: 550
      message: '5.7.1 not authorized'


``dkim`` filter renamed ``dkim_sign``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
