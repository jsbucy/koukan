====
NEWS
====

``dns_policy`` branch
=====================

(merged 2026/3/17)

Add DnsResolutionFilterOutput :ref:`dns_resolution_filter`

drive-by improvements:
- set smtp server ident
- smtp client logging


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
