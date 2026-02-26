====
NEWS
====

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
