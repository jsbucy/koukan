====
NEWS
====

Signals branch
==============

(merged ~2026/2/25)

This introduces machinery to take exceptional actions on messages
matching specified predicates. :ref:`signals`


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
