==========
Quickstart
==========

Dependencies
=============
Any `actively-supported version <https://devguide.python.org/versions/#versions>`__ of Python3.

`bazel <https://bazel.build/>`__ to run the tests

a supported database: `sqlite3 <https://www.sqlite.org/>`__ or
`PostgreSQL <https://www.postgresql.org/>`__


Caveats
=======

1. The HTTP/REST endpoints have NO AUTHENTICATION and thus must not be
   open to an untrusted network. Use a front proxy/sidecar to control
   access to this.
2. The SMTP submission server has limited support for SMTP
   authentication using the AUTH PLAIN SASL mechanism. This uses a local
   set of credentials, think robot/service accounts.

General
=======
The router and gateway binaries are configured entirely through
`yaml <https://en.wikipedia.org/wiki/YAML>`__ config files. The only
command-line parameter they accept is the path to the config file.

The router buffers some inflight data through the filesystem but
this does not need to be durable; ``EmptyDir`` is fine. The smtp gateway
should not require a writable filesystem at all.

If you want the Koukan smtp gateway to terminate port 25/587 connections
directly from the public internet, you will need to arrange for it to
be able to bind privileged ports. On Linux, you can do this with ``capsh`` per this Stack Overflow `answer <https://stackoverflow.com/questions/413807/is-there-a-way-for-non-root-processes-to-bind-to-privileged-ports-on-linux>`__

Alternatively, you use a front proxy such as `Envoy
<https://envoyproxy.io>`__ to terminate port 25 and let the gateway
listen on an unprivilged port. Note that Envoy cannot currently
terminate SMTP STARTTLS but there has been some work in this area
`bug`_.

.. _bug: https://github.com/envoyproxy/envoy/issues/19765


Database Setup
==============

SQLite
------

::

    sqlite3 my_db.sqlite3 < koukan/init_storage.sql

PostgreSQL
----------

::

   createdb my_db

   psql my_db < koukan/init_storage_postgres.sql

| storage.url in config/local-test/router.yaml is a SQLAlchemy `database
  url <https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls>`__
| edit this to point to your database

Local Test Environment
======================

for local testing, you can just run it from the top-level directory

install pip dependencies::

    pip install -r requirements.txt

run the gateway::

    bash config/local-test/run_gateway.sh

listens on 1025 (mx/receive) and 1587 (submission/send) for smtp,  8001 for rest

run the router::

    bash config/local-test/run_router.sh

listens on 8000 for rest

run the rest listener::

    PYTHONPATH=. uvicorn --host localhost --port 8002 \
    --log-level debug \
    --factory 'examples.receiver.fastapi_receiver:create_app'

listens on 8002 for rest and drops files in ``/tmp/my_messages``

start the smtp sink::

    python3 koukan/fake_smtpd.py 3025

listens on 3025 for smtp, prints messages to stdout

gateway port 1025 → router “ingress” sender ``smtp-mx`` tag

- routes all addresses at domain ``example.com`` to ``fake_smtpd`` via gateway
- routes ``bob@rest-application.example.com`` (and + extension addresses) to
  examples/receiver

gateway port 1587 → router “submission” sender ``smtp-msa`` tag

- routes all addresses to ``fake_smtpd`` via gateway

Send some messages
------------------

SMTP ingress to smtp receiver
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
::

    python koukan/ssmtp.py --host=localhost --port=1025 --ehlo=localhost --mail_from=alice@example.com bob@example.com <<< 'hello, world!'

should print out on the ``fake_smtpd`` console

SMTP ingress to rest receiver
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
::

    python koukan/ssmtp.py --host=localhost --port=1025 --ehlo=localhost --mail_from=alice@example.com bob@rest-application.example.com <<< 'hello, world!'

should print out on the ``examples/receiver`` console

SMTP submission
~~~~~~~~~~~~~~~
::

    python koukan/ssmtp.py --host=localhost --port=1587 --ehlo=localhost --mail_from=alice@example.com bob@example.com <<< 'hello, world!'

should print out on the ``fake_smtpd`` console

Rest submission
~~~~~~~~~~~~~~~
::

    python examples/send_message/send_message.py --mail_from alice@example.com --message_builder_filename examples/send_message/message_builder.json bob@example.com

should print out on the ``fake_smtpd`` console

Public Internet
===============

Prerequisites
-------------

-  a domain name you control
-  a VM that can connect to destination SMTP servers on port 25 with an
   IP that you can make the forward and reverse dns match (FCrDNS
   `wikipedia
   <https://en.wikipedia.org/wiki/Forward-confirmed_reverse_DNS>`__)
   in particular GCP blocks port 25 many smtp servers now expect the
   SMTP EHLO hostname to match the rdns of the connecting IP. Set
   ``ehlo_host`` in ``smtp_output`` in ``gateway.yaml`` to the
   hostname.
-  an SSL/TLS certificate for the SMTP server. letsencrypt is fine. Set
   ``cert`` and ``key`` in ``smtp_listener`` in ``gateway.yaml``.
-  SPF records for your sending domain that authorize your sending IP
   (`wikipedia <https://en.wikipedia.org/wiki/Sender_Policy_Framework>`__)
-  DKIM keys (`wikipedia
   <https://en.wikipedia.org/wiki/DomainKeys_Identified_Mail>`__)
   create with ``dknewkey`` from `dkimpy
   <https://pypi.org/project/dkimpy/>`__ and publish the public key in
   dns. Edit ``dkim`` filter in output chains in ``router.yaml`` to
   point to the .key file.

SMTP Authentication
-------------------

In the default config, gateway smtp listener port 1025 routes to router
ingress endpoint and port 1587 routes to submission. The
``ingress`` chain only accepts mail for addresses that match the
configured address_list routing policies. The ``submission`` chain
routes any address to the dns name/mx record in the rhs of the
destination address. Without access control, this will be an open relay!

At the moment, we support minimal smtp auth with a local set of users.
Enable ``auth_secrets`` on the gateway ``smtp_listener`` and create
``secrets.json`` which is a dict from username to secret-hash

::

   {"my-application": "9d66f38b02c08c8d8ed496032107f02370f3513957bf129325eefa9b3fdfe02e"}

run ``python koukan/smtp_auth.py`` which emits
``<secret> <secret hash>``, configure the secret in the application and
the hash in ``secrets.json``. Enable ``relay_auth`` filter for the
``outbound-gw`` endpoint/chain in the router. Note that this secret
storage is only suitable for high-entropy secrets and NOT user-provided
passwords, please DO NOT reuse a password here!

The gateway will save a single bool ``auth`` in the ``smtp_meta`` field
of the rest transaction sent to the router if the client successfully
authenticated. ``relay_auth_filter`` keys off of this.

Cluster/k8s/multi-node
======================

For both router and gateway, configure ``rest_listener.session_uri`` to
point to the dns alias or ip of the individual pod/replica. For router,
configure ``rest_listener.service_uri`` to the router service dns alias. Set
``endpoint.rest_lro`` to false for endpoints that the gateway injects into
and true to endpoints that native rest clients use.


aiosmtpd and smtplib
====================

We are in the process of making some scalability improvements to
aiosmtpd and cpython smtplib. Unless you are handling a lot of large
messages, this isn’t critical. Koukan will take advantage of both if
available but remains compatible with mainline.

Our fork of aiosmtpd is at
`github <https://github.com/jsbucy/aiosmtpd/tree/data_chunk3>`__. For
the time being, we are maintaining smtplib in a fork of the cpython tree
at `github <https://github.com/jsbucy/cpython/tree/smtplib_bdat>`__.
However we are shipping a copy of our fork of smtplib in the Koukan tree
and use this by default.
