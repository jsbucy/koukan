dependencies  
reasonably recent python3 (\>= 3.7 for `aiosmtpd`)

[bazel](https://bazel.build/) to run the tests  

a supported database: [sqlite3](https://www.sqlite.org/) or
[PostgreSQL](https://www.postgresql.org/)

# Caveats

1. The HTTP/REST endpoints have NO AUTHENTICATION and thus must not be open to an untrusted network. Use a front proxy/sidecar to control access to this.
2. The SMTP submission server has limited support for SMTP
authentication using the AUTH PLAIN SASL mechanism. This uses a local
set of credentials, think robot/service accounts.



# Database Setup

## Sqlite

`sqlite3 my_db.sqlite3 < koukan/init_storage.sql`

## Postgres

```
createdb my_db

psql my_db < koukan/init_storage_postgres.sql
```

storage.url in config/local-test/router.yaml is a SQLAlchemy [database url](https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls)  
edit this to point to your database

# Local Test Environment

for local testing, you can just run it from the top-level directory

`pip install -r requirements.txt`

`bash config/local-test/run_gateway.sh`
runs the gateway listening on 1025 (mx/receive) and 1587 (submission/send) for smtp  
8001 for rest  
`bash config/local-test/run_router.sh`
listens on 8000 for rest  
```
PYTHONPATH=. hypercorn -b localhost:8002 -w0 --access-logfile - \
--log-level debug \
'examples.receiver.fastapi_receiver:create_app(path="/tmp/my_messages")'
```
listens on 8002 for rest and drops files in `/tmp/my_messages`

`python3 koukan/fake_smtpd.py 3025`
listens on 3025 for smtp  
sink for outbound messages  
prints messages to stdout

gateway 1025 → router “inbound-gw”  
routes all addresses at domain “example.com” to `fake_smtpd` via gateway  
routes bob@rest-application.example.com (and \+ extension addresses) to examples/receiver

1587 → router “outbound-gw”  
routes all addresses to `fake_smtpd` via gateway

## Send some messages

- mx receive  
`python koukan/ssmtp.py --host=localhost --port=1025 --ehlo=localhost --mail_from=alice@example.com bob@example.com <<< 'hello, world!'`

should print out on the `fake_smtpd` console

- mx receive to rest receiver
`python koukan/ssmtp.py --host=localhost --port=1025 --ehlo=localhost --mail_from=alice@example.com bob@rest-application.example.com <<< 'hello, world!'`

should print out on the `examples/receiver` console

- smtp submission  
`python koukan/ssmtp.py --host=localhost --port=1587 --ehlo=localhost --mail_from=alice@example.com bob@example.com <<< 'hello, world!'`

should print out on the `fake_smtpd` console

- rest submission  
`python examples/send_message/send_message.py --mail_from alice@example.com --message_builder_filename examples/send_message/message_builder.json bob@example.com`

should print out on the `fake_smtpd` console


# Public Internet

## Prerequisites

- a domain name you control  
- a VM that can connect to destination SMTP servers on port 25  
with an IP that you can make the forward and reverse dns match (FCrDNS [wikipedia](https://en.wikipedia.org/wiki/Forward-confirmed_reverse_DNS))  
in particular GCP blocks port 25  
many smtp servers now expect the SMTP EHLO hostname to match the rdns of the connecting IP  
- an SSL/TLS certificate  
letsencrypt is fine  
- SPF records for your sending domain that authorize your sending IP ([wikipedia](https://en.wikipedia.org/wiki/Sender_Policy_Framework))  
- DKIM keys ([wikipedia](https://en.wikipedia.org/wiki/DomainKeys_Identified_Mail))  
create with `dknewkey` from [dkimpy](https://pypi.org/project/dkimpy/) and publish the public key in dns  
edit router.yaml to point to the .key file

## SMTP Authentication

In the default config, gateway smtp listener port 1025 routes to
router inbound-gw endpoint and port 1587 routes to outbound-gw. The
`inbound-gw` chain only accepts mail for addresses that match the
configured address_list routing policies. The `outbound-gw` chain routes
any address to the dns name/mx record in the rhs of the destination
address. Without access control, this will be an open relay!

At the moment, we support minimal smtp auth with a local set of
users. Enable `auth_secrets` on the gateway `smtp_listener` and create 
`secrets.json` which is a dict 
from username to secret-hash
```
{"my-application": "9d66f38b02c08c8d8ed496032107f02370f3513957bf129325eefa9b3fdfe02e"}
```

run `python koukan/smtp_auth.py` which emits `<secret> <secret hash>`,
configure the secret in the application and the hash in
`secrets.json`. Enable `relay_auth` filter for the `outbound-gw`
endpoint/chain in the router. Note that this secret storage is only
suitable for high-entropy secrets and NOT user-provided passwords,
please DO NOT reuse a password here!

The gateway will save a single bool `auth` in the `smtp_meta` field of
the rest transaction sent to the router if the client successfully
authenticated. `relay_auth_filter` keys off of this.

## Cluster/k8s/multi-node

For both router and gateway, configure rest_listener.session_uri to point to the dns alias or ip of the individual pod/replica. For router, configure rest_listener.service_uri to the router service dns alias. Set endpoint.rest_lro to false for endpoints that the gateway injects into and true to endpoints that native rest clients use.


## Configuration

The timeouts in the default config are artificially low for testing.


Arrange for the gateway to be able to bind privileged ports (\<1024).
On Linux, I’ve been using `capsh` to do this, example in
`config/local-test/run_gateway.sh`

