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
PYTHONPATH=. gunicorn3 -b localhost:8002 --access-logfile - --log-level debug\ 
'examples.receiver.receiver:create_app()'
```
listens on 8002 for rest

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
`python koukan/ssmtp.py localhost 1025 localhost alice@example.com bob@example.com <<< 'hello, world!'`

should print out on the `fake_smtpd` console

- mx receive to rest receiver
`python koukan/ssmtp.py localhost 1025 localhost alice@example.com bob@rest-application.example.com <<< 'hello, world!'`

should print out on the `examples/receiver` console

- smtp submission  
`python koukan/ssmtp.py localhost 1587 localhost alice@example.com bob@example.com <<< 'hello, world!'`

should print out on the `fake_smtpd` console

- rest submission  
`python examples/cli/send_message.py --mail_from alice@example.com --message_builder_filename examples/cli/message_builder.json bob@example.com`

should print out on the `fake_smtpd` console


# Public Internet

You will need a few things:

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

Arrange for the gateway to be able to bind privileged ports (\<1024).
On Linux, I’ve been using capsh to do this, example in
`config/local-test/run_gateway.sh`

