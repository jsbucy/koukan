dependencies  
reasonably recent python3  
\>= 3.7 for aiosmtpd  
bazel to run the tests  
a supported database  
sqlite3 or PostgreSQL

# Caveats

1\. the HTTP/REST endpoints have NO AUTHENTICATION and thus must not be open to an untrusted network  
use a front proxy/sidecar to control access to this  
2: the SMTP submission server has limited support for SMTP authentication  
with a local set of credentials, think robot/service accounts  
AUTH PLAIN SASL mechanism

# Database Setup

## Sqlite

sqlite3 my\_db.sqlite3 \< koukan/init\_storage.sql

## Postgres

createdb my\_db  
psql my\_db \< koukan/init\_storage\_postgres.sql

storage.url in config/local-test/router.yaml is a SQLAlchemy [database url](https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls)  
edit this to point to your database

# Local Test Environment

for local testing, you can just run it from the top-level directory

pip install -r requirements.txt

bash config/local-test/run\_gateway.sh  
runs the gateway listening on 1025 (mx/receive) and 1587 (submission/send) for smtp  
8001 for rest  
bash config/local-test/run\_router.sh  
listens on 8000 for rest  
PYTHONPATH=. gunicorn3 \-b localhost:8002 \--access-logfile \- \--log-level debug  
   'examples.receiver.receiver:create\_app()'  
listens on 8002 for rest

python3 fake\_smtpd.py 3025  
listens on 3025 for smtp  
sink for outbound messages  
prints messages to stdout

gateway 1025 \-\> router “inbound-gw”  
routes all addresses at domain “example.com” to fake\_smtpd via gateway  
routes bob@rest-application.example.com (and \+ extension addresses) to examples/receiver

1587 \-\> router “outbound-gw”  
routes all addresses to fake\_smtpd via gateway

mx receive  
python ./ssmtp.py localhost 1025 localhost alice@example.com bob@example.com

smtp submission  
python ./ssmtp.py localhost 1587 localhost alice@example.com bob@example.com

rest submission  
python examples/cli/send\_message.py \--mail\_from alice@example.com \--message\_builder\_filename examples/cli/message\_builder.json bob@example.com

# Public Internet

You will need a few things:

a domain name you control  
a VM that can connect to destination SMTP servers on port 25  
with an IP that you can make the forward and reverse dns match (FCrDNS [wikipedia](https://en.wikipedia.org/wiki/Forward-confirmed_reverse_DNS))  
in particular GCP blocks port 25  
many smtp servers now expect the SMTP EHLO hostname to match the rdns of the connecting IP  
an SSL/TLS certificate  
letsencrypt is fine  
SPF records for your sending domain that authorize your sending IP ([wikipedia](https://en.wikipedia.org/wiki/Sender_Policy_Framework))  
DKIM keys ([wikipedia](https://en.wikipedia.org/wiki/DomainKeys_Identified_Mail))  
create with dknewkey from dkimpy and publish the public key in dns  
edit router.yaml to point to the .key file

arrange for the gateway to be able to bind privileged ports (\<1024)  
on Linux, I’ve been using capsh to do this per this [SA answer](https://stackoverflow.com/questions/413807/is-there-a-way-for-non-root-processes-to-bind-to-privileged-ports-on-linux)

