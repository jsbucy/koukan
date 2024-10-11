# General

## What is Koukan?

Koukan is a email Message Transfer Agent (MTA [wikipedia](https://en.wikipedia.org/wiki/Message_transfer_agent)) with an integrated HTTP/JSON REST api.

Key features include  
\- integrated message-format handling  
rest clients can interact via JSON and Koukan handles the rfc822/MIME  
\- extensible via plugin api  
\- queue storage compatible with autoscaling/k8s  
\- opportunistic cut-through delivery  
report many message send errors \~synchronously instead of receiving a bounce after-the-fact

## Why did you call it Koukan?

Definition at [jisho.org](https://jisho.org/word/%E4%BA%A4%E6%8F%9B), think “interchange”

## Why wouldn’t I just use Sendgrid/SaaS?

Email Service Providers (ESPs) offer tremendous value for many important use cases. If you are running a public-facing application, e-commerce site, etc that needs to ensure deliverability of e-mail to every mailbox provider in the world, that is what you should be using.

If you are sending mail within your own organization/site, SaaS may make less sense. You probably have a number of miscellaneous applications that send a small amount of traffic to a small number of destinations, maybe just your own domain. You might be delivering operational mail/alerts to a mailbox server within the same cluster. In these cases, you want to deploy something locally and forget about it. 

## Am I the target audience for Koukan?

Yes  
You have have run an MTA before and have some experience with http/rest apis and are willing to get your hands dirty with Python.

No  
You want to deliver mail to many local users on a timesharing system  
You need extreme performance/scalability  
You need robust integrated spam/phishing/antivirus out of the box

## Should I use Koukan in a high-integrity application?

Not yet. This is a prototype/alpha-quality software.

## How does Koukan relate to Unix mailers like Exim and Postfix?

To use an analogy, if Exim and Postfix are Apache, Koukan is Envoy.

Koukan does not deliver messages by spawning other programs or writing to files in the filesystem, it only connects to a socket. So you need something to listen on a socket to receive messages from Koukan.

# Implementation

## How does Koukan work?

There are 2 main components:  
smtp gateway  
proxies smtp \<-\> HTTP/JSON REST  
stateless, minimal business logic  
router  
only speaks http/rest

The router has 2 basic flows  
input: rest → storage  
fastapi routes invoke RestHandler  
RestHandler writes to StorageWriterFilter  
StorageWriterFilter writes to durable storage  
output: storage → rest  
OutputHandler reads from durable storage  
OutputHandler writes to the output filter chain  
the last of these is RestEndpoint which sends via http

Except that we have to handle multi-recipient transactions from SMTP so there may be fan-out between the input side and the output side. This is implemented by the Exploder. So now we have

http → fastapi → RestHandler → StorageWriterFilter → Storage  
writing a single storage transaction with all downstream recipients  
Storage → OutputHandler → \[ Exploder → StorageWriterFilter → Storage \]  
writing a separate storage transaction for each recipient  
Storage → OutputHandler → SyncFilter output chain → RestEndpoint → http  
separate instance of this flow for each recipient

All data between the input and output sides flows through the Storage module. This uses SQLAlchemy Core to get to the underlying database and my intent is for it to be portable across databases and not depend on a lot of engine-specific features in particular change notifications. Instead the router uses in-process synchronization to coordinate the downstream and upstream handlers for a given transaction.

## Why did you use Python?

1: Python has a number of high-quality implementations of core email standards, in particular the rfc822/MIME codec, SMTP and domainkeys that saved a ton of time not to write from scratch.  
2: This is a prototype  
3: It remains to be seen if the (perceived) performance limitations of Python will be a factor in practice for use cases that are a good fit for this. It is possible by being smart about memory and forking and with the possibility of auto-scaling on k8s that the current Python implementation can be “scalable enough” for many use cases.

## Why don’t you use \<my favorite framework/middleware/...\>?

1: I have tried to limit myself to things that I’m familiar with and am confident are a good fit for this.  
2: I have tried to keep the dependency footprint small. I think we have all had the experience of failing to get some software working that depends on the head-of-tree version of 10 different packages.

## Koukan supports both Flask and FastAPI?

I started with Flask which I was already familiar with from other projects and discovered FastAPI more recently. I didn’t want to burn the bridge on Flask when I integrated FastAPI. FastAPI is specifically tailored to this use case and has been working well and ASGI is a major upgrade over WSGI. So I expect to drop Flask at some point.

# Deployment

## Can I run Koukan on Kubernetes k8s?

I am not an expert on k8s but my intent from the outset has been to be compatible with k8s.

The current implementation buffers data through the local filesystem but this does not need to be durable across restarts; emptyDir is fine. This is local to each router process; the router and gateway do not share data through the filesystem.

With a single replica, it should just work. To support multiple replicas and especially autoscaling, there needs to be sticky http routing at the level of restmtp transactions. In other words, all http requests to a given REST /transactions/123 resource need to go to the process that currently has it leased. The minimum support needed from the Koukan router is probably to be able to http-redirect requests to the correct replica. This should be a tiny patch but didn’t make the initial release. I hope to include this in the first follow-up release \~late Oct 2024\. Please ping \<FR\> if you’re interested in this.

## Can I use Envoy as a front proxy for the Koukan SMTP Gateway?

Great idea\! STARTTLS is a blocker for most use cases, upvote [this bug](https://github.com/envoyproxy/envoy/issues/19765).

# Features/Roadmap

## Features

spamc/clamav filters

some kind of control on which “from” addresses submission clients can use

get multi-node/k8s working out of the box

## Performance

The smtp gateway currently buffers the entire message in memory both sending and receiving. A complete solution to this probably requires some minor upstream changes to the underlying cpython smtplib and aiosmtpd libraries which I would like to pursue.

Adjacent to that, neither of those libraries support the SMTP CHUNKING extension aka BDAT rfc3030 which would be \~easy to add and is now widely supported by other MTAs.

## Gateway/Edge

If this is wildly successful, I would reimplement the smtp gateway as an [Envoy filter](https://github.com/envoyproxy/envoy/issues/9133)  
