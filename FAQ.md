# General

## What is Koukan?

Koukan is a email Message Transfer Agent (MTA [wikipedia](https://en.wikipedia.org/wiki/Message_transfer_agent)) with an integrated HTTP/JSON REST api.

Key features include  
- integrated message-format handling  
rest clients can interact via JSON and Koukan handles the rfc822/MIME  
- extensible via plugin api  
- queue storage compatible with autoscaling/k8s  
- opportunistic cut-through delivery:
report many message send errors \~synchronously instead of receiving a bounce after-the-fact

## Why did you call it Koukan?

Definition at [jisho.org](https://jisho.org/word/%E4%BA%A4%E6%8F%9B), think “interchange”

## Why wouldn’t I just use Sendgrid/SaaS?

Email Service Providers (ESPs) offer tremendous value for many important use cases. If you are running a public-facing application, e-commerce site, etc that needs to ensure deliverability of e-mail to every mailbox provider in the world, that is what you should be using.

If you are sending mail within your own organization/site, SaaS may make less sense. You probably have a number of miscellaneous applications that send a small amount of traffic to a small number of destinations, maybe just your own domain. You might be delivering operational mail/alerts to a mailbox server within the same cluster. In these cases, you want to deploy something locally and forget about it. 

## Am I the target audience for Koukan?

### Yes  
- You have have run an MTA before
- have some experience with http/rest apis
- are willing to get your hands dirty with Python.

### No  
- You want to deliver mail to many local users on a timesharing system  
- You need extreme performance/scalability  
- You need robust integrated spam/phishing/antivirus out of the box

## Should I use Koukan in a high-integrity application?

Not yet. This is a prototype/alpha-quality software.

## How does Koukan relate to Unix mailers like Exim and Postfix?

To use an analogy, if Exim and Postfix are Apache, Koukan is Envoy.

Koukan does not deliver messages by spawning other programs or writing to files in the filesystem, it only connects to a socket. So you need something to listen on a socket to receive messages from Koukan.

# Implementation

## How does Koukan work?

There are 2 main components:  
### SMTP gateway  
- proxies smtp ↔ HTTP/JSON REST  
- stateless, minimal business logic  
### Router  
- only speaks http/rest

The router has 2 basic flows  
### Input: rest → storage  
fastapi routes invoke RestHandler  
RestHandler writes to StorageWriterFilter  
StorageWriterFilter writes to durable storage  
### Output: storage → rest  
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

1. Python has a number of high-quality implementations of core email standards, in particular the rfc822/MIME codec, SMTP and domainkeys that saved a ton of time not to write from scratch.  
2. This is a prototype  
3. It remains to be seen if the (perceived) performance limitations of Python will be a factor in practice for use cases that are a good fit for this. It is possible by being smart about memory and forking and with the possibility of auto-scaling on k8s that the current Python implementation can be “scalable enough” for many use cases.

## Why don’t you use \<my favorite framework/middleware/...\>?

1. I have tried to limit myself to things that I’m familiar with and am confident are a good fit for this.  
2. I have tried to keep the dependency footprint small. I think we have all had the experience of failing to get some software working that depends on the head-of-tree version of 10 different packages.

# Deployment

## Can I run Koukan on Kubernetes k8s or other multi-node/cluster environment?

YES! Basic support for this was added in ccff073fc686b40c96d50d03a79665e715b49a23.

All replicas share the same underlying database.

The current implementation buffers data through the local filesystem but this does not need to be durable across restarts; emptyDir is fine. This is local to each router process; the router and gateway do not share data through the filesystem.

Koukan may return http redirects in response to requests to endpoints with rest_lro enabled; native rest clients must be prepared to follow these. 

## Can I use Envoy as a front proxy for the Koukan SMTP Gateway?

Great idea\! STARTTLS is a blocker for most use cases, upvote [this bug](https://github.com/envoyproxy/envoy/issues/19765).

# Features/Roadmap

## Features

spamc/clamav filters

some kind of control on which “from” addresses submission clients can use

## Performance

The smtp gateway currently buffers the entire message in memory both sending and receiving. A complete solution to this probably requires some minor upstream changes to the underlying cpython smtplib and aiosmtpd libraries which I would like to pursue.

Adjacent to that, neither of those libraries support the SMTP CHUNKING extension aka BDAT rfc3030 which would be \~easy to add and is now widely supported by other MTAs.

## Gateway/Edge

If this is wildly successful, I would reimplement the smtp gateway as an [Envoy filter](https://github.com/envoyproxy/envoy/issues/9133)  
