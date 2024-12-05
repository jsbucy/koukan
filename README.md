# Koukan Email Gateway

Koukan is an email/application gateway.

Koukan provides mail transfer agent ([MTA](https://en.wikipedia.org/wiki/Message_transfer_agent)) and mail submission agent ([MSA](https://en.wikipedia.org/wiki/Message_submission_agent))
functionality for existing applications that expect SMTP [store&forward](https://en.wikipedia.org/wiki/Store_and_forward#Email)
semantics.

Koukan provides a rich HTTP/JSON REST api for new applications. The
api includes rfc822/MIME format handling which has uneven support across
programming languages/frameworks.

Koukan is targeting on-premises/self-hosted use cases where
software-as-a-service (SAAS) isn't a good fit and you want the email
stack local to the application.

Key features:
- opportunistic [cut-through delivery](https://en.wikipedia.org/wiki/Cut-through_switching#Use_in_SMTP): synchronous reporting of
common-case errors rather than accept&bounce
- compatible with horizontal scaling on multi-node/cluster/Kubernetes environments
- extensible via plugin api inspired by [Envoy](https://www.envoyproxy.io/) [filter chains](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/http/http_routing). Koukan aspires to be "Envoy for email."

Koukan is written in Python3 leveraging high-quality
Python implementations of email technical standards/RFCs that already exist such as [smtplib](https://docs.python.org/3/library/smtplib.html),
[aiosmtpd](https://aiosmtpd.aio-libs.org/), [email](https://docs.python.org/3/library/email.html), and [dkimpy](https://launchpad.net/dkimpy/).

[Frequently Asked Questions](FAQ.md)

[Quickstart](QUICKSTART.md)

