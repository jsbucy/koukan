# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import unittest
import logging

from koukan.mx_resolution import DnsResolutionFilter
from koukan.filter import HostPort, Mailbox, Resolution, TransactionMetadata
from koukan.fake_dns_wrapper import FakeResolver
from koukan.response import Response

from dns.resolver import NXDOMAIN, NoNameservers
from dns.resolver import Answer
from dns.rrset import RRset
from dns.rdataclass import RdataClass
from dns.rdatatype import RdataType
import dns.rrset
import dns.flags
from dns.rdtypes.ANY.PTR import PTR
import dns.name

mx_message_text = """id 1234
opcode QUERY
rcode NOERROR
flags QR AA RD
;QUESTION
example.com. IN MX
;ANSWER
example.com. IN MX 10 mx.example.com.
;AUTHORITY
;ADDITIONAL
"""

# this pattern of constructing Answer is cribbed from
# dnspython/tests/test_resolution.py
# but doesn't type check:
# error: Argument 4 to "Answer" has incompatible type "Message"; expected "QueryMessage"  [arg-type]

mx_message = dns.message.from_text(mx_message_text)
mx_answer = Answer(
    dns.name.from_text('example.com.'),
    RdataType.MX,
    RdataClass.IN,
    mx_message)  # type: ignore[arg-type]

a_message_text = """id 1234
opcode QUERY
rcode NOERROR
flags QR AA RD
;QUESTION
mx.example.com. IN A
;ANSWER
mx.example.com. 1 IN A 1.2.3.4
;AUTHORITY
;ADDITIONAL
"""

a_message = dns.message.from_text(a_message_text)
a_answer = Answer(
    dns.name.from_text('mx.example.com.'),
    RdataType.A,
    RdataClass.IN,
    a_message)  # type: ignore[arg-type]


aaaa_message_text = """id 1234
opcode QUERY
rcode NOERROR
flags QR AA RD
;QUESTION
mx.example.com. IN AAAA
;ANSWER
mx.example.com. 1 IN AAAA 0123:4567:89ab:cdef:0123:4567:89ab:cdef
;AUTHORITY
;ADDITIONAL
"""

aaaa_message = dns.message.from_text(aaaa_message_text)
aaaa_answer = Answer(
    dns.name.from_text('mx.example.com.'),
    RdataType.AAAA,
    RdataClass.IN,
    aaaa_message)  # type: ignore[arg-type]


class DnsResolutionFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
            '%(message)s')

    def test_static(self):
        filter = DnsResolutionFilter(
            static_resolution=Resolution([HostPort('1.2.3.4', 25)]),
            suffix='.com')
        filter.wire_downstream(TransactionMetadata())
        filter.wire_upstream(TransactionMetadata())

        delta = TransactionMetadata(
            mail_from = Mailbox('alice'),
            resolution = Resolution([HostPort('example.CoM', 25)]))
        filter.downstream_tx.merge_from(delta)
        filter.on_update(delta)
        self.assertEqual(filter.upstream_tx.resolution.hosts[0].host,
                         '1.2.3.4')

    def test_dns(self):
        resolver = FakeResolver([
            mx_answer,
            a_answer,
            dns.resolver.NoAnswer()])  # AAAA

        filter = DnsResolutionFilter(
            suffix='',
            resolver=resolver)
        filter.wire_downstream(TransactionMetadata())
        filter.wire_upstream(TransactionMetadata())

        def upstream():
            return TransactionMetadata()

        delta = TransactionMetadata(
            resolution = Resolution([HostPort('example.CoM', 25)]))
        filter.downstream_tx.merge_from(delta)
        filter.on_update(delta)
        self.assertEqual([h.host for h in filter.upstream_tx.resolution.hosts],
                         ['1.2.3.4'])

    def test_no_mx(self):
        resolver = FakeResolver([
            dns.resolver.NoAnswer(),
            a_answer,
            dns.resolver.NoAnswer()])  # AAAA

        filter = DnsResolutionFilter(suffix='', resolver=resolver)
        filter.wire_downstream(TransactionMetadata())
        filter.wire_upstream(TransactionMetadata())

        delta = TransactionMetadata(
            resolution = Resolution([HostPort('mx.example.com', 25)]))
        filter.downstream_tx.merge_from(delta)
        filter.on_update(delta)
        self.assertEqual([h.host for h in filter.upstream_tx.resolution.hosts],
                         ['1.2.3.4'])

    def test_ipv6(self):
        resolver = FakeResolver([
            mx_answer,
            dns.resolver.NoAnswer(),  # A
            aaaa_answer])

        filter = DnsResolutionFilter(
            suffix='',
            resolver=resolver)
        filter.wire_downstream(TransactionMetadata())
        filter.wire_upstream(TransactionMetadata())

        delta = TransactionMetadata(
            resolution = Resolution([HostPort('example.CoM', 25)]))
        filter.downstream_tx.merge_from(delta)
        filter.on_update(delta)
        self.assertEqual([h.host for h in filter.upstream_tx.resolution.hosts],
                         ['0123:4567:89ab:cdef:0123:4567:89ab:cdef'])

    def test_needs_resolution(self):
        resolver = FakeResolver()
        filter = DnsResolutionFilter(suffix='.net', resolver=resolver)
        filter.wire_downstream(TransactionMetadata())
        filter.wire_upstream(TransactionMetadata())

        delta = TransactionMetadata(
            resolution = Resolution([HostPort('1.2.3.4', 25),
                                     HostPort('example.com', 25)]))
        filter.downstream_tx.merge_from(delta)
        filter.on_update(delta)
        self.assertEqual([h.host for h in filter.upstream_tx.resolution.hosts],
                         ['1.2.3.4', 'example.com'])

    def test_empty_result(self):
        resolver = FakeResolver([
            mx_answer,
            NXDOMAIN(),
            NXDOMAIN()])

        filter = DnsResolutionFilter(
            suffix='',
            resolver=resolver)
        filter.wire_downstream(TransactionMetadata())
        filter.wire_upstream(TransactionMetadata())

        delta = TransactionMetadata(
            mail_from=Mailbox('alice'),
            resolution = Resolution([HostPort('example.CoM', 25)]))
        filter.downstream_tx.merge_from(delta)
        filter_result = filter.on_update(delta)
        self.assertTrue(filter_result.downstream_delta.mail_response.code, 450)
        self.assertIn('empty', filter_result.downstream_delta.mail_response.message)

    def test_servfail(self):
        resolver = FakeResolver([NoNameservers()])
        filter = DnsResolutionFilter(suffix='', resolver=resolver)
        filter.wire_downstream(TransactionMetadata())
        filter.wire_upstream(TransactionMetadata())

        delta = TransactionMetadata(
            mail_from=Mailbox('alice'),
            resolution = Resolution([HostPort('example.CoM', 25)]))
        filter.downstream_tx.merge_from(delta)
        filter_result = filter.on_update(delta)
        self.assertTrue(filter_result.downstream_delta.mail_response.code, 450)
        self.assertIn('empty', filter_result.downstream_delta.mail_response.message)

    def test_duplicate_ip(self):
        filter = DnsResolutionFilter(
            static_resolution=Resolution([HostPort('1.2.3.4', 25)]),
            suffix='.example.com')
        filter.wire_downstream(TransactionMetadata())
        filter.wire_upstream(TransactionMetadata())

        delta = TransactionMetadata(
            resolution=Resolution([HostPort('us-west1.example.com', 25),
                                   HostPort('us-west2.example.com', 25)]))
        filter.downstream_tx.merge_from(delta)
        filter.on_update(delta)
        self.assertEqual([h.host for h in filter.upstream_tx.resolution.hosts],
                         ['1.2.3.4'])

    def test_noop_ip(self):
        filter = DnsResolutionFilter(suffix='', resolver=FakeResolver())
        filter.wire_downstream(TransactionMetadata())
        filter.wire_upstream(TransactionMetadata())

        delta = TransactionMetadata(
            mail_from=Mailbox('alice'),
            resolution=Resolution([HostPort('1.2.3.4', 25)]))
        filter.downstream_tx.merge_from(delta)
        filter.on_update(delta)
        self.assertEqual(filter.upstream_tx.resolution.hosts[0].host,
                         '1.2.3.4')

    def test_noop_no_match(self):
        filter = DnsResolutionFilter(
            static_resolution=Resolution([HostPort('1.2.3.4', 25)]),
            literal='example1.com')
        filter.wire_downstream(TransactionMetadata())
        filter.wire_upstream(TransactionMetadata())

        delta = TransactionMetadata(
            resolution=Resolution([HostPort('example2.com', 25),
                                   HostPort('example1.com', 25)]))
        filter.downstream_tx.merge_from(delta)
        filter.on_update(delta)
        self.assertEqual([h.host for h in filter.upstream_tx.resolution.hosts],
                         ['example2.com', '1.2.3.4'])


if __name__ == '__main__':
    unittest.main()
