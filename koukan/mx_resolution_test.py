# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import unittest
import logging

from koukan.mx_resolution import DnsResolutionFilter
from koukan.filter import HostPort, Mailbox, Resolution, TransactionMetadata
from koukan.fake_dns_wrapper import FakeResolver

from dns.resolver import NXDOMAIN, NoNameservers
from dns.resolver import Answer
from dns.rrset import RRset
from dns.rdataclass import RdataClass
from dns.rdatatype import RdataType
import dns.rrset
from dns.message import QueryMessage
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

mx_message = dns.message.from_text(mx_message_text)
mx_answer = Answer(
    dns.name.from_text('example.com.'),
    RdataType.MX,
    RdataClass.IN,
    mx_message)


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
    a_message)


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
    aaaa_message)


class DnsResolutionFilterTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
            '%(message)s')

    def test_static(self):
        upstream = FakeSyncFilter()

        filter = DnsResolutionFilter(
            upstream,
            static_resolution=Resolution([HostPort('1.2.3.4', 25)]),
            suffix='.com')

        def exp(tx, delta):
            self.assertEqual(tx.resolution.hosts[0].host,
                             '1.2.3.4')
            return TransactionMetadata()

        upstream.add_expectation(exp)

        tx = TransactionMetadata()
        tx.resolution = Resolution([HostPort('example.CoM', 25)])
        upstream_delta = filter.on_update(tx, tx.copy())

    async def test_dns(self):
        resolver = FakeResolver([
            mx_answer,
            a_answer,
            dns.resolver.NoAnswer()])  # AAAA

        filter = DnsResolutionFilter(
            suffix='',
            resolver=resolver)
        tx = TransactionMetadata()
        filter.wire_downstream(tx)

        async def upstream():
            return TransactionMetadata()

        delta = TransactionMetadata()
        delta.resolution = Resolution([HostPort('example.CoM', 25)])
        filter.downstream.merge_from(delta)
        await filter.update(delta, upstream)
        self.assertEqual([h.host for h in tx.resolution.hosts],
                         ['1.2.3.4'])

    def test_no_mx(self):
        upstream = FakeSyncFilter()
        resolver = FakeResolver([
            dns.resolver.NoAnswer(),
            a_answer,
            dns.resolver.NoAnswer()])  # AAAA

        filter = DnsResolutionFilter(
            upstream,
            suffix='',
            resolver=resolver)

        def exp(tx, delta):
            self.assertEqual([h.host for h in tx.resolution.hosts],
                             ['1.2.3.4'])
            return TransactionMetadata()

        upstream.add_expectation(exp)

        tx = TransactionMetadata()
        tx.resolution = Resolution([HostPort('mx.example.com', 25)])
        upstream_delta = filter.on_update(tx, tx.copy())

    def test_ipv6(self):
        upstream = FakeSyncFilter()
        resolver = FakeResolver([
            mx_answer,
            dns.resolver.NoAnswer(),  # A
            aaaa_answer])

        filter = DnsResolutionFilter(
            upstream,
            suffix='',
            resolver=resolver)

        def exp(tx, delta):
            self.assertEqual([h.host for h in tx.resolution.hosts],
                             ['0123:4567:89ab:cdef:0123:4567:89ab:cdef'])
            return TransactionMetadata()

        upstream.add_expectation(exp)

        tx = TransactionMetadata()
        tx.resolution = Resolution([HostPort('example.CoM', 25)])
        upstream_delta = filter.on_update(tx, tx.copy())

    def test_needs_resolution(self):
        upstream = FakeSyncFilter()
        resolver = FakeResolver()

        filter = DnsResolutionFilter(
            upstream,
            suffix='.net',
            resolver=resolver)

        def exp(tx, delta):
            self.assertEqual([h.host for h in tx.resolution.hosts],
                             ['1.2.3.4', 'example.com'])
            return TransactionMetadata()

        upstream.add_expectation(exp)

        tx = TransactionMetadata()
        tx.resolution = Resolution([HostPort('1.2.3.4', 25),
                                    HostPort('example.com', 25)])
        upstream_delta = filter.on_update(tx, tx.copy())


    def test_empty_result(self):
        resolver = FakeResolver([
            mx_answer,
            NXDOMAIN(),
            NXDOMAIN()])

        filter = DnsResolutionFilter(
            upstream=None,
            suffix='',
            resolver=resolver)

        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        tx.resolution = Resolution([HostPort('example.CoM', 25)])
        upstream_delta = filter.on_update(tx, tx.copy())
        self.assertTrue(tx.mail_response.code, 450)
        self.assertIn('empty', tx.mail_response.message)

    def test_servfail(self):
        resolver = FakeResolver([
            NoNameservers()])

        filter = DnsResolutionFilter(
            upstream=None,
            suffix='',
            resolver=resolver)

        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        tx.resolution = Resolution([HostPort('example.CoM', 25)])
        upstream_delta = filter.on_update(tx, tx.copy())
        self.assertTrue(tx.mail_response.code, 450)
        self.assertIn('empty', tx.mail_response.message)

    def test_duplicate_ip(self):
        upstream = FakeSyncFilter()

        filter = DnsResolutionFilter(
            upstream,
            static_resolution=Resolution([HostPort('1.2.3.4', 25)]),
            suffix='.example.com')

        def exp(tx, delta):
            self.assertEqual([h.host for h in tx.resolution.hosts],
                             ['1.2.3.4'])
            return TransactionMetadata()

        upstream.add_expectation(exp)

        tx = TransactionMetadata(
            resolution=Resolution([HostPort('us-west1.example.com', 25),
                                   HostPort('us-west2.example.com', 25)]))
        upstream_delta = filter.on_update(tx, tx.copy())


    def test_noop_ip(self):
        upstream = FakeSyncFilter()
        resolver = FakeResolver()

        filter = DnsResolutionFilter(
            upstream,
            suffix='',
            resolver=resolver)

        def exp(tx, delta):
            self.assertEqual(tx.resolution.hosts[0].host,
                             '1.2.3.4')
            return TransactionMetadata()

        upstream.add_expectation(exp)

        tx = TransactionMetadata(
            resolution=Resolution([HostPort('1.2.3.4', 25)]))
        upstream_delta = filter.on_update(tx, tx.copy())

    def test_noop_no_match(self):
        upstream = FakeSyncFilter()

        filter = DnsResolutionFilter(
            upstream,
            static_resolution=Resolution([HostPort('1.2.3.4', 25)]),
            literal='example1.com')

        def exp(tx, delta):
            self.assertEqual([h.host for h in tx.resolution.hosts],
                             ['example2.com', '1.2.3.4'])
            return TransactionMetadata()

        upstream.add_expectation(exp)

        tx = TransactionMetadata(
            resolution=Resolution([HostPort('example2.com', 25),
                                   HostPort('example1.com', 25)]))
        upstream_delta = filter.on_update(tx, tx.copy())



if __name__ == '__main__':
    unittest.main()
