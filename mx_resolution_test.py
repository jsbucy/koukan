import unittest
import logging

from mx_resolution import DnsResolutionFilter
from filter import HostPort, Resolution, TransactionMetadata
from fake_endpoints import FakeSyncFilter
from fake_dns_wrapper import FakeResolver

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


class DnsResolutionFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

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

    def test_dns(self):
        upstream = FakeSyncFilter()
        resolver = FakeResolver()
        resolver.answers = [mx_answer,
                            a_answer,
                            dns.resolver.NoAnswer()]  # aaaa

        filter = DnsResolutionFilter(
            upstream,
            suffix='',
            resolver=resolver)

        def exp(tx, delta):
            self.assertEqual(tx.resolution.hosts[0].host,
                             '1.2.3.4')
            return TransactionMetadata()

        upstream.add_expectation(exp)

        tx = TransactionMetadata()
        tx.resolution = Resolution([HostPort('example.CoM', 25)])
        upstream_delta = filter.on_update(tx, tx.copy())


if __name__ == '__main__':
    unittest.main()
