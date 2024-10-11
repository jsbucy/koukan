# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import List, Union
import unittest
import logging

from dns.resolver import Answer
from dns.rrset import RRset
from dns.rdataclass import RdataClass
from dns.rdatatype import RdataType
import dns.rrset
from dns.message import QueryMessage
import dns.flags
from dns.rdtypes.ANY.PTR import PTR
import dns.name

from koukan.filter import HostPort, Mailbox, TransactionMetadata
from koukan.response import Response
from koukan.remote_host_filter import (
    RemoteHostFilter,
    Resolver )
from koukan.fake_endpoints import FakeSyncFilter
from koukan.fake_dns_wrapper import FakeResolver

ptr_message_text = """id 1234
opcode QUERY
rcode NOERROR
flags QR AA RD
;QUESTION
4.3.2.1.in-addr.arpa. IN PTR
;ANSWER
4.3.2.1.in-addr.arpa. 1 IN PTR tachygraph.gloop.org.
;AUTHORITY
;ADDITIONAL
"""

ptr_message = dns.message.from_text(ptr_message_text)
ptr_answer = Answer(
    dns.name.from_text('4.3.2.1.in-addr.arpa.'),
    RdataType.PTR,
    RdataClass.IN,
    ptr_message)

a_message_text = """id 1234
opcode QUERY
rcode NOERROR
flags QR AA RD
;QUESTION
tachygraph.gloop.org. IN A
;ANSWER
tachygraph.gloop.org. 1 IN A 1.2.3.4
;AUTHORITY
;ADDITIONAL
"""

a_message = dns.message.from_text(a_message_text)
a_answer = Answer(
    dns.name.from_text('sandbox.gloop.org.'),
    RdataType.A,
    RdataClass.IN,
    a_message)


ptr6_message_text = """id 1234
opcode QUERY
rcode NOERROR
flags QR AA RD
;QUESTION
f.e.d.c.b.a.9.8.7.6.5.4.3.2.1.0.f.e.d.c.b.a.9.8.7.6.5.4.3.2.1.0.ip6.arpa. IN PTR
;ANSWER
f.e.d.c.b.a.9.8.7.6.5.4.3.2.1.0.f.e.d.c.b.a.9.8.7.6.5.4.3.2.1.0.ip6.arpa. IN PTR tachygraph.gloop.org.
;AUTHORITY
;ADDITIONAL
"""

ptr6_message = dns.message.from_text(ptr6_message_text)
ptr6_answer = Answer(
    dns.name.from_text('f.e.d.c.b.a.9.8.7.6.5.4.3.2.1.0.f.e.d.c.b.a.9.8.7.6.5.4.3.2.1.0.ip6.arpa.'),
    RdataType.PTR,
    RdataClass.IN,
    ptr6_message)

aaaa_message_text = """id 1234
opcode QUERY
rcode NOERROR
flags QR AA RD
;QUESTION
tachygraph.gloop.org. IN AAAA
;ANSWER
tachygraph.gloop.org. 1 IN AAAA 0123:4567:89ab:cdef:0123:4567:89ab:cdef
;AUTHORITY
;ADDITIONAL
"""

aaaa_message = dns.message.from_text(aaaa_message_text)
aaaa_answer = Answer(
    dns.name.from_text('sandbox.gloop.org.'),
    RdataType.AAAA,
    RdataClass.IN,
    aaaa_message)

class RemoteHostFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')


    def _test(self, addr,
              answers : List[Union[Answer, Exception]],
              exp_hostname, exp_fcrdns, exp_resp=201):

        resolver = FakeResolver(answers)

        upstream = FakeSyncFilter()
        def exp(tx, tx_delta):
            self.assertEqual(exp_hostname, tx.remote_hostname)
            self.assertEqual(exp_fcrdns, tx.fcrdns)
            self.assertEqual(exp_hostname, tx_delta.remote_hostname)
            self.assertEqual(exp_fcrdns, tx_delta.fcrdns)

            upstream_delta = TransactionMetadata(
                mail_response=Response(201))
            self.assertIsNotNone(tx.merge_from(upstream_delta))
            return upstream_delta
        upstream.add_expectation(exp)

        tx = TransactionMetadata(
                remote_host=HostPort(addr, 12345),
                mail_from=Mailbox('alice'))
        filter = RemoteHostFilter(upstream, resolver)
        upstream_delta = filter.on_update(tx, tx.copy())
        logging.info('%s %s', tx.remote_hostname, tx.fcrdns)
        self.assertEqual(exp_resp, tx.mail_response.code)
        self.assertEqual(exp_resp, upstream_delta.mail_response.code)

    def test_success_ipv4(self):
        self._test(
            '1.2.3.4',
            [ptr_answer, a_answer],
            'tachygraph.gloop.org.', True)

    def test_success_ipv6(self):
        self._test(
            '0123:4567:89ab:cdef:0123:4567:89ab:cdef',
            [ptr6_answer, aaaa_answer],
            'tachygraph.gloop.org.', True)

    def test_nx_ptr(self):
        self._test('1.2.3.4',
                   [dns.resolver.NXDOMAIN()],
                   '', False)

    def test_nx_fwd(self):
        self._test('1.2.3.4',
                   [ptr_answer,
                    dns.resolver.NXDOMAIN(),   # A
                    dns.resolver.NXDOMAIN()],  # AAAA
                   'tachygraph.gloop.org.', False)

    def test_servfail_ptr(self):
        self._test('1.2.3.4',
                   [dns.resolver.NoNameservers()],
                   None, None,
                   exp_resp=450)

    def test_servfail_fwd(self):
        self._test('1.2.3.4',
                   [ptr_answer,
                    dns.resolver.NoNameservers(),   # A
                    dns.resolver.NoNameservers()],  # AAAA
                   'tachygraph.gloop.org.', False,
                   exp_resp=450)


if __name__ == '__main__':
    unittest.main()
