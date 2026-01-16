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
from koukan.fake_dns_wrapper import FakeResolver

ptr_message_text = """id 1234
opcode QUERY
rcode NOERROR
flags QR AA RD
;QUESTION
4.3.2.1.in-addr.arpa. IN PTR
;ANSWER
4.3.2.1.in-addr.arpa. 1 IN PTR example.com.
;AUTHORITY
;ADDITIONAL
"""

# this pattern of constructing Answer is cribbed from
# dnspython/tests/test_resolution.py
# but doesn't type check:
# error: Argument 4 to "Answer" has incompatible type "Message"; expected "QueryMessage"  [arg-type]

ptr_message = dns.message.from_text(ptr_message_text)
ptr_answer = Answer(
    dns.name.from_text('4.3.2.1.in-addr.arpa.'),
    RdataType.PTR,
    RdataClass.IN,
    ptr_message)  # type: ignore[arg-type]

a_message_text = """id 1234
opcode QUERY
rcode NOERROR
flags QR AA RD
;QUESTION
example.com. IN A
;ANSWER
example.com. 1 IN A 1.2.3.4
;AUTHORITY
;ADDITIONAL
"""

a_message = dns.message.from_text(a_message_text)
a_answer = Answer(
    dns.name.from_text('example.com.'),
    RdataType.A,
    RdataClass.IN,
    a_message)  # type: ignore[arg-type]


ptr6_message_text = """id 1234
opcode QUERY
rcode NOERROR
flags QR AA RD
;QUESTION
f.e.d.c.b.a.9.8.7.6.5.4.3.2.1.0.f.e.d.c.b.a.9.8.7.6.5.4.3.2.1.0.ip6.arpa. IN PTR
;ANSWER
f.e.d.c.b.a.9.8.7.6.5.4.3.2.1.0.f.e.d.c.b.a.9.8.7.6.5.4.3.2.1.0.ip6.arpa. IN PTR example.com.
;AUTHORITY
;ADDITIONAL
"""

ptr6_message = dns.message.from_text(ptr6_message_text)
ptr6_answer = Answer(
    dns.name.from_text('f.e.d.c.b.a.9.8.7.6.5.4.3.2.1.0.f.e.d.c.b.a.9.8.7.6.5.4.3.2.1.0.ip6.arpa.'),
    RdataType.PTR,
    RdataClass.IN,
    ptr6_message)  # type: ignore[arg-type]

aaaa_message_text = """id 1234
opcode QUERY
rcode NOERROR
flags QR AA RD
;QUESTION
example.com. IN AAAA
;ANSWER
example.com. 1 IN AAAA 0123:4567:89ab:cdef:0123:4567:89ab:cdef
;AUTHORITY
;ADDITIONAL
"""

aaaa_message = dns.message.from_text(aaaa_message_text)
aaaa_answer = Answer(
    dns.name.from_text('example.com.'),
    RdataType.AAAA,
    RdataClass.IN,
    aaaa_message)  # type: ignore[arg-type]

class RemoteHostFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
            '%(message)s')


    def _test(self, addr,
              answers : List[Union[Answer, Exception]],
              exp_hostname, exp_fcrdns, exp_err=False,
              exp_ehlo_alignment=False):
        resolver = FakeResolver(answers)
        filter = RemoteHostFilter(resolver)
        tx = TransactionMetadata()
        filter.wire_downstream(tx)

        prev = tx.copy()
        tx.smtp_meta = {'ehlo_host': 'example.com'}
        tx.remote_host = HostPort(addr, 12345)
        tx.mail_from = Mailbox('alice')
        tx.rcpt_to = [Mailbox('bob')]
        filter.on_update(prev.delta(tx))
        assert tx.filter_output is not None
        self.assertIsNotNone(
            rh := tx.filter_output[RemoteHostFilter.fullname()])
        self.assertTrue(
            rh.match({'fcrdns': exp_fcrdns is True}))
        self.assertTrue(
            rh.match({'ehlo_alignment': exp_ehlo_alignment}))
        logging.info('%s %s', rh.remote_hostname, rh.fcrdns)
        if exp_err:
            assert tx.mail_response is not None
            self.assertEqual(450, tx.mail_response.code)
        else:
            self.assertIsNone(tx.mail_response)
        self.assertEqual(exp_hostname, rh.remote_hostname)
        self.assertEqual(exp_fcrdns, rh.fcrdns)

    def test_success_ipv4(self):
        self._test(
            '1.2.3.4',
            [ptr_answer, a_answer],
            'example.com.', True, exp_ehlo_alignment=True)

    def test_success_ipv6(self):
        self._test(
            '0123:4567:89ab:cdef:0123:4567:89ab:cdef',
            [ptr6_answer, aaaa_answer],
            'example.com.', True, exp_ehlo_alignment=True)

    def test_nx_ptr(self):
        self._test('1.2.3.4',
                   [dns.resolver.NXDOMAIN()],
                   '', False)

    def test_nx_fwd(self):
        self._test('1.2.3.4',
                   [ptr_answer,
                    dns.resolver.NXDOMAIN(),   # A
                    dns.resolver.NXDOMAIN()],  # AAAA
                   'example.com.', False)

    def test_servfail_ptr(self):
        self._test('1.2.3.4',
                   [dns.resolver.NoNameservers()],
                   None, None,
                   exp_err=True)

    def test_servfail_fwd(self):
        self._test('1.2.3.4',
                   [ptr_answer,
                    dns.resolver.NoNameservers(),   # A
                    dns.resolver.NoNameservers()],  # AAAA
                   'example.com.', False,
                   exp_err=True)


if __name__ == '__main__':
    unittest.main()
