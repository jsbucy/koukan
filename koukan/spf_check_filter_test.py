# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0

import logging
import unittest

import socket
import spf

from koukan.filter import HostPort, Mailbox, TransactionMetadata
from koukan.spf_check_filter import SpfCheckFilter, SpfCheckFilterOutput
from koukan.matcher_result import MatcherResult
from koukan.rest_schema import WhichJson

def fake_dns_lookup(name,qtype,strict=True,timeout=None,level=0):
    logging.debug('%s %s', name, qtype)
    if name == 'mx.example.com':
        if qtype == 'TXT':
            return [((name, qtype), ('v=spf1 a -all'))]
        elif qtype == 'A':
            return [((name, qtype),
                     (socket.inet_pton(socket.AF_INET, '1.2.3.4')))]
    if name == 'example.com':
        return [((name, qtype), ('v=spf1 ip4:1.2.3.0/24 -all'))]
    if name == 'inbound-gateway.local':
        return [((name, qtype), ('v=spf1 ip4:4.3.2.0/24 -all'))]
    return []

class SpfCheckFilterTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = 4096
        self._dnslookup = spf.DNSLookup

    def tearDown(self):
        spf.DNSLookup = self._dnslookup

    def test_mail_from_fail(self):
        spf.DNSLookup = fake_dns_lookup

        f = SpfCheckFilter([])
        tx = TransactionMetadata()
        f.wire_downstream(tx)
        prev = tx.copy()
        tx.remote_host = HostPort('1.2.5.4', 25000)
        tx.smtp_meta = {'ehlo_host': 'mx.example.com'}
        tx.mail_from = Mailbox('alice@example.com')
        f.on_update(prev.delta(tx))

        out = tx.get_filter_output(f.fullname())
        self.assertEqual(SpfCheckFilterOutput.Status.fail, out.mail_from_result)
        self.assertEqual(MatcherResult.NO_MATCH,
                         out.match({'mail_from_result': 'pass'}, rcpt_num=None))
        self.assertEqual(MatcherResult.MATCH,
                         out.match({'mail_from_result': 'fail'}, rcpt_num=None))

        self.assertEqual(
            {'mail_from_result': 3},
            out.to_json(WhichJson.DB_ATTEMPT))

    def test_mail_from_pass(self):
        spf.DNSLookup = fake_dns_lookup

        f = SpfCheckFilter([])
        tx = TransactionMetadata()
        f.wire_downstream(tx)
        prev = tx.copy()
        tx.remote_host = HostPort('1.2.3.4', 25000)
        tx.smtp_meta = {'ehlo_host': 'mx.example.com'}
        tx.mail_from = Mailbox('alice@example.com')
        f.on_update(prev.delta(tx))

        out = tx.get_filter_output(f.fullname())
        self.assertEqual(SpfCheckFilterOutput.Status.spf_pass,
                         out.mail_from_result)
        self.assertEqual(
            MatcherResult.MATCH,
            out.match({'mail_from_result': 'pass'}, rcpt_num=None))

        prev = tx.copy()
        tx.rcpt_to.append(Mailbox('bob@example.com'))
        f.on_update(prev.delta(tx))

    def test_ehlo_pass(self):
        spf.DNSLookup = fake_dns_lookup

        f = SpfCheckFilter([])
        tx = TransactionMetadata()
        f.wire_downstream(tx)
        prev = tx.copy()
        tx.remote_host = HostPort('1.2.3.4', 25000)
        tx.smtp_meta = {'ehlo_host': 'mx.example.com'}
        tx.mail_from = Mailbox('')
        f.on_update(prev.delta(tx))

        out = tx.get_filter_output(f.fullname())
        self.assertEqual(SpfCheckFilterOutput.Status.spf_pass,
                         out.mail_from_result)
        self.assertEqual(
            MatcherResult.MATCH,
            out.match({'mail_from_result': 'pass'}, rcpt_num=None))

    def test_extra_domains(self):
        spf.DNSLookup = fake_dns_lookup

        f = SpfCheckFilter(['inbound-gateway.local'])
        tx = TransactionMetadata()
        f.wire_downstream(tx)
        prev = tx.copy()
        tx.remote_host = HostPort('4.3.2.1', 25000)
        tx.smtp_meta = {'ehlo_host': 'mx.example.com'}
        tx.mail_from = Mailbox('alice@somewhere-else.local')
        f.on_update(prev.delta(tx))

        out = tx.get_filter_output(f.fullname())
        self.assertEqual(MatcherResult.MATCH,
                         out.match({'mail_from_result': 'none'}, rcpt_num=None))
        self.assertEqual(
            MatcherResult.MATCH,
            out.match({'extra_domain': 'inbound-gateway.local',
                       'extra_domain_result': 'pass'}, rcpt_num=None))
        self.assertEqual(
            MatcherResult.NO_MATCH,
            out.match({'extra_domain': 'inbound-gateway.local',
                       'extra_domain_result': 'fail'}, rcpt_num=None))

        js = out.to_json(WhichJson.DB_ATTEMPT)
        logging.debug(js)
        self.assertEqual(
            {'extra_domains_results': {'inbound-gateway.local': 1},
             'mail_from_result': 5},
            js)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d %(message)s')

    unittest.main()
