# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0

from functools import partial
import logging
import unittest
import tempfile
import time

import socket
from email.headerregistry import Address, AddressHeader

from dkim import dknewkey, sign


from koukan.filter import HostPort, Mailbox, TransactionMetadata
from koukan.dkim_check_filter import (
    Alignment,
    DkimCheckFilter,
    DkimCheckFilterOutput,
    Status )
from koukan.matcher_result import MatcherResult
from koukan.rest_schema import WhichJson

from koukan.blob import InlineBlob
from koukan.executor import Executor

from koukan.message_validation_filter import (
    MessageValidationFilter,
    MessageValidationFilterOutput )
from koukan.fake_endpoints import FakeTxGroup


class DkimCheckFilterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.TemporaryDirectory()
        dir = cls.tempdir.name
        cls.privkey_filename = dir + '/privkey'
        cls.pubkey_filename = dir + '/pubkey'
        dknewkey.GenRSAKeys(cls.privkey_filename)
        dknewkey.ExtractRSADnsPublicKey(cls.privkey_filename, cls.pubkey_filename)
        with open(cls.privkey_filename, 'rb') as priv:
            cls.privkey = priv.read()
        with open(cls.pubkey_filename, 'rb') as pub:
            cls.pubkey = pub.read()

    @classmethod
    def tearDownClass(cls):
        cls.tempdir.cleanup()

    def setUp(self):
        self.maxDiff = 4096

    def dns(self, name, timeout):
        logging.debug(name)
        if name == b'my-selector._domainkey.example.com.':
            return self.pubkey
        if name == b'my-selector._domainkey.lists.example.com.':
            return self.pubkey
        if name == b'my-selector._domainkey.somewhere-else.com.':
            return self.pubkey
        return None

    def test_smoke(self):
        with open('testdata/trivial.msg', 'rb') as message_file:
            message = message_file.read()

        sig = sign(message, b'my-selector', b'example.com', self.privkey)
        sig = sig.replace(b'a=rsa-sha256;', b'a=rsa-sha1024;')
        message = sig + message

        for domain in [b'lists.example.com', b'example.com', b'somewhere-else.com']:
            sig = sign(message, b'my-selector', domain, self.privkey)
            message = sig + message

        logging.debug(message)

        valid = MessageValidationFilterOutput()
        valid.parsed_header_from = Address(addr_spec='alice@example.com')

        f = DkimCheckFilter(self.dns)
        tx = TransactionMetadata()
        f.wire_downstream(tx)
        prev = tx.copy()
        tx.mail_from = Mailbox('alice@example.com')
        tx.body = InlineBlob(message, last=True)
        tx.add_filter_output(MessageValidationFilter.fullname(), valid)
        f.on_update(prev.delta(tx))

        out = tx.get_filter_output(f.fullname())
        self.assertEqual(4, len(out.results))
        r0 = out.results[1]
        self.assertEqual(Status.dkim_pass, r0.status)
        self.assertEqual(Alignment.domain, r0.alignment)
        self.assertEqual('example.com', r0.domain)
        self.assertEqual(['from', 'subject', 'message-id', 'date', 'from'],
                         r0.headers)
        self.assertLess(abs(r0.timestamp - time.time()), 5)

        r1 = out.results[2]
        self.assertEqual(Status.dkim_pass, r1.status)
        self.assertEqual(Alignment.same_sld, r1.alignment)
        self.assertEqual('lists.example.com', r1.domain)

        r2 = out.results[3]
        self.assertEqual(Status.unknown_algo, r2.status)

        r3 = out.results[0]
        self.assertEqual(Status.dkim_pass, r3.status)
        self.assertEqual(Alignment.other, r3.alignment)
        self.assertEqual('somewhere-else.com', r3.domain)

        js = out.to_json(WhichJson.DB_ATTEMPT)
        self.assertEqual(['from', 'subject', 'message-id', 'date', 'from'],
                         js['results'][1]['headers'])

        self.assertEqual(
            MatcherResult.MATCH,
            out.match({'alignment': 'domain',
                       'status': 'dkim_pass'},
                      rcpt_num=None))
        self.assertEqual(
            MatcherResult.NO_MATCH,
            out.match({'alignment': 'domain',
                       'status': 'temp_err'},
                      rcpt_num=None))
        self.assertEqual(
            MatcherResult.MATCH,
            out.match({'status': 'dkim_pass',
                       'domains': ['somewhere-else.com']},
                      rcpt_num=None))
        self.assertEqual(
            MatcherResult.NO_MATCH,
            out.match({'status': 'dkim_pass',
                       'domains': ['somewhere-else.org']},
                      rcpt_num=None))

    def test_fixup_tags(self):
        f = DkimCheckFilter(self.dns)

        result = DkimCheckFilterOutput.Result()
        f._fixup_tags(b't=12345;x=-2', result)
        logging.debug(result.tags)
        self.assertEqual(12345, result.timestamp)
        self.assertIsNone(result.expiration)
        self.assertEqual('-2', result.tags['x'])
        self.assertEqual({'status': 2, 'timestamp': 12345,
                          'tags': {'x': '-2'}},
                         result.to_json())

        result = DkimCheckFilterOutput.Result()
        f._fixup_tags(b'h=from : subject : message-id : date : from;\r\n',
                      result)
        self.assertEqual(
            ['from', 'subject', 'message-id', 'date', 'from'],
            result.headers)
        self.assertEqual(
            {'status': 2,
             'headers': ['from', 'subject', 'message-id', 'date', 'from']},
            result.to_json())

    def test_invalid_rfc822(self):
        tx = TransactionMetadata(
            body=InlineBlob(b'hello, world!', last=True))
        f = DkimCheckFilter(self.dns)
        f.wire_downstream(tx)
        f.on_update(tx)
        filter_output = tx.get_filter_output(f.fullname())
        self.assertEqual(0, len(filter_output.results))

    def test_group_reuse(self):
        tx0 = TransactionMetadata(group_index=0)
        tx1 = TransactionMetadata(group_index=1)

        group = tx0.group = tx1.group = FakeTxGroup([tx0, tx1])

        f0 = DkimCheckFilter(self.dns)
        f0.wire_downstream(tx0)

        f0.on_update(TransactionMetadata(
            body=InlineBlob(b'Subject: hello\r\n\r\nworld!\r\n', last=True)))
        filter_output = tx0.get_filter_output(f0.fullname())

        f1 = DkimCheckFilter(self.dns)
        f1.wire_downstream(tx1)

        f1.on_update(TransactionMetadata(
            body=InlineBlob(b'Hello, world!', last=True)))

        out1 = tx1.get_filter_output(f1.fullname())
        self.assertIs(out1, filter_output)

    def test_inflight_waiting(self):
        tx0 = TransactionMetadata(group_index=0)
        tx1 = TransactionMetadata(group_index=1)

        group = tx0.group = tx1.group = FakeTxGroup([tx0, tx1])

        f0 = DkimCheckFilter(self.dns)
        f0.wire_downstream(tx0)

        f1 = DkimCheckFilter(self.dns)
        f1.wire_downstream(tx1)

        group.maybe_start_inflight(f1.fullname(), 0)

        executor = Executor(inflight_limit=10)
        fut = executor.submit(
            partial(f1.on_update, TransactionMetadata(
            body=InlineBlob(b'Hello, world!', last=True))))

        filter_output = DkimCheckFilterOutput()
        tx0.add_filter_output(f1.fullname(), filter_output)
        group.set_done(f1.fullname())

        fut.result()
        out1 = tx1.get_filter_output(f1.fullname())
        self.assertIs(out1, filter_output)



if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d %(message)s')

    unittest.main()
