# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0

import logging
import unittest
import tempfile

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

from koukan.message_validation_filter import (
    MessageValidationFilter,
    MessageValidationFilterResult )


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

        valid = MessageValidationFilterResult()
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

        logging.debug(out.to_json(WhichJson.DB_ATTEMPT))

        self.assertEqual(
            MatcherResult.MATCH,
            out.match({'alignment': 'domain',
                       'status': 'dkim_pass'}))
        self.assertEqual(
            MatcherResult.NO_MATCH,
            out.match({'alignment': 'domain',
                       'status': 'temp_err'}))

        self.assertEqual(
            MatcherResult.MATCH,
            out.match({'status': 'dkim_pass',
                       'domains': ['somewhere-else.com']}))
        self.assertEqual(
            MatcherResult.NO_MATCH,
            out.match({'status': 'dkim_pass',
                       'domains': ['somewhere-else.org']}))



if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d %(message)s')

    unittest.main()
