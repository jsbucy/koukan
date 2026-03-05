# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import unittest
import logging

from email.headerregistry import Address
from koukan.message_validation_filter import MessageValidationFilterOutput

from koukan.address_list_policy import (
    AddressListPolicy,
    match_address_list)
from koukan.recipient_router_filter import Destination
from koukan.filter import Mailbox, TransactionMetadata

from koukan.matcher_result import MatcherResult

class AddressListPolicyTest(unittest.TestCase):
    def test_smoke(self) -> None:
        my_dest = Destination('http://my-endpoint')
        policy = AddressListPolicy(['example.com'], '+', ['alice'], my_dest)

        # invalid address never matches
        for addr in ['alice', 'alice@', '@example.com', '@@@']:
            dest, err = policy.endpoint_for_rcpt(addr)
            self.assertIsNone(dest)
            self.assertIsNone(err)

        for addr in ['alice@example.com',
                     'alice+@example.com',
                     'alice+a@example.com']:
            dest, err = policy.endpoint_for_rcpt(addr)
            self.assertEqual(dest, my_dest)
            self.assertIsNone(err)

        for addr in ['alice@example.comm',
                     'alice@e.example.com',
                     'alicee@example.com']:
            dest, err = policy.endpoint_for_rcpt(addr)
            self.assertIsNone(dest)
            self.assertIsNone(err)

        # delimiter = None -> exact match only
        policy = AddressListPolicy(['example.com'], None, ['alice'], my_dest)
        dest, err = policy.endpoint_for_rcpt('alice@example.com')
        self.assertEqual(dest, my_dest)
        self.assertIsNone(err)
        dest, err = policy.endpoint_for_rcpt('alicee@example.com')
        self.assertIsNone(dest)
        self.assertIsNone(err)

        # domains [] -> match any domain
        policy = AddressListPolicy([], '+', ['mailer-daemon'], my_dest)
        dest, err = policy.endpoint_for_rcpt('mailer-daemon@qqq.com')
        self.assertEqual(dest, my_dest)
        self.assertIsNone(err)
        dest, err = policy.endpoint_for_rcpt('qqq@qqq.com')
        self.assertIsNone(dest)
        self.assertIsNone(err)

        # prefixes [] -> match any local-part
        policy = AddressListPolicy(['example.com'], None, [], my_dest)
        dest, err = policy.endpoint_for_rcpt('qqq@example.com')
        self.assertEqual(dest, my_dest)
        self.assertIsNone(err)

        # dest == None -> reject
        policy = AddressListPolicy(['example.com'], None, [], None)
        dest, err = policy.endpoint_for_rcpt('qqq@example.com')
        self.assertIsNone(dest)
        assert err is not None
        self.assertEqual(err.code, 550)


    def test_matcher(self) -> None:
        match_yaml = {
            'which_addr': 'header_from',
            'domains': ['example.com'],
        }
        tx = TransactionMetadata()
        vo = MessageValidationFilterOutput()
        vo.parsed_header_from = Address(addr_spec='alice@example.com')
        tx.add_filter_output('koukan.message_validation_filter', vo)
        self.assertTrue(match_address_list(match_yaml, tx, rcpt_num=None))
        vo.parsed_header_from = Address(addr_spec='alice@ex.com')
        self.assertEqual(
            MatcherResult.NO_MATCH,
            match_address_list(match_yaml, tx, rcpt_num=None))

        vo.parsed_header_from = None
        tx.mail_from = Mailbox('alice@example.com')
        match_yaml['which_addr'] = 'mail_from'
        self.assertTrue(match_address_list(match_yaml, tx, rcpt_num=None))

        tx.mail_from = Mailbox('alice@ex.com')
        self.assertEqual(
            MatcherResult.NO_MATCH,
            match_address_list(match_yaml, tx, rcpt_num=None))


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d %(message)s')
    unittest.main()
