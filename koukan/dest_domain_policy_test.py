# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import unittest
import logging

from email.headerregistry import Address
from koukan.message_validation_filter import MessageValidationFilterOutput

from koukan.dest_domain_policy import DestDomainPolicy
from koukan.recipient_router_filter import Destination
from koukan.filter import Mailbox, TransactionMetadata

from koukan.matcher_result import MatcherResult

class DestDomainPolicyTest(unittest.TestCase):
    def test_smoke(self):
        dest = Destination('http://gateway')
        policy = DestDomainPolicy(dest)
        rcpt_dest, resp = policy.endpoint_for_rcpt('alice@example.com')
        self.assertEqual(dest.rest_endpoint, rcpt_dest.rest_endpoint)

        for rcpt in ['alice', 'alice@', '@example.com', '@@@']:
            rcpt_dest, resp = policy.endpoint_for_rcpt(rcpt)
            self.assertIsNone(rcpt_dest)
            self.assertIsNone(resp)

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d %(message)s')
    unittest.main()
