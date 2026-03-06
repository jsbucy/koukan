# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import unittest
import logging

from koukan.address import domain_from_address


class AddressListPolicyTest(unittest.TestCase):
    def test_smoke(self) -> None:
        for addr in ['alice@example.com', 'アリス@example.com']:
            self.assertEqual('example.com',
                             domain_from_address(addr))

        for addr in ['alice', 'alice@', '@example.com', '@@@']:
            self.assertIsNone(domain_from_address(addr))


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d %(message)s')
    unittest.main()
