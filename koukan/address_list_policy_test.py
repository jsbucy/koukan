import unittest
import logging

from koukan.address_list_policy import AddressListPolicy
from koukan.recipient_router_filter import Destination

class AddressListPolicyTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

    def test_smoke(self):
        my_dest = Destination('http://my-endpoint')
        policy = AddressListPolicy(['example.com'], '+', ['alice'], my_dest)

        for addr in ['@@@', 'alice']:
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
        self.assertEqual(err.code, 550)

if __name__ == '__main__':
    unittest.main()
