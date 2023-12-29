import unittest
import logging

from filter import Mailbox, TransactionMetadata

class FilterTest(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

    def testBasic(self):
        orig = TransactionMetadata()
        orig.host = 'host'
        delta = TransactionMetadata()
        merged = orig.merge(delta)
        self.assertIsNotNone(merged)
        orig.mail_from = Mailbox('alice')
        self.assertEqual(orig.to_json(),
                         {'mail_from': {'m': 'alice'}, 'host': 'host'})

        orig = TransactionMetadata.from_json(
            {'mail_from': {'m': 'alice'}, 'host': 'host'})
        delta.host = 'host2'
        merged = orig.merge(delta)
        self.assertIsNone(merged)

    def testDelta(self):
        orig = TransactionMetadata()
        self.assertFalse(orig)
        orig.host = 'host'
        next = TransactionMetadata()
        next.host = orig.host
        next.local_host = 'local_host'
        delta = orig.delta(next)
        self.assertIsNotNone(delta)
        self.assertTrue(delta)
        self.assertFalse(hasattr(delta, 'host') and getattr(delta, 'host'))
        self.assertEqual(delta.local_host, 'local_host')

        del next.host
        delta = orig.delta(next)
        self.assertIsNone(delta)


if __name__ == '__main__':
    unittest.main()
