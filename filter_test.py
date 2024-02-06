import unittest
import logging

from filter import HostPort, Mailbox, TransactionMetadata, WhichJson

class FilterTest(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')


    def testSingleField(self):
        tx = TransactionMetadata()
        js = tx.to_json()
        self.assertEqual(js, {})

        self.assertIsNone(TransactionMetadata.from_json({'mail_from': None}))
        self.assertIsNone(TransactionMetadata.from_json({'rcpt_to': []}))

        succ = TransactionMetadata(mail_from=Mailbox('alice'))
        delta = tx.delta(succ)
        self.assertEqual(delta.rcpt_to, [])
        self.assertEqual(delta.to_json(), {'mail_from': {'m': 'alice'}})

        merged = tx.merge(delta)
        self.assertEqual(merged.to_json(), {'mail_from': {'m': 'alice'}})

    def testBadSingleField(self):
        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        succ = TransactionMetadata(mail_from=Mailbox('bob'))
        self.assertIsNone(tx.delta(succ))

    def testRcptFirst(self):
        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        succ = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')])

        delta = tx.delta(succ)
        self.assertEqual(delta.to_json(), {'rcpt_to': [{'m': 'bob'}]})

        merged = tx.merge(delta)
        self.assertEqual(merged.to_json(), {'mail_from': {'m': 'alice'},
                                            'rcpt_to': [{'m': 'bob'}]})

    def testRcptAppend(self):
        tx = TransactionMetadata(mail_from=Mailbox('alice'),
                                 rcpt_to=[Mailbox('bob')])
        succ = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob'), Mailbox('bob2')])

        delta = tx.delta(succ)
        self.assertEqual(delta.to_json(), {'rcpt_to': [{'m': 'bob2'}]})

        merged = tx.merge(delta)
        self.assertEqual(merged.to_json(), {
            'mail_from': {'m': 'alice'},
            'rcpt_to': [{'m': 'bob'}, {'m': 'bob2'}]})

    def testBadRcpt(self):
        tx = TransactionMetadata(mail_from=Mailbox('alice'),
                                 rcpt_to=[Mailbox('bob')])
        succ = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob2'), Mailbox('bob2')])
        # not a prefix
        self.assertIsNone(tx.delta(succ))

    def testEmptyStr(self):
        tx = TransactionMetadata.from_json({'body': ''}, WhichJson.REST_UPDATE)
        self.assertEqual(tx.body, '')

if __name__ == '__main__':
    unittest.main()
