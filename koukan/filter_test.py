# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import unittest
import logging

from koukan.filter import (
    HostPort,
    Mailbox,
    TransactionMetadata,
    WhichJson )
from koukan.response import Response
from koukan.blob import InlineBlob
from koukan.deadline import Deadline

from koukan.fake_endpoints import MockAsyncFilter

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
        self.assertEqual(delta.to_json(WhichJson.REST_UPDATE), {
            'rcpt_to': [{'m': 'bob2'}],
            'rcpt_to_list_offset': 1 })

        merged = tx.merge(delta)
        self.assertEqual(merged.to_json(), {
            'mail_from': {'m': 'alice'},
            'rcpt_to': [{'m': 'bob'}, {'m': 'bob2'}]})

    def test_rest_placeholder(self):
        tx = TransactionMetadata(mail_from=Mailbox('alice'),
                                 rcpt_to=[Mailbox('bob')])
        rest_js = tx.to_json(WhichJson.REST_READ)
        self.assertEqual(rest_js, {
            'mail_from': {},
            'rcpt_to': [{}],
        })
        rest_tx_out = TransactionMetadata.from_json(
            rest_js, WhichJson.REST_READ)

        succ = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob'), Mailbox('bob2')])

        delta = tx.delta(succ)
        self.assertEqual(delta.to_json(WhichJson.REST_UPDATE), {
            'rcpt_to': [{'m': 'bob2'}],
            'rcpt_to_list_offset': 1 })

        merged = tx.merge(delta)
        self.assertEqual(merged.to_json(), {
            'mail_from': {'m': 'alice'},
            'rcpt_to': [{'m': 'bob'}, {'m': 'bob2'}]})

        logging.debug('test_rest_placeholder invalid delta: truncate rcpt_to')
        self.assertIsNotNone(merged.mail_from)
        self.assertEqual(len(merged.rcpt_to), 2)
        self.assertIsNone(merged.delta(
            TransactionMetadata(
                mail_from=None,
                rcpt_to=[None])), WhichJson.REST_READ)

        logging.debug('test_rest_placeholder invalid delta: non-placeholder')
        self.assertIsNone(merged.delta(
            TransactionMetadata(
                mail_from=None,
                rcpt_to=[None, Mailbox('bob2')])), WhichJson.REST_READ)


        logging.debug('test_rest_placeholder valid delta')

        succ = TransactionMetadata(
            mail_from=None,
            rcpt_to=[None, None],
            body=InlineBlob(b'xyz', last=True))

        delta = merged.delta(succ, WhichJson.REST_READ)
        self.assertEqual(delta.to_json(), {'body': {'inline': 'xyz'}})

        merged = merged.merge(delta)
        self.assertEqual(merged.to_json(), {
            'mail_from': {'m': 'alice'},
            'rcpt_to': [{'m': 'bob'}, {'m': 'bob2'}],
            'body': {'inline': 'xyz'}})


    def testBadRcpt(self):
        tx = TransactionMetadata(mail_from=Mailbox('alice'),
                                 rcpt_to=[Mailbox('bob')])
        succ = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob2'), Mailbox('bob2')])
        # not a prefix
        self.assertIsNone(tx.delta(succ))

    def testEmptyDict(self):
        tx = TransactionMetadata(retry = {})
        tx = TransactionMetadata.from_json(tx.to_json())
        self.assertEqual(tx.retry, {})

    def testRespFields(self):
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to = [Mailbox('bob1'), Mailbox('bob2')])
        self.assertTrue(tx.req_inflight())
        tx.mail_response=Response()
        self.assertTrue(tx.req_inflight())
        tx.rcpt_response=[Response()]
        self.assertTrue(tx.req_inflight())
        tx.rcpt_response.append(Response())
        self.assertFalse(tx.req_inflight())
        json = tx.to_json(WhichJson.REST_READ)
        logging.debug('testRespFields %s', json)
        self.assertEqual(json['mail_from'], {})
        self.assertEqual(json['rcpt_to'], [{}, {}])

    def test_fill_inflight_responses(self):
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to = [Mailbox('bob1'), Mailbox('bob2')],
            body_blob=InlineBlob(b'hello', last=True))
        tx.fill_inflight_responses(Response(123))
        self.assertEqual(tx.mail_response.code, 123)
        self.assertEqual([r.code for r in tx.rcpt_response], [123,123])
        self.assertEqual(tx.data_response.code, 123)

    def test_copy(self):
        tx = TransactionMetadata(
            rcpt_to=[Mailbox('alice')],
            rcpt_response=[Response(234)])
        tx2 = tx.copy()
        tx2.rcpt_to.append(Mailbox('alice'))
        tx2.rcpt_response.append(Response(456))
        self.assertEqual([m.mailbox for m in tx.rcpt_to], ['alice'])
        self.assertEqual([r.code for r in tx.rcpt_response], [234])

    def test_tx_bool(self):
        tx = TransactionMetadata()
        self.assertFalse(bool(tx))

        tx = TransactionMetadata(mail_from = Mailbox('alice'))
        self.assertTrue(bool(tx))

        tx = TransactionMetadata(rcpt_to = [])
        self.assertFalse(bool(tx))

        tx = TransactionMetadata(rcpt_to = [Mailbox('bob')])
        self.assertTrue(bool(tx))

        tx = TransactionMetadata(body_blob = InlineBlob(b'hello', last=True))
        self.assertTrue(bool(tx))

        tx = TransactionMetadata(retry = {})
        self.assertTrue(bool(tx))


    def test_idempotence(self):
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob1')])
        updated_tx = tx.copy()
        updated_tx.rcpt_to.append(Mailbox('bob2'))
        delta = tx.delta(updated_tx)
        self.assertIsNotNone(delta)
        self.assertEqual(delta.rcpt_to_list_offset, 1)
        self.assertEqual(
            [m.mailbox for m in delta.rcpt_to], ['bob2'])

        delta_json = delta.to_json(WhichJson.REST_UPDATE)
        self.assertEqual(delta_json, {
            'rcpt_to': [{'m': 'bob2'}],
            'rcpt_to_list_offset': 1
        })

        delta_from_json = TransactionMetadata.from_json(
            delta_json, WhichJson.REST_UPDATE)
        delta_copy = delta.copy_valid(WhichJson.REST_UPDATE)
        for d in [delta_from_json, delta_copy]:
            updated_tx = tx.copy()
            self.assertIsNotNone(updated_tx.merge_from(d))
            self.assertEqual(
                [m.mailbox for m in updated_tx.rcpt_to], ['bob1', 'bob2'])

        tx = updated_tx
        updated_tx = tx.copy()
        updated_tx.rcpt_response.append(Response(201))
        updated_tx.rcpt_response.append(Response(202))
        delta = tx.delta(updated_tx)
        self.assertEqual(delta.rcpt_response_list_offset, 0)

        merged = tx.merge(delta)
        self.assertEqual([r.code for r in merged.rcpt_response], [201,202])
        self.assertIsNone(merged.merge(delta))


if __name__ == '__main__':
    unittest.main()
