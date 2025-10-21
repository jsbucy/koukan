# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import unittest
import logging

from koukan.filter import (
    HostPort,
    Mailbox,
    TransactionMetadata,
    WhichJson,
    body_from_json,
    body_to_json )
from koukan.response import Response
from koukan.blob import InlineBlob
from koukan.deadline import Deadline
from koukan.message_builder import MessageBuilderSpec
from koukan.rest_schema import BlobUri
from koukan.storage_schema import BlobSpec

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
        delta = tx.maybe_delta(succ)
        self.assertEqual(delta.rcpt_to, [])
        self.assertEqual(delta.to_json(), {'mail_from': {'m': 'alice'}})

        merged = tx.merge(delta)
        self.assertEqual(merged.to_json(), {'mail_from': {'m': 'alice'}})

    def testBadSingleField(self):
        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        succ = TransactionMetadata(mail_from=Mailbox('bob'))
        self.assertIsNone(tx.maybe_delta(succ))

    def testRcptFirst(self):
        tx = TransactionMetadata(mail_from=Mailbox('alice'))
        succ = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')])

        delta = tx.maybe_delta(succ)
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

        delta = tx.maybe_delta(succ)
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

        delta = tx.maybe_delta(succ)
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
        self.assertIsNone(merged.maybe_delta(
            TransactionMetadata(
                mail_from=None,
                rcpt_to=[None])), WhichJson.REST_READ)

        logging.debug('test_rest_placeholder invalid delta: non-placeholder')
        self.assertIsNone(merged.maybe_delta(
            TransactionMetadata(
                mail_from=None,
                rcpt_to=[None, Mailbox('bob2')])), WhichJson.REST_READ)


        logging.debug('test_rest_placeholder valid delta')

        succ = TransactionMetadata(
            mail_from=None,
            rcpt_to=[None, None],
            body=InlineBlob(b'xyz', last=True))

        delta = merged.maybe_delta(succ, WhichJson.REST_READ)
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
        self.assertIsNone(tx.maybe_delta(succ))

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
        tx.body = InlineBlob(b'hello')
        self.assertFalse(tx.req_inflight())
        tx.body = InlineBlob(b'hello', last=True)
        self.assertTrue(tx.req_inflight())
        tx.data_response = Response(250)
        self.assertFalse(tx.req_inflight())

    def test_fill_inflight_responses(self):
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to = [Mailbox('bob1'), Mailbox('bob2')],
            body=InlineBlob(b'hello', last=True))
        tx.fill_inflight_responses(Response(123))
        self.assertEqual(tx.mail_response.code, 123)
        self.assertEqual([r.code for r in tx.rcpt_response], [503,503])
        self.assertEqual(tx.data_response.code, 503)

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

        tx = TransactionMetadata(body = InlineBlob(b'hello', last=True))
        self.assertTrue(bool(tx))

        tx = TransactionMetadata(retry = {})
        self.assertTrue(bool(tx))


    def test_idempotence(self):
        tx = TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob1')])
        updated_tx = tx.copy()
        updated_tx.rcpt_to.append(Mailbox('bob2'))
        delta = tx.maybe_delta(updated_tx)
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
        delta = tx.maybe_delta(updated_tx)
        self.assertEqual(delta.rcpt_response_list_offset, 0)

        merged = tx.merge(delta)
        self.assertEqual([r.code for r in merged.rcpt_response], [201,202])
        self.assertIsNone(merged.merge(delta))


    def test_copy_valid(self):
        rcpt = Mailbox('bob1')
        rcpt.routed = True
        tx = TransactionMetadata(mail_from=Mailbox('alice'), rcpt_to=[rcpt])
        all = tx.copy_valid(WhichJson.ALL)
        self.assertTrue(all.rcpt_to[0].routed)
        db = tx.copy_valid(WhichJson.DB)
        self.assertFalse(db.rcpt_to[0].routed)

    def test_check_preconditions(self):
        tx = TransactionMetadata(
            mail_from = Mailbox('alice'),
            mail_response = Response(450),
            rcpt_to = [Mailbox('bob')],
            body = InlineBlob('hello, world!'))
        self.assertFalse(tx.check_preconditions())
        self.assertEqual([503], [r.code for r in tx.rcpt_response])
        self.assertEqual(503, tx.data_response.code)

        tx = TransactionMetadata(
            mail_from = Mailbox('alice'),
            mail_response = Response(201),
            rcpt_to = [Mailbox('bob')],
            rcpt_response = [Response(202)],
            body = InlineBlob('hello, world!'),
            data_response=Response(403))
        self.assertFalse(tx.check_preconditions())


    # these 3 are basically the same?
    # gw -> router: no body in req, router returns body_uri
    # router -> gw: no body in req, router returns body_uri
    # rest client -> router: no body in req, router returns body_uri
    def test_body_json_body_uri(self):
        blob_spec = BlobSpec(
            reuse_uri=BlobUri(
                tx_id='123', tx_body=True,
                parsed_uri='http://router/transactions/123/body'))
        self.assertEqual({
            'blob_status': {'uri': '/transactions/123/body'}
        }, body_to_json(blob_spec, WhichJson.REST_READ))

    # rest client -> router: reuse body uri in req
    def test_body_json_reuse_body_uri(self):
        js = {
            'reuse_uri': '/transactions/123/body'
        }
        body = body_from_json(js, WhichJson.REST_CREATE)
        logging.debug(body)
        self.assertIsInstance(body, BlobSpec)
        self.assertEqual('123', body.reuse_uri.tx_id)
        self.assertEqual(True, body.reuse_uri.tx_body)

    # rest client -> router: message builder spec in req, new blobs
    def test_body_json_message_builder_new(self):
        # client req
        js = {
            'message_builder': {
                'headers': [],
                'text_body': [
                  {'content': {'create_id': 'blob1'},
                   'content_type': 'text/plain'}]
            }
        }
        body = body_from_json(js, WhichJson.REST_CREATE)
        logging.debug(body)
        self.assertIsInstance(body, MessageBuilderSpec)
        self.assertEqual({'blob1'}, body.blobs.keys())
        self.assertIsInstance(body.blobs['blob1'], BlobSpec)
        self.assertEqual('blob1', body.blobs['blob1'].create_id)

        body.blobs['blob1'].reuse_uri = BlobUri(tx_id='123', blob='blob1')

        # server resp
        js = body_to_json(body, WhichJson.REST_READ)
        logging.debug(js)
        self.assertEqual({
            'message_builder': {'blob_status': {
                'blob1': {'uri': '/transactions/123/blob/blob1'}}}
        }, js)

    # rest client -> router: message builder spec in req, reuse blobs
    def test_body_json_message_builder_reuse(self):
        # client req
        js = {
            'message_builder': {
                'headers': [],
                'text_body': [
                  {'content': {'reuse_uri': '/transactions/122/blob/blob1'},
                   'content_type': 'text/plain'}]
            }
        }
        body = body_from_json(js, WhichJson.REST_CREATE)
        logging.debug(body)
        self.assertIsInstance(body, MessageBuilderSpec)
        self.assertEqual({'blob1'}, body.blobs.keys())
        self.assertIsInstance(body.blobs['blob1'], BlobSpec)
        self.assertEqual('blob1', body.blobs['blob1'].reuse_uri.blob)

        body.blobs['blob1'] = InlineBlob(b'hello', last=True)

        # server resp
        js = body_to_json(body, WhichJson.REST_READ)
        logging.debug(js)
        self.assertEqual({
            'message_builder': {'blob_status': {
                'blob1': {'length': 5, 'content_length': 5, 'finalized': True}}}
        }, js)


    # router -> rest receiver (serialized + message builder) no reuse,
    # after initial request from router, receiver sends 'skeleton'
    # spec with just body and message_builder uris
    def test_body_json_receive_parsed_initial(self):
        js = {
            'blob_status': {'uri': '/transactions/123/body'},
            'message_builder': {
                'uri': '/transactions/123/message_builder'}}

        body = body_from_json(js, WhichJson.REST_READ)
        logging.debug(body)
        self.assertIsInstance(body, MessageBuilderSpec)
        self.assertEqual('/transactions/123/message_builder', body.uri)
        self.assertIsInstance(body.body_blob, BlobSpec)
        self.assertEqual('/transactions/123/body',
                         body.body_blob.reuse_uri.parsed_uri)


    # response from /tx/123/message_builder upload with blob uris
    def test_body_json_receive_parsed_blob_status(self):
        # RestEndpoint is deeply involved in what's sent to the receiver

        # receiver response
        js = {
            'blob_status': {'uri': '/transactions/123/body'},
            'message_builder': {
                'blob_status': {
                    'blob1': {'uri': '/transactions/123/blob/blob1'}
                }
            }
        }
        body = body_from_json(js, WhichJson.REST_READ)
        logging.debug(body)
        self.assertIsInstance(body, MessageBuilderSpec)
        self.assertIsInstance(body.body_blob, BlobSpec)
        self.assertEqual('/transactions/123/body',
                         body.body_blob.reuse_uri.parsed_uri)
        self.assertEqual({'blob1'}, body.blobs.keys())
        self.assertIsInstance(body.blobs['blob1'], BlobSpec)
        self.assertEqual('/transactions/123/blob/blob1',
                         body.blobs['blob1'].reuse_uri.parsed_uri)

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
        '%(message)s')

    unittest.main()
