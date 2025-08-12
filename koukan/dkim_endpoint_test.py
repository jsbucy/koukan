# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import unittest
import logging
from datetime import datetime, timezone
import tempfile

from dkim import dknewkey

from koukan.blob import InlineBlob
from koukan.dkim_endpoint import DkimEndpoint
from koukan.filter import HostPort, Mailbox, TransactionMetadata
from koukan.response import Response

class DkimEndpointTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

        self.tempdir = tempfile.TemporaryDirectory()
        dir = self.tempdir.name
        self.privkey = dir + '/privkey'
        self.pubkey = dir + '/pubkey'
        dknewkey.GenRSAKeys(self.privkey)
        dknewkey.ExtractRSADnsPublicKey(self.privkey, self.pubkey)

    def tearDown(self):
        self.tempdir.cleanup()

    def test_smoke(self):
        dkim_endpoint = DkimEndpoint('example.com', 'selector123', self.privkey)
        dkim_endpoint.wire_downstream(TransactionMetadata())
        dkim_endpoint.wire_upstream(TransactionMetadata())

        delta = TransactionMetadata(
            remote_host=HostPort('example.com', port=25000),
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob@domain')],
            body = InlineBlob(
                b'From: <alice>\r\n'
                b'To: <bob>\r\n'
                b'\r\n'
                b'hello\r\n',
                last=False))

        def upstream():
            tx = dkim_endpoint.upstream
            upstream_delta = TransactionMetadata()
            if tx.mail_from and not tx.mail_response:
                upstream_delta.mail_response=Response(201)
            if tx.rcpt_to and not tx.rcpt_response:
                upstream_delta.rcpt_response=[Response(202)]
            tx.merge_from(upstream_delta)
            return upstream_delta

        dkim_endpoint.downstream.merge_from(delta)
        dkim_endpoint.on_update(delta)
        tx = dkim_endpoint.downstream
        tx.body.append(b'world!\r\n', last=True)
        filter_result = dkim_endpoint.on_update(
            TransactionMetadata(body=tx.body))
        upstream_body = dkim_endpoint.upstream.body
        self.assertTrue(upstream_body.finalized())
        logging.debug(upstream_body.pread(0))
        self.assertTrue(upstream_body.pread(0).startswith(
            b'DKIM-Signature:'))
        self.assertIsNone(filter_result.downstream_delta)

    def test_bad(self):
        dkim_endpoint = DkimEndpoint('example.com', 'selector123',
                                     self.privkey)
        tx = TransactionMetadata()
        dkim_endpoint.wire_downstream(tx)
        dkim_endpoint.wire_upstream(TransactionMetadata())
        delta = TransactionMetadata(
            body = InlineBlob(b'definitely not valid rfc822\r\n', last=True))
        tx.merge_from(delta)
        filter_result = dkim_endpoint.on_update(delta)
        self.assertIsNone(dkim_endpoint.upstream.body)
        self.assertEqual(500, filter_result.downstream_delta.data_response.code)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')
    unittest.main()
