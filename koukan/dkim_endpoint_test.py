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
from koukan.fake_endpoints import FakeSyncFilter
from koukan.response import Response

class DkimEndpointTest(unittest.IsolatedAsyncioTestCase):
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

    async def test_smoke(self):
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

        async def upstream():
            tx = dkim_endpoint.upstream
            upstream_delta = TransactionMetadata()
            if tx.mail_from and not tx.mail_response:
                upstream_delta.mail_response=Response(201)
            if tx.rcpt_to and not tx.rcpt_response:
                upstream_delta.rcpt_response=[Response(202)]
            if tx.body is not None and tx.body.finalized():
                logging.debug(tx.body.pread(0))
                self.assertTrue(tx.body.pread(0).startswith(
                    b'DKIM-Signature:'))
                upstream_delta.data_response=Response(203)
            tx.merge_from(upstream_delta)
            return upstream_delta

        dkim_endpoint.downstream.merge_from(delta)
        await dkim_endpoint.on_update(delta, upstream)
        tx = dkim_endpoint.downstream
        tx.body.append(b'world!\r\n', last=True)
        await dkim_endpoint.on_update(
            TransactionMetadata(body=tx.body), upstream)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])
        self.assertEqual(tx.data_response.code, 203)


    async def test_bad(self):
        dkim_endpoint = DkimEndpoint('example.com', 'selector123',
                                     self.privkey)
        tx = TransactionMetadata()
        dkim_endpoint.wire_downstream(tx)
        dkim_endpoint.wire_upstream(TransactionMetadata())
        async def unexpected_upstream():
            self.fail()
        delta = TransactionMetadata(
            remote_host=HostPort('example.com', port=25000),
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob@domain')])
        delta.body = InlineBlob(
            b'definitely not valid rfc822\r\n', last=True)
        tx.merge_from(delta)
        await dkim_endpoint.on_update(delta, unexpected_upstream)
        self.assertEqual(tx.data_response.code, 500)

        return

        def exp(tx, delta):
            self.assertTrue(tx.cancelled)
            return TransactionMetadata()
        upstream.add_expectation(exp)
        cancel = TransactionMetadata(cancelled=True)
        tx.merge_from(cancel)
        upstream_delta = dkim_endpoint.on_update(tx, cancel)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')
    unittest.main()
