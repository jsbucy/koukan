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

    def test_basic(self):
        upstream = FakeSyncFilter()
        dkim_endpoint = DkimEndpoint('example.com', 'selector123',
                                     self.privkey, upstream)

        tx = TransactionMetadata(
            remote_host=HostPort('example.com', port=25000),
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob@domain')])
        tx.body_blob = InlineBlob(
            b'From: <alice>\r\n'
            b'To: <bob>\r\n'
            b'\r\n'
            b'hello\r\n')

        def exp(tx, delta):
            logging.debug(delta.body_blob.read(0))
            self.assertTrue(delta.body_blob.read(0).startswith(
                b'DKIM-Signature:'))

            upstream_delta = TransactionMetadata(
                mail_response=Response(201),
                rcpt_response=[Response(202)],
                data_response=Response(203))
            tx.merge_from(upstream_delta)
            return upstream_delta
        upstream.add_expectation(exp)

        upstream_delta = dkim_endpoint.on_update(tx, tx.copy())
        self.assertEqual(upstream_delta.mail_response.code, 201)
        self.assertEqual([r.code for r in upstream_delta.rcpt_response], [202])
        self.assertEqual(upstream_delta.data_response.code, 203)


    def test_bad(self):
        upstream = FakeSyncFilter()
        dkim_endpoint = DkimEndpoint('example.com', 'selector123',
                                     self.privkey, upstream)

        tx = TransactionMetadata(
            remote_host=HostPort('example.com', port=25000),
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob@domain')])
        tx.body_blob = InlineBlob(
            b'definitely not valid rfc822\r\n')
        upstream_delta = dkim_endpoint.on_update(tx, tx.copy())
        self.assertEqual(tx.data_response.code, 500)
        self.assertEqual(upstream_delta.data_response.code, 500)

        def exp(tx, delta):
            self.assertTrue(tx.cancelled)
            return TransactionMetadata()
        upstream.add_expectation(exp)
        cancel = TransactionMetadata(cancelled=True)
        tx.merge_from(cancel)
        upstream_delta = dkim_endpoint.on_update(tx, cancel)

if __name__ == '__main__':
    unittest.main()
