import unittest
import logging
from datetime import datetime, timezone
import tempfile

from dkim import dknewkey

from blob import InlineBlob
from dkim_endpoint import DkimEndpoint
from dest_domain_policy import DestDomainPolicy
from filter import HostPort, Mailbox, TransactionMetadata
from fake_endpoints import SyncEndpoint
from response import Response

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
        next = SyncEndpoint()
        policy = DestDomainPolicy()
        dkim_endpoint = DkimEndpoint(b'example.com', b'selector123',
                                     self.privkey, next)

        next.set_mail_response(Response(201))
        next.add_rcpt_response(Response(202))
        next.add_data_response(Response(203))
        tx = TransactionMetadata(
            remote_host=HostPort('example.com', port=25000),
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob@domain')])
        tx.body_blob = InlineBlob(
            b'From: <alice>\r\n'
            b'To: <bob>\r\n'
            b'\r\n'
            b'hello\r\n')

        dkim_endpoint.on_update(tx)
        self.assertEqual(tx.mail_response.code, 201)
        self.assertEqual([r.code for r in tx.rcpt_response], [202])
        self.assertEqual(tx.data_response.code, 203)
        signed_msg = next.body_blob.read(0)
        self.assertTrue(signed_msg.startswith(b'DKIM-Signature:'))

if __name__ == '__main__':
    unittest.main()
