from typing import Optional
import unittest
import logging
import socketserver

from koukan.fake_smtpd import FakeSmtpd

import koukan_cpython_smtplib.smtplib as smtplib
from koukan.smtp_endpoint import SmtpEndpoint
from koukan.filter import HostPort, Mailbox, TransactionMetadata
from koukan.blob import InlineBlob
from koukan.response import Response

class SmtpEndpointTest(unittest.TestCase):
    endpoint : Optional[SmtpEndpoint] = None

    def setUp(self) -> None:
        self.fake_smtpd_port = self.find_unused_port()

        self.fake_smtpd = FakeSmtpd(
            "::1", self.fake_smtpd_port, 'smtp')
        self.fake_smtpd.start()

    def tearDown(self) -> None:
        if self.endpoint:
            self.endpoint._shutdown()

    def find_unused_port(self) -> int:
        with socketserver.TCPServer(
                ("localhost", 0),
                lambda x,y,z: socketserver.BaseRequestHandler(x,y,z)) as s:
            return s.server_address[1]

    def test_smoke(self) -> None:
        self.endpoint = SmtpEndpoint(smtplib, 'localhost', protocol='smtp')
        tx = TransactionMetadata()
        self.endpoint.wire_downstream(tx)
        prev = tx.copy()
        tx.mail_from = Mailbox('alice@example.com')
        tx.rcpt_to = [Mailbox('bob@example.com')]
        tx.body = InlineBlob(b'hello, world!', last=True)
        tx.remote_host = HostPort('::1', self.fake_smtpd_port)
        self.endpoint.on_update(prev.delta(tx))
        assert tx.mail_response is not None
        self.assertEqual(250, tx.mail_response.code)
        def _code(r : Optional[Response]) -> int:
            assert r is not None
            return r.code
        self.assertEqual([250], [_code(r) for r in tx.rcpt_response])
        assert tx.data_response is not None
        self.assertEqual(250, tx.data_response.code)

    def test_connect_fail(self) -> None:
        dead_port = self.find_unused_port()
        self.endpoint = SmtpEndpoint(smtplib, 'localhost', protocol='smtp')
        tx = TransactionMetadata()
        self.endpoint.wire_downstream(tx)
        prev = tx.copy()
        tx.mail_from = Mailbox('alice@example.com')
        tx.remote_host = HostPort('::1', dead_port)
        self.endpoint.on_update(prev.delta(tx))
        assert tx.mail_response is not None
        self.assertEqual(400, tx.mail_response.code)

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d  %(message)s')

    unittest.main()
