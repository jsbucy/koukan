import unittest
import logging

from koukan.dsn import generate_dsn
from koukan.response import Response

class DsnTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s [%(thread)d] %(message)s')

    def test_dsn(self):
        headers=('Date: Thu, 28 Mar 2024 22:39:00 -0000\r\n'
                 'From: alice a <alice@example.com>\r\n'
                 'To: bob b <bob@example.com>\r\n'
                 'Subject: hello\r\n'
                 'message-id: <xyz@abc>\r\n'
                 '\r\n')

        dsn = generate_dsn("mailer-daemon@example.com",
                           "alice@example.com", "bob@example.com",
                           headers,
                           received_date=1711665547,
                           now=1711665550,
                           response=Response(450, 'EBUSY'))
        dsn_str = bytes(dsn).decode('utf-8')

        # like message_builder, this is not an exhaustive test (or
        # worse: change-detector test) of the underlying mime library,
        # just a spot check that all of the expected fields made it
        # into the output

        self.assertIn('from: Mail Delivery Subsystem '
                      '<mailer-daemon@example.com>', dsn_str)
        self.assertIn('to: alice@example.com', dsn_str)
        self.assertIn('date: Thu, 28 Mar 2024 22:39:10 -0000', dsn_str)
        self.assertIn('subject: Delivery Status Notification (failure)', dsn_str)

        self.assertIn('in-reply-to: <xyz@abc>', dsn_str)
        self.assertIn('references: <xyz@abc>', dsn_str)


        self.assertIn('Content-Type: multipart/report; '
                      'report_type="delivery-status"', dsn_str)
        self.assertIn('Content-Type: text/plain', dsn_str)

        self.assertIn('Content-Type: message/delivery-status', dsn_str)
        self.assertIn('Reporting-MTA: dns;example.com', dsn_str)
        self.assertIn('Arrival-Date: Thu, 28 Mar 2024 22:39:07 -0000', dsn_str)


        self.assertIn('Last-Attempt-Date: Thu, 28 Mar 2024 22:39:10 -0000',
                      dsn_str)
        self.assertIn('Final-Recipient: rfc822;bob@example.com', dsn_str)
        self.assertIn('Status: 4.0.0', dsn_str)
        self.assertIn('Diagnostic-Code: smtp;450 EBUSY', dsn_str)

        self.assertIn('Content-Type: text/rfc822-headers', dsn_str)

        self.assertIn(headers, dsn_str)

        print(dsn_str)


if __name__ == '__main__':
    unittest.main()
