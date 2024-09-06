import unittest
import logging
import json
from tempfile import TemporaryFile


from message_builder import MessageBuilder

class MessageBuilderTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s [%(thread)d] %(message)s')


    def test_basic(self):
        with open('message_builder.json', 'r') as f:
            js = json.loads(f.read())
        builder = MessageBuilder(js, blob_factory=None)
        with TemporaryFile('w+b') as out:
            #with open('/tmp/out', 'w+b') as out:
            builder.build(out)
            out.seek(0)
            msg = out.read()

            # this is just a spot-check that we emitted all of the
            # fields in the request, not a detailed parse/validation of
            # email.message output
            self.assertIn(b'from: alice a <alice@example.com>', msg)
            self.assertIn(b'to: bob@example.com', msg)
            self.assertIn(b'subject: hello', msg)
            self.assertIn(b'date: Wed, 06 Mar 2024 10:42:31 -0800', msg)
            self.assertIn(b'message-id: <abc@xyz>', msg)
            self.assertIn(b'in-reply-to: <abd@xyz>', msg)
            self.assertIn(b'references: <abd@xyz> <abe@xyz>', msg)

            self.assertIn(b'Content-Type: multipart/mixed', msg)
            self.assertIn(b'Content-Type: multipart/alternative', msg)
            self.assertIn(b'Content-Type: multipart/related', msg)
            self.assertIn(b'Content-Type: text/html', msg)
            self.assertIn(b'Content-Type: text/plain', msg)
            self.assertIn(b'Content-Type: image/png', msg)
            self.assertIn(b'content-disposition: inline\r\ncontent-id: xyz123',
                          msg)
            self.assertIn(b'content-disposition: attachment; '
                          b'filename="hipstercat.png"', msg)

            logging.debug(msg)

            headers = builder.build_headers_for_notification()
            logging.debug(headers)
            self.assertIn(b'from: alice a <alice@example.com>', headers)
            self.assertIn(b'to: bob@example.com', headers)
            self.assertIn(b'subject: hello', headers)
            self.assertIn(b'date: Wed, 06 Mar 2024 10:42:31 -0800', headers)
            self.assertIn(b'message-id: <abc@xyz>', headers)
            self.assertIn(b'in-reply-to: <abd@xyz>', headers)
            self.assertIn(b'references: <abd@xyz> <abe@xyz>', headers)


    def test_get_blobs(self):
        json = { 'text_body': [ { 'content_uri': '/blob/xyz' } ] }
        reuse = MessageBuilder.get_blobs(
            json, lambda x: x.removeprefix('/blob/'))
        self.assertEqual(reuse, ['xyz'])
        self.assertEqual(json, { 'text_body': [ { 'blob_rest_id': 'xyz' } ] } )

if __name__ == '__main__':
    unittest.main()
