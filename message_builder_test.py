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
        file = TemporaryFile('w+b')
        builder.build(file)
        file.seek(0)
        logging.debug(file.read())

    def test_get_blobs(self):
        json = { 'text_body': [ { 'content_uri': '/blob/xyz' } ] }
        reuse = MessageBuilder.get_blobs(
            json, lambda x: x.removeprefix('/blob/'))
        self.assertEqual(reuse, ['xyz'])
        self.assertEqual(json, { 'text_body': [ { 'blob_rest_id': 'xyz' } ] } )

if __name__ == '__main__':
    unittest.main()
