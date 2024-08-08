from typing import Optional
import unittest
import logging

import json

from message_parser import MessageParser, ParsedMessage
from blob import InlineBlob

class MessageParserTest(unittest.TestCase):
    _blob_id : int = 0
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

    def _blob_factory(self):
        blob_id = self._blob_id
        self._blob_id += 1
        return InlineBlob(b'', rest_id=str(blob_id))

    def _parse(self, parser : MessageParser, filename
               ) -> Optional[ParsedMessage]:
            with open(filename, 'rb') as f:
                parsed = parser.parse(f)
                return parsed

    def test_smoke(self):
        parser = MessageParser(blob_factory=self._blob_factory,
                               max_inline=1000)

        parsed = self._parse(parser, 'testdata/multipart.msg')

        with open('testdata/multipart.json', 'r') as f:
            expected_json = json.loads(f.read())

        mixed = parsed.json['parts']
        self.assertEqual(parsed.json, expected_json)

        self.assertEqual(mixed['content_type'], 'multipart/mixed')

        related = mixed['parts'][0]
        self.assertEqual(related['content_type'], 'multipart/related')

        alternative = related['parts'][0]
        self.assertEqual(alternative['content_type'], 'multipart/alternative')

        plain = alternative['parts'][0]
        self.assertEqual(plain['content_type'], 'text/plain')
        html = alternative['parts'][1]
        self.assertEqual(html['content_type'], 'text/html')

        image = mixed['parts'][1]
        self.assertEqual(image['content_type'], 'image/png')
        self.assertEqual(image['blob_id'], '0')
        self.assertEqual(parsed.blobs[0].read(0), b'image/png')


    def test_dsn(self):

        for filename, exp_content_type in [
                ('testdata/dsn.msg','message/rfc822'),
                ('testdata/dsn-text-rfc822-headers.msg',
                 'text/rfc822-headers')]:
            parser = MessageParser(blob_factory=self._blob_factory,
                                   max_inline=0)
            parsed = self._parse(parser, filename)

            #logging.debug('%s %s', filename, json.dumps(parsed.json, indent=2))
            for k,v in parsed.json['parts']['headers']:
                if k != 'content-type':
                    continue
                self.assertEqual(v[0], 'multipart/report')
                self.assertEqual(v[1]['report-type'], 'delivery-status')
                break
            else:
                self.fail('missing multipart/report')

            delivery_status = parsed.json['parts']['parts'][1]
            self.assertEqual(delivery_status['content_type'],
                             'message/delivery-status')
            self.assertIn(['reporting-mta', 'dns; googlemail.com'],
                          delivery_status['parts'][0]['headers'])
            self.assertIn(['final-recipient', 'rfc822; xyz@sandbox.gloop.org'],
                          delivery_status['parts'][1]['headers'])

            self.assertEqual(parsed.json['parts']['parts'][2]['content_type'],
                             exp_content_type)



if __name__ == '__main__':
    unittest.main()
