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
        for base, blob in [('multipart', [b'yolocat', b'yolocat2']),
                     ('multipart2', [b'yolocat', b'yolocat2']),
                     ('related', [b'yolocat'])
                     ]:
            parser = MessageParser(blob_factory=self._blob_factory,
                                   max_inline=1000)
            logging.debug('test_smoke %s', base)
            parsed = self._parse(parser, 'testdata/' + base + '.msg')

            logging.debug(json.dumps(parsed.json, indent=2))

            with open('testdata/' + base + '.json', 'r') as f:
                expected_json = json.loads(f.read())
            self.assertEqual(parsed.json, expected_json)

            self.assertEqual(len(parsed.blobs), len(blob))
            for i in range(0, len(blob)):
                self.assertEqual(blob[i], parsed.blobs[i].read(0))

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
