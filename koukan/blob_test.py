# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import unittest
import logging
import os

from koukan.blob import BlobReader, InlineBlob, CompositeBlob


class BlobTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')


    def test_inline(self):
        b = InlineBlob(b'wxyz')
        self.assertEqual(b.pread(1,2), b'xy')
        self.assertEqual(b.pread(1,None), b'xyz')

    def test_composite(self):
        b = CompositeBlob()
        self.assertEqual(b.content_length(), None)
        b.append(InlineBlob(b'aabcc'), 1, 3)
        self.assertEqual(b.content_length(), None)
        b.append(InlineBlob(b'ddeff'), 1, 3)
        self.assertEqual(b.content_length(), None)
        b.append(InlineBlob(b'gghii'), 1, 3, True)
        self.assertEqual(b.content_length(), 9)

        self.assertEqual(b.pread(0, 3), b'abc')

        self.assertEqual(b.pread(0), b'abcdefghi')
        self.assertEqual(b.pread(1,2), b'bc')
        self.assertEqual(b.pread(1,4), b'bcde')
        self.assertEqual(b.pread(1,6), b'bcdefg')

    def test_reader(self):
        b = InlineBlob(b'xyz')
        r = BlobReader(b)
        self.assertEqual(0, r.seek(0, os.SEEK_CUR))
        self.assertEqual(3, r.seek(0, os.SEEK_END))
        self.assertEqual(0, r.seek(0, os.SEEK_SET))
        self.assertEqual(b'x', r.read(1))
        self.assertEqual(b'yz', r.read())
        self.assertEqual(b'', r.read())

if __name__ == '__main__':
    unittest.main()
