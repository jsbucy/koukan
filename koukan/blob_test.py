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

    def test_inline_fifo(self):
        b = InlineBlob(b'u')
        self.assertEqual(b'u', b.pread(0))
        b.append(b'v')
        self.assertEqual(b'uv', b.pread(0))
        b.trim_front(1)
        self.assertEqual(2, b.len())
        with self.assertRaises(ValueError):
            b.pread(0)
        self.assertEqual(b'v', b.pread(1))
        b.append_data(2, b'w')
        self.assertEqual(b'vw', b.pread(1))
        b.trim_front(3)
        self.assertEqual(3, b.len())

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

    def test_append_blob(self):
        src = InlineBlob(b'abcdef')
        dest = InlineBlob(b'xyz')
        self.assertEqual(3, dest.append_blob(src, 2, 3, chunk_size=2))
        self.assertEqual(b'xyzcde', dest.pread(0))

if __name__ == '__main__':
    unittest.main()
