import unittest
import logging

from blob import InlineBlob, CompositeBlob


class BlobTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')


    def test_inline(self):
        b = InlineBlob(b'wxyz')
        self.assertEqual(b.read(1,2), b'xy')
        self.assertEqual(b.read(1,None), b'xyz')

    def test_composite(self):
        b = CompositeBlob()
        b.append(InlineBlob(b'aabcc'), 1, 3)
        b.append(InlineBlob(b'ddeff'), 1, 3)
        b.append(InlineBlob(b'gghii'), 1, 3)

        self.assertEqual(b.read(0, 3), b'abc')

        self.assertEqual(b.read(0), b'abcdefghi')
        self.assertEqual(b.read(1,2), b'bc')
        self.assertEqual(b.read(1,4), b'bcde')
        self.assertEqual(b.read(1,6), b'bcdefg')

if __name__ == '__main__':
    unittest.main()
