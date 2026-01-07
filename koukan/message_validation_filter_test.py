# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import unittest
import logging
from datetime import datetime, timezone

from koukan.blob import InlineBlob
from koukan.filter import (
    TransactionMetadata )
from koukan.filter_chain import FilterResult

from koukan.message_validation_filter import (
    MessageValidationFilter,
    MessageValidationFilterResult )

Status = MessageValidationFilterResult.Status

class ReceivedHeaderFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
            '%(message)s')

    def test_vec(self):
        inputs : List[Tuple[str, Status, int]] = [
            ('testdata/trivial.msg', Status.HIGH, 1),
            ('testdata/multipart.msg', Status.HIGH, 0),
            ('testdata/bad_date.msg', Status.BASIC, 0),
            ('testdata/bad_headers.msg', Status.NONE, 0),
            ('testdata/bad_multipart.msg', Status.MEDIUM, 0),
            ('testdata/missing_headers.msg', Status.BASIC, 1),
        ]

        try:
            for filename,status,count in inputs:
                with open(filename, 'rb') as f:
                    b = f.read()
                delta = TransactionMetadata(body = InlineBlob(b, last=True))
                filter = MessageValidationFilter()
                filter.wire_downstream(TransactionMetadata())

                filter.on_update(delta)
                self.assertIsNotNone(
                    out := filter.downstream_tx.filter_output.get(filter.fullname(), None))
                self.assertEqual(status, out.status)
                self.assertEqual(count, out.received_header_count)
        except:
            logging.debug(filename)
            raise

    def test_max_headers(self):
         with open('testdata/multipart.msg', 'rb') as f:
             b = f.read()
         delta = TransactionMetadata(body = InlineBlob(b, last=True))

         filter = MessageValidationFilter(max_header_bytes=20)
         filter.wire_downstream(TransactionMetadata())
         filter.on_update(delta)
         self.assertIsNotNone(
             out := filter.downstream_tx.filter_output.get(filter.fullname(), None))
         self.assertEqual(Status.NONE, out.status)
         self.assertEqual(
             'grossly excessive/malformed headers',
             out.check_validity(Status.BASIC))


         filter = MessageValidationFilter(max_header_bytes=200)
         filter.wire_downstream(TransactionMetadata())
         filter.on_update(delta)
         self.assertIsNotNone(
             out := filter.downstream_tx.filter_output.get(filter.fullname(), None))
         self.assertIsNone(out.check_validity(Status.HIGH))

    def test_max_nesting(self):
         with open('testdata/multipart.msg', 'rb') as f:
             b = f.read()
         delta = TransactionMetadata(body = InlineBlob(b, last=True))

         filter = MessageValidationFilter(max_mime_tree_depth=1)
         filter.wire_downstream(TransactionMetadata())
         filter.on_update(delta)
         self.assertIsNotNone(
             out := filter.downstream_tx.filter_output.get(filter.fullname(), None))
         self.assertEqual('mime tree', out.check_validity(Status.HIGH))

         filter = MessageValidationFilter(max_mime_tree_depth=5)
         filter.wire_downstream(TransactionMetadata())
         filter.on_update(delta)
         self.assertIsNotNone(
             out := filter.downstream_tx.filter_output.get(filter.fullname(), None))
         self.assertIsNone(out.check_validity(Status.HIGH))


if __name__ == '__main__':
    #unittest.util._MAX_LENGTH = 1024
    unittest.main()
