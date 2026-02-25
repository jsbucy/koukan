# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import unittest
import logging
from datetime import datetime, timezone

from koukan.blob import InlineBlob
from koukan.filter import (
    EsmtpParam,
    Mailbox,
    TransactionMetadata )
from koukan.filter_chain import FilterResult

from koukan.message_validation_filter import (
    MessageValidationFilter,
    MessageValidationFilterOutput )

from koukan.matcher_result import MatcherResult

Status = MessageValidationFilterOutput.Status

class MessageValidationFilterTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
            '%(message)s')

    def test_vec(self):
        # filename, expected status, expected received header count,
        # SMTPUTF8 esmtp capability
        inputs : List[Tuple[str, Status, int, bool]] = [
            # problems with overall headers
            # MissingHeaderBodySeparatorDefect
            ('testdata/bad_headers.msg', Status.NONE, 0, False),
            # ascii control chars
            ('testdata/ascii_ctl_headers.msg', Status.BASIC, 1, False),
            ('testdata/ascii_ctl_headers.msg', Status.BASIC, 1, True),
            # high-bit chars that are not valid utf8
            ('testdata/8bit_headers.msg', Status.BASIC, 1, False),
            ('testdata/8bit_headers.msg', Status.BASIC, 1, True),
            # valid utf8 with/without SMTPUTF8
            ('testdata/utf8_headers.msg', Status.BASIC, 1, False),
            ('testdata/utf8_headers.msg', Status.HIGH, 1, True),
            ('testdata/utf8_local_part.msg', Status.BASIC, 1, False),
            ('testdata/utf8_local_part.msg', Status.HIGH, 1, True),

            # problems with required headers: from/date/message-id
            ('testdata/bad_from.msg', Status.BASIC, 1, False),
            ('testdata/bad_date.msg', Status.BASIC, 0, False),
            ('testdata/missing_headers.msg', Status.BASIC, 1, False),

            # problems with mime tree
            ('testdata/bad_cc.msg', Status.MEDIUM, 1, False),
            ('testdata/bad_multipart.msg', Status.MEDIUM, 0, False),
            #
            ('testdata/unknown_content_type.msg', Status.HIGH, 0, False),

            # valid
            ('testdata/trivial.msg', Status.HIGH, 1, False),
            ('testdata/multipart.msg', Status.HIGH, 0, False),
        ]

        try:
            for filename,status,count,smtputf8 in inputs:
                logging.debug(filename)
                with open(filename, 'rb') as f:
                    b = f.read()
                esmtp = [EsmtpParam('SMTPUTF8')] if smtputf8 else []
                delta = TransactionMetadata(
                    mail_from = Mailbox('alice@example.com', esmtp=esmtp),
                    body = InlineBlob(b, last=True))
                filter = MessageValidationFilter()
                filter.wire_downstream(TransactionMetadata())
                filter.downstream_tx.merge_from(delta)

                filter.on_update(delta)
                self.assertIsNotNone(
                    out := filter.downstream_tx.filter_output.get(
                        filter.fullname(), None))
                logging.debug('%s %s', out.status, out.err)
                self.assertEqual(status, out.status)
                if status > Status.NONE:
                    self.assertEqual(count, out.received_header_count)
                    self.assertEqual(MatcherResult.MATCH, out.match({
                        'max_received_headers': count-1}))
                if status < Status.HIGH:
                    threshold = Status(status+1)
                    self.assertEqual(MatcherResult.MATCH, out.match({'validity_threshold': threshold.name}))
                else:
                    self.assertEqual(MatcherResult.NO_MATCH, out.match({'validity_threshold': status.name}))


        except:
            logging.debug(filename)
            raise

    def test_max_headers(self):
         with open('testdata/multipart.msg', 'rb') as f:
             b = f.read()
         delta = TransactionMetadata(
             mail_from = Mailbox('alice@example.com'),
             body = InlineBlob(b, last=True))

         filter = MessageValidationFilter(max_header_bytes=20)
         filter.wire_downstream(TransactionMetadata())
         filter.downstream_tx.merge_from(delta)
         filter.on_update(delta)
         self.assertIsNotNone(
             out := filter.downstream_tx.filter_output.get(
                 filter.fullname(), None))
         self.assertEqual(Status.NONE, out.status)
         self.assertEqual(
             'couldn\'t find end of rfc822 headers in 20',
             out.check_validity(Status.BASIC))


         filter = MessageValidationFilter(max_header_bytes=200)
         filter.wire_downstream(TransactionMetadata(
             mail_from = Mailbox('alice@example.com')))
         filter.downstream_tx.merge_from(delta)
         filter.on_update(delta)
         self.assertIsNotNone(
             out := filter.downstream_tx.filter_output.get(
                 filter.fullname(), None))
         self.assertIsNone(out.check_validity(Status.HIGH))

    def test_max_nesting(self):
         with open('testdata/multipart.msg', 'rb') as f:
             b = f.read()
         delta = TransactionMetadata(
             mail_from = Mailbox('alice@example.com'),
             body = InlineBlob(b, last=True))

         filter = MessageValidationFilter(max_mime_tree_depth=1)
         filter.wire_downstream(TransactionMetadata())
         filter.downstream_tx.merge_from(delta)
         filter.on_update(delta)
         self.assertIsNotNone(
             out := filter.downstream_tx.filter_output.get(
                 filter.fullname(), None))
         self.assertEqual('mime tree', out.check_validity(Status.HIGH))

         filter = MessageValidationFilter(max_mime_tree_depth=5)
         filter.wire_downstream(TransactionMetadata(
             mail_from = Mailbox('alice@example.com')))
         filter.downstream_tx.merge_from(delta)
         filter.on_update(delta)
         self.assertIsNotNone(
             out := filter.downstream_tx.filter_output.get(
                 filter.fullname(), None))
         self.assertIsNone(out.check_validity(Status.HIGH))

    def test_smoke(self):
        with open('testdata/trivial.msg', 'rb') as f:
            b = f.read()
        delta = TransactionMetadata(
            mail_from = Mailbox('alice@example.com'),
            body = InlineBlob(b, last=True))
        filter = MessageValidationFilter()
        filter.wire_downstream(TransactionMetadata())
        filter.downstream_tx.merge_from(delta)

        filter.on_update(delta)
        self.assertIsNotNone(
            out := filter.downstream_tx.filter_output.get(
                filter.fullname(), None))
        self.assertEqual(Status.HIGH, out.status)
        self.assertEqual('alice@example.com', out.parsed_header_from.addr_spec)

if __name__ == '__main__':
    unittest.main()
