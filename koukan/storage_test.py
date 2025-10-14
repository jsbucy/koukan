# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional

from typing import List
from threading import Thread
import time
import unittest
import logging
import base64
import os
from datetime import datetime, timedelta
from functools import partial

from koukan.blob import Blob
from koukan.storage import BlobCursor, Storage, TransactionCursor
from koukan.storage_schema import BlobSpec, VersionConflictException
from koukan.response import Response
from koukan.filter import HostPort, Mailbox, TransactionMetadata
from koukan.rest_schema import BlobUri

from koukan.message_builder import MessageBuilderSpec

import koukan.postgres_test_utils as postgres_test_utils
import koukan.sqlite_test_utils as sqlite_test_utils

def setUpModule():
    postgres_test_utils.setUpModule()

def tearDownModule():
    postgres_test_utils.tearDownModule()


class StorageTestBase(unittest.TestCase):
    sqlite : bool

    def setUp(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
            '%(message)s')

    def tearDown(self):
        self.s._del_session()
        self.s.engine.dispose()

    def test_basic_lifecycle(self):
        downstream = self.s.get_transaction_cursor()
        downstream.create(
            'tx_rest_id',
            TransactionMetadata(
                remote_host=HostPort('remote_host', 2525), host='host'),
            create_leased=True)

        downstream.write_envelope(TransactionMetadata(
            mail_from=Mailbox('alice')))

        upstream = downstream
        del downstream

        buggy = self.s.get_transaction_cursor()
        buggy.load(rest_id='tx_rest_id')
        with self.assertRaises(Exception):
            buggy.start_attempt()

        self.assertTrue(upstream.start_attempt())
        upstream.write_envelope(TransactionMetadata(
            mail_response=Response(450)))

        downstream = self.s.get_transaction_cursor()
        self.assertIsNotNone(downstream.load(rest_id='tx_rest_id'))
        downstream.write_envelope(TransactionMetadata(
            rcpt_to=[Mailbox('bob')]))
        self.assertEqual(downstream.tx.remote_host.host, 'remote_host')
        self.assertEqual(downstream.tx.host, 'host')
        self.assertEqual(downstream.tx.mail_from.mailbox, 'alice')
        self.assertEqual(downstream.tx.mail_response.code, 450)
        self.assertEqual(downstream.tx.rcpt_to[0].mailbox, 'bob')

        with self.s.begin_transaction() as db_tx:
            self.assertTrue(downstream.check_input_done(db_tx))

        downstream.write_envelope(
            TransactionMetadata(body=BlobSpec(create_tx_body=True)))
        blob_writer = downstream.get_blob_for_append(
            BlobUri(tx_id='tx_rest_id', tx_body=True))
        self.assertIsNone(blob_writer.rest_id())
        self.assertIsInstance(hash(blob_writer), int)

        with self.s.begin_transaction() as db_tx:
            self.assertFalse(downstream.check_input_done(db_tx))
        logging.debug(blob_writer.blob_uri())
        blob_writer.append_data(0, d=b'abc')
        self.assertFalse(blob_writer.last)

        downstream.load()
        tx_version = downstream.version
        blob_writer.append_data(3, d=b'xyz', content_length=9)
        self.assertFalse(blob_writer.last)

        # blob write should not ping tx version (blob_tx_refresh_interval)
        downstream.load()
        self.assertEqual(tx_version, downstream.version)

        blob_writer = downstream.get_blob_for_append(
            BlobUri(tx_id='tx_rest_id', tx_body=True))
        blob_writer.append_data(6, d=b'uvw', content_length=9)
        self.assertTrue(blob_writer.last)
        del blob_writer

        downstream.load()
        downstream.write_envelope(
            TransactionMetadata(
                retry={'max_attempts': 100}))
        logging.info('test_basic check tx input done')
        self.assertTrue(downstream.input_done)

        upstream.load()
        self.assertEqual(upstream.tx.retry['max_attempts'], 100)
        upstream.write_envelope(TransactionMetadata(
            rcpt_response=[Response(456)]))

        upstream.write_envelope(TransactionMetadata(),
                                finalize_attempt = True)

        blob_reader = upstream.tx.body
        self.assertTrue(isinstance(blob_reader, Blob))
        b = blob_reader.pread(0, 3)
        self.assertEqual(b'abc', b)
        b = blob_reader.pread(3)
        self.assertEqual(b'xyzuvw', b)
        b = blob_reader.pread(4, 3)
        self.assertEqual(b'yzu', b)

        upstream = self.s.load_one()
        self.assertFalse(upstream.no_final_notification)
        upstream.write_envelope(TransactionMetadata(), finalize_attempt=True)

        upstream = self.s.load_one()
        self.assertIsNotNone(upstream)
        self.assertEqual(upstream.db_id, downstream.db_id)
        self.assertIsNone(upstream.tx.mail_response)
        self.assertEqual(upstream.tx.rcpt_response, [])
        self.assertIsNone(upstream.tx.data_response)

        upstream.write_envelope(
            TransactionMetadata(),
            final_attempt_reason = 'retry max attempts',
            finalize_attempt = True)
        self.assertIsNone(self.s.load_one())

        self.assertTrue(self.s._refresh_session())
        downstream.load()
        downstream.write_envelope(
            TransactionMetadata(notification={'yes': True}))
        upstream = self.s.load_one()
        self.assertIsNotNone(upstream)
        self.assertTrue(upstream.no_final_notification)
        upstream.write_envelope(TransactionMetadata(), notification_done=True)
        self.assertIsNone(self.s.load_one())

        logging.debug(self.s.debug_dump())

    def test_mixed_notify_retry(self):
        cursor = self.s.get_transaction_cursor()
        cursor.create(
            'tx_rest_id',
            TransactionMetadata(
                remote_host=HostPort('remote_host', 2525), host='host'),
            create_leased=True)
        with self.assertRaises(AssertionError):
            cursor.write_envelope(TransactionMetadata(
                mail_from=Mailbox('alice'),
                retry=None,
                notification={'host': 'submission'}))

    def test_blob_8bitclean(self):
        cursor = self.s.get_transaction_cursor()
        cursor.create(
            'tx_rest_id',
            TransactionMetadata(body=BlobSpec(create_tx_body=True)),
            create_leased=True)
        blob_writer = cursor.get_blob_for_append(BlobUri('tx_rest_id', tx_body=True))
        # 32 random bytes, not valid utf8, etc.
        d = base64.b64decode('LNGxKynVCXMDfb6HD4PMryGN7/wb8WoAz1YcDgRBLdc=')
        self.assertEqual(d[23], 0)  # contains null octets
        with self.assertRaises(UnicodeDecodeError):
            s = d.decode('utf-8')
        blob_writer.append_data(0, d, len(d)*2)
        blob_writer.append_data(len(d), d, len(d)*2)

        tx_reader = self.s.get_transaction_cursor()
        tx_reader.load(rest_id='tx_rest_id')
        self.assertEqual(tx_reader.tx.body.pread(0), d+d)

    def test_body_reuse(self):
        tx_writer = self.s.get_transaction_cursor()
        tx_writer.create('tx_rest_id', TransactionMetadata(
            remote_host=HostPort('remote_host', 2525), host='host'))
        tx_writer.write_envelope(TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')]))
        tx_writer.write_envelope(
            TransactionMetadata(body=BlobSpec(create_tx_body=True)))
        blob_writer = tx_writer.get_blob_for_append(
            BlobUri(tx_id='tx_rest_id', tx_body=True))
        # incomplete blob
        d = b'hello, world!'
        blob_writer.append_data(0, d[0:7], None)

        # write a tx attempting to reuse a non-existent blob rest id,
        # this should fail
        tx_writer2 = self.s.get_transaction_cursor()
        tx = TransactionMetadata(
            remote_host=HostPort('remote_host', 2525), host='host',
            mail_from=Mailbox('alice'), rcpt_to=[Mailbox('bob')])

        # reusing body of non-existent tx should fail
        with self.assertRaises(ValueError):
            tx_writer2.create('tx_rest_id2', TransactionMetadata(
                body=BlobSpec(
                    reuse_uri=BlobUri(tx_id='nonexistent', tx_body=True))))

        # non-finalized blobs cannot be reused
        with self.assertRaises(ValueError):
            tx.body = BlobSpec(
                reuse_uri=BlobUri(tx_id='tx_rest_id', tx_body=True))
            tx_writer2 = self.s.get_transaction_cursor()
            tx_writer2.create('tx_rest_id2', tx)

        # reusing tx body as message builder blob should fail
        with self.assertRaises(ValueError):
            tx_writer2.create('tx_rest_id2', TransactionMetadata(
                body=MessageBuilderSpec(
                    {"text_body": []},
                    {'blob_rest_id': BlobSpec(reuse_uri=BlobUri(tx_id='tx_rest_id', tx_body=True))})))


        # shouldn't have been ref'd into tx2
        tx_writer2.load()
        self.assertIsNone(tx_writer2.tx.body)

        blob_writer.append_data(7, d[7:], len(d))

        logging.debug('test_body_reuse reuse success')

        tx_writer.load()
        tx_writer = self.s.get_transaction_cursor()
        tx.body = BlobSpec(reuse_uri=BlobUri(tx_id='tx_rest_id', tx_body=True))
        tx_writer.create('tx_rest_id2', tx)
        self.assertTrue(tx_writer.input_done)

        tx_writer.load()
        self.assertTrue(isinstance(tx_writer.tx.body, Blob))
        self.assertTrue(tx_writer.tx.body.finalized())
        self.assertEqual(tx_writer.tx.body.content_length(), len(d))


    def test_blob_reuse(self):
        tx_writer = self.s.get_transaction_cursor()
        tx_writer.create('tx_rest_id1', TransactionMetadata(
            remote_host=HostPort('remote_host', 2525), host='host'))
        tx_writer.write_envelope(
            TransactionMetadata(
                mail_from=Mailbox('alice'),
                rcpt_to=[Mailbox('bob')],
                body = MessageBuilderSpec(
                    {"text_body": []},
                    blobs = {
                        'blob_rest_id1': BlobSpec(create_id='blob_rest_id1'),
                        'blob_rest_id2': BlobSpec(create_id='blob_rest_id2'),
                        'blob_rest_id3': BlobSpec(create_id='blob_rest_id3')})))
        self.assertEqual(3, len(tx_writer.blobs))
        contents = {}
        for i, blob in enumerate(tx_writer.blobs):
            self.assertEqual('blob_rest_id%d' % (i + 1), blob.rest_id())
            b = b'hello, world %d!' % i
            contents[blob.rest_id()] = b
            blob.append_data(0, b, last=True)

        tx_writer2 = self.s.get_transaction_cursor()
        tx_writer2.create('tx_rest_id2', TransactionMetadata(
            remote_host=HostPort('remote_host', 2525), host='host'))
        tx_writer2.write_envelope(
            TransactionMetadata(
                mail_from=Mailbox('alice'),
                rcpt_to=[Mailbox('bob')],
                body = MessageBuilderSpec(
                    {"text_body": []},
                    blobs={
                        'blob_rest_id1': BlobSpec(reuse_uri=tx_writer.blobs[0].blob_uri()),
                        'blob_rest_id2': BlobSpec(reuse_uri=tx_writer.blobs[1].blob_uri()),
                        'blob_rest_id3': BlobSpec(reuse_uri=tx_writer.blobs[2].blob_uri()),
                        'blob_rest_id4': BlobSpec(create_id='blob_rest_id4')})))
        blob4 = BlobUri(tx_id='tx_rest_id2', blob='blob_rest_id4')
        blob_writer4 = tx_writer2.get_blob_for_append(blob4)
        b4 = b'another blob'
        blob_writer4.append_data(0, b4, len(b4))
        tx_writer2.load()

        # verify blobs were ref'd into tx_rest_id2
        self.assertEqual(4, len(tx_writer2.tx.body.blobs))
        exp_content = contents
        exp_content['blob_rest_id4'] = b4
        self.assertEqual(exp_content.keys(), tx_writer2.tx.body.blobs.keys())
        for blob_id,blob in tx_writer2.tx.body.blobs.items():
            self.assertEqual(exp_content[blob_id], blob.pread(0))


    def test_message_builder_no_blob(self):
        tx_writer = self.s.get_transaction_cursor()
        tx = TransactionMetadata(
            remote_host=HostPort('remote_host', 2525),
            host='host')
        tx.body = MessageBuilderSpec({ "headers": [ ["subject", "hello"] ] })
        tx_writer.create('tx_rest_id', tx)
        self.assertTrue(tx_writer.input_done)

        # add test
        # create tx w/message_builder that creates a blob
        # verify !input_done
        # write blob to completion
        # verify input_done

    def test_sessions(self):
        stale_session = self.s.session_id
        self.s = self._connect()

        self.assertEqual(0, self.s._gc_session(timedelta(seconds=1)))
        for i in range(0,4):
            logging.debug('%d', i)
            time.sleep(0.5)
            self.s._refresh_session()

        self.assertEqual(1, self.s._gc_session(timedelta(seconds=1)))
        self.assertFalse(self.s.testonly_get_session(stale_session)['live'])
        self.assertTrue(self.s.testonly_get_session(self.s.session_id)['live'])

    def test_recovery(self):
        old_session = self._connect()
        old_tx = old_session.get_transaction_cursor()
        old_tx.create('tx_rest_id', TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')]))
        old_tx.write_envelope(
            TransactionMetadata(body=BlobSpec(create_tx_body=True)))
        blob_writer = old_tx.get_blob_for_append(
            BlobUri(tx_id='tx_rest_id', tx_body=True))
        b = b'hello, world!'
        blob_writer.append_data(0, b, len(b))
        # don't cleanup the session
        old_session.session_id = None
        try:
            del old_session
        except:
            pass

        self.assertEqual(0, self.s.recover(session_ttl=timedelta(hours=1)))
        time.sleep(2)
        self.s._refresh_session()
        self.assertEqual(1, self.s.recover(session_ttl=timedelta(seconds=1)))

        reader = self.s.load_one()
        self.assertIsNotNone(reader)
        self.assertEqual(reader.db_id, old_tx.db_id)
        self.assertEqual(reader.tx.mail_from.mailbox, 'alice')

    def test_non_durable(self):
        writer = self.s.get_transaction_cursor()
        writer.create('xyz', TransactionMetadata(
            local_host=HostPort('local_host', 25),
            remote_host=HostPort('remote_host', 2525),
            host='host',
            retry={}))
        writer.write_envelope(TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')]))

        reader = self.s.load_one()
        self.assertIsNotNone(reader)

        reader.write_envelope(
            TransactionMetadata(),
            final_attempt_reason = 'retry policy max attempts',
            finalize_attempt = True)

        reader = self.s.get_transaction_cursor()
        self.assertTrue(reader.load(writer.db_id))

        reader = self.s.load_one()
        self.assertIsNone(reader)

    def test_gc(self):
        tx_writer = self.s.get_transaction_cursor()
        tx_writer.create(
            'xyz',
            TransactionMetadata(
                host='host',
                local_host=HostPort('local_host', 25),
                remote_host=HostPort('remote_host', 2525),
                retry={}))
        tx_writer.write_envelope(
            TransactionMetadata(body=BlobSpec(create_tx_body=True)))
        blob_writer = tx_writer.get_blob_for_append(
            BlobUri(tx_id='xyz', tx_body=True))
        d = b'hello, world!'
        blob_writer.append_data(0, d, len(d))

        tx_writer.load()
        tx_writer.write_envelope(TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')],))

        tx_reader = self.s.load_one()
        blob_reader = BlobCursor(self.s)
        self.assertEqual(tx_reader.db_id, tx_writer.db_id)

        # not expired, leased
        count = self.s.gc(ttl=timedelta(seconds=10))
        self.assertEqual(count, (0, 0))
        self.assertTrue(tx_reader.load())
        self.assertTrue(isinstance(tx_reader.tx.body, Blob))
        self.assertEqual(d, tx_reader.tx.body.pread(0))
        self.assertEqual(tx_reader.tx.body.content_length(), len(d))

        time.sleep(2)

        # expired, leased
        self.assertEqual((0, 0), self.s.gc(ttl=timedelta(seconds=1)))
        self.assertTrue(tx_reader.load())
        self.assertTrue(isinstance(tx_reader.tx.body, Blob))
        self.assertEqual(d, tx_reader.tx.body.pread(0))
        self.assertEqual(tx_reader.tx.body.content_length(), len(d))

        tx_reader.write_envelope(TransactionMetadata(),
                                 final_attempt_reason = 'upstream success',
                                 finalize_attempt = True)

        time.sleep(2)

        # expired, unleased
        self.assertEqual((1,1), self.s.gc(ttl=timedelta(seconds=1)))

        self.assertIsNone(tx_reader.load())

    def test_waiting_slowpath(self):
        writer = self.s.get_transaction_cursor()
        writer.create('xyz', TransactionMetadata(
            local_host=HostPort('local_host', 25),
            remote_host=HostPort('remote_host', 2525),
            host='host'))
        reader = self.s.get_transaction_cursor()
        self.assertIsNotNone(reader.load(writer.db_id))
        self.assertIsNone(reader.tx.mail_from)
        self.assertFalse(bool(reader.tx.rcpt_to))
        self.assertEqual((False, False), reader.wait(1))

        writer.write_envelope(TransactionMetadata(
            mail_from=Mailbox('alice'),
            rcpt_to=[Mailbox('bob')]))

        self.assertTrue(reader.wait(0))
        reader.load()
        self.assertEqual(reader.tx.mail_from.mailbox, 'alice')
        self.assertEqual(reader.tx.rcpt_to[0].mailbox, 'bob')

    def wait_for(self, reader, rv):
        logging.info('test wait')
        rv[0] = reader.wait(5)

    def test_waiting_inflight(self):
        tx_cursor = self.s.get_transaction_cursor()
        tx_cursor.create('xyz', TransactionMetadata(
            host='outbound',
            retry={}))

        # needs to be inflight to wait
        reader = self.s.load_one()
        self.assertIsNotNone(reader)
        self.assertEqual(reader.db_id, tx_cursor.db_id)
        self.assertIsNone(reader.tx.mail_from)
        self.assertFalse(bool(reader.tx.rcpt_to))

        rv = [None]
        t = Thread(target = partial(self.wait_for, reader, rv))
        t.start()
        time.sleep(0.1)
        logging.info('test append')

        tx_cursor.load()
        tx_cursor.write_envelope(TransactionMetadata(
            mail_from=Mailbox('alice'), rcpt_to=[Mailbox('bob')]))

        t.join()

        self.assertTrue(rv[0])
        reader.load()
        self.assertEqual(reader.tx.mail_from.mailbox, 'alice')


    @staticmethod
    def wait_blob(reader, rv):
        rv[0] = reader.wait(5)

    def start_wait(self, reader, rv):
        t = Thread(target =
                   lambda: StorageTestBase.wait_blob(reader, rv))
        t.start()
        time.sleep(0.1)
        return t

    def join(self, t):
        t.join(timeout=5)
        self.assertFalse(t.is_alive())

    def reader(self, reader : TransactionCursor, dd : List[bytes]):
        d = bytes()
        assert reader.tx is not None
        assert isinstance(reader.tx.body, Blob)
        while (((cl := reader.tx.body.content_length()) is None) or
               len(d) < cl):
            logging.info('reader %d', len(d))
            reader.load()
            ddd = reader.tx.body.pread(len(d))
            assert ddd is not None
            d += ddd
        dd[0] = d

    def test_blob_waiting_poll(self):
        cursor = self.s.get_transaction_cursor()
        cursor.create(
            'tx_rest_id',
            TransactionMetadata(body=BlobSpec(create_tx_body=True)),
            create_leased=True)
        blob_writer = cursor.get_blob_for_append(
            BlobUri('tx_rest_id', tx_body=True))

        tx_reader = self.s.get_transaction_cursor()
        tx_reader.load(rest_id='tx_rest_id')

        dd = [None]
        t = Thread(target = lambda: self.reader(tx_reader, dd), daemon=True)
        t.start()

        d = None
        with open('/dev/urandom', 'rb') as f:
            d = f.read(128)

        for i in range(0, len(d)):
            logging.info('test_blob_waiting2 %d', i)
            blob_writer.append_data(i, d[i:i+1], len(d))
            self.assertEqual(blob_writer.length, i+1)

        t.join()
        self.assertFalse(t.is_alive())
        self.assertEqual(d, dd[0])
        self.assertEqual(len(d), tx_reader.tx.body.content_length())
        self.assertEqual(len(d), tx_reader.tx.body.len())

    def disabled_test_pingpong(self):
        upstream = self.s.get_transaction_cursor()
        upstream.create(
            'tx_rest_id',
            TransactionMetadata(
                remote_host=HostPort('remote_host', 2525), host='host'),
            create_leased=True)
        self.assertIsNotNone(upstream.load(start_attempt=True))

        downstream = self.s.get_transaction_cursor()
        self.assertIsNotNone(downstream.load(rest_id='tx_rest_id'))

        def ping(cursor):
            # add rcpt
            read = write = readc = 0
            delta = TransactionMetadata(
                rcpt_to=[Mailbox('bob@example.com')])
            delta.rcpt_to_list_offset = len(cursor.tx.rcpt_to)
            start = time.monotonic()
            cursor.write_envelope(delta)
            write += time.monotonic() - start

            # read rcpt response
            while len(cursor.tx.rcpt_response) < len(cursor.tx.rcpt_to):
                cursor.wait()
                start = time.monotonic()
                cursor.load()
                read += time.monotonic() - start
                readc += 1
            return write, read, readc

        def pong(cursor):
            read = write = readc = 0
            while len(cursor.tx.rcpt_response) == len(cursor.tx.rcpt_to):
                cursor.wait()
                start = time.monotonic()
                cursor.load()
                read += time.monotonic() - start
                readc += 1
            delta = TransactionMetadata()
            delta.rcpt_response = []
            delta.rcpt_response_list_offset = len(cursor.tx.rcpt_response)
            for i in range(len(cursor.tx.rcpt_response), len(cursor.tx.rcpt_to)):
                delta.rcpt_response.append(Response())
            start = time.monotonic()
            cursor.write_envelope(delta)
            write += time.monotonic() - start

            return write, read, readc

        def runn(n, fn):
            logging.warning(fn)
            write = read = readc = 0
            for i in range(0, n):
                w,r,rc = fn()
                write += w
                read += r
                readc += rc
            logging.warning('%f %f %d', write, read, readc)

        iters=1000

        start = time.monotonic()
        t1=Thread(target=partial(runn, iters, partial(ping, downstream)))
        t1.start()
        t2=Thread(target=partial(runn, iters, partial(pong, upstream)))
        t2.start()
        t1.join()
        t2.join()
        total = time.monotonic() - start
        logging.warning('done %f %f', total, iters/total)


    def disabled_test_micro(self):
        upstream = self.s.get_transaction_cursor()
        upstream.create(
            'tx_rest_id',
            TransactionMetadata(
                remote_host=HostPort('remote_host', 2525), host='host',
                mail_from = Mailbox('alice'),
                rcpt_to = [Mailbox('bob')]),
            create_leased=True)
        self.assertIsNotNone(upstream.start_attempt())
        upstream.write_envelope(TransactionMetadata(
            mail_response=Response(201),
            rcpt_response=[Response(202)]))

        iters = 10

        start = time.monotonic()
        for i in range(0, iters):
            downstream = self.s.get_transaction_cursor()
            downstream.load(rest_id='tx_rest_id')
        logging.debug('%f', time.monotonic() - start)

        start = time.monotonic()
        for i in range(0, iters):
            downstream = self.s.get_transaction_cursor(rest_id='tx_rest_id')
            logging.debug(downstream.check())
        duration = time.monotonic() - start
        logging.debug('%f %f', duration, duration/iters)


    # verify handoff via VersionCache returns the same cursor/tx you
    # would have gotten by doing a fresh read
    # basic setup: 2 cursors representing downstream/RestHandler and
    # upstream/OutputHandler
    # one does a write
    # the other verifies it reads it back from the cache
    # then a 3rd cursor reads it from the db and verfies it got the
    # same thing as the cache
    def test_handoff(self):
        prev_reads = self.s._tx_reads
        upstream = self.s.get_transaction_cursor()
        upstream.create(
            'tx_rest_id',
            TransactionMetadata(
                remote_host=HostPort('remote_host', 2525), host='host',
                mail_from = Mailbox('alice'),
                rcpt_to = [Mailbox('bob')],
                body = BlobSpec(create_tx_body=True)),
            create_leased=True)
        self.assertIsNotNone(upstream.start_attempt())
        self.assertTrue(upstream.in_attempt)
        self.assertIsNotNone(upstream.inflight_session_id)

        downstream = self.s.get_transaction_cursor(rest_id='tx_rest_id')
        self.assertTrue(downstream.try_cache())
        self.assertEqual(0, self.s._tx_reads - prev_reads)
        self.assertFalse(downstream.in_attempt)
        self.assertEqual(upstream.inflight_session_id,
                         downstream.inflight_session_id)

        from_db = self.s.get_transaction_cursor(rest_id='tx_rest_id')
        self.assertTrue(from_db.load())
        self.assertFalse(from_db.in_attempt)
        self.assertEqual(upstream.inflight_session_id,
                         from_db.inflight_session_id)

        self.assertEqual(1, self.s._tx_reads - prev_reads)
        prev_reads = self.s._tx_reads


        upstream.write_envelope(TransactionMetadata(
            mail_response=Response(201),
            rcpt_response=[Response(202)]))
        self.assertTrue(downstream.try_cache())
        self.assertEqual(upstream.tx.rcpt_response,
                         downstream.tx.rcpt_response)
        self.assertEqual(0, self.s._tx_reads - prev_reads)

        from_db = self.s.get_transaction_cursor(rest_id='tx_rest_id')
        self.assertTrue(from_db.load())
        self.assertEqual(upstream.tx.rcpt_response,
                         from_db.tx.rcpt_response)
        self.assertEqual(1, self.s._tx_reads - prev_reads)
        prev_reads = self.s._tx_reads


        blob_writer = downstream.get_blob_for_append(
            BlobUri('tx_rest_id', tx_body=True))
        b = b'hello, world!'
        blob_writer.append_data(0, b, last=True)

        self.assertTrue(upstream.try_cache())
        self.assertEqual(len(b), upstream.tx.body.content_length())
        self.assertEqual(0, self.s._tx_reads - prev_reads)

        from_db = self.s.get_transaction_cursor(rest_id='tx_rest_id')
        self.assertTrue(from_db.load())
        self.assertEqual(len(b), from_db.tx.body.content_length())
        self.assertEqual(1, self.s._tx_reads - prev_reads)


    def test_handoff_message_builder(self):
        prev_reads = self.s._tx_reads
        upstream = self.s.get_transaction_cursor()

        message_builder = MessageBuilderSpec(
            { "headers": [ ["subject", "hello"] ] },
            blobs={
                'blob1': BlobSpec(create_id='blob1'),
                'blob2': BlobSpec(create_id='blob2')})
        upstream.create(
            'tx_rest_id',
            TransactionMetadata(
                remote_host=HostPort('remote_host', 2525), host='host',
                mail_from = Mailbox('alice'),
                rcpt_to = [Mailbox('bob')],
                body = message_builder),
            create_leased=True)
        self.assertIsNotNone(upstream.start_attempt())
        self.assertTrue(upstream.in_attempt)
        self.assertIsNotNone(upstream.inflight_session_id)

        downstream = self.s.get_transaction_cursor(rest_id='tx_rest_id')
        self.assertTrue(downstream.try_cache())
        self.assertEqual(0, self.s._tx_reads - prev_reads)
        self.assertFalse(downstream.in_attempt)
        self.assertEqual(upstream.inflight_session_id,
                         downstream.inflight_session_id)

        from_db = self.s.get_transaction_cursor(rest_id='tx_rest_id')
        self.assertTrue(from_db.load())
        self.assertFalse(from_db.in_attempt)
        self.assertEqual(upstream.inflight_session_id,
                         from_db.inflight_session_id)

        self.assertEqual(1, self.s._tx_reads - prev_reads)
        prev_reads = self.s._tx_reads

        logging.debug(downstream.blobs)
        blob1_writer = downstream.get_blob_for_append(
            BlobUri('tx_rest_id', blob='blob1'))
        b1 = b'hello blob1'
        blob1_writer.append_data(0, b1, last=True)

        self.assertTrue(upstream.try_cache())
        blob1_reader = upstream.blobs[0]
        self.assertEqual('blob1', blob1_reader.blob_uri().blob)
        self.assertEqual(len(b1), blob1_reader.content_length())
        blob2_reader = upstream.blobs[1]
        self.assertEqual('blob2', blob2_reader.blob_uri().blob)
        self.assertIsNone(blob2_reader.content_length())

        self.assertEqual(0, self.s._tx_reads - prev_reads)

        from_db = self.s.get_transaction_cursor(rest_id='tx_rest_id')
        self.assertTrue(from_db.load())
        blob1_reader = from_db.blobs[0]
        self.assertEqual('blob1', blob1_reader.blob_uri().blob)
        self.assertEqual(len(b1), blob1_reader.content_length())
        blob2_reader = from_db.blobs[1]
        self.assertEqual('blob2', blob2_reader.blob_uri().blob)
        self.assertIsNone(blob2_reader.content_length())
        self.assertEqual(1, self.s._tx_reads - prev_reads)
        prev_reads = self.s._tx_reads

        blob2_writer = downstream.get_blob_for_append(
            BlobUri('tx_rest_id', blob='blob2'))
        b2 = b'hello blob2'
        blob2_writer.append_data(0, b2, last=True)

        self.assertTrue(upstream.try_cache())
        blob1_reader = upstream.blobs[0]
        self.assertEqual('blob1', blob1_reader.blob_uri().blob)
        self.assertEqual(len(b1), blob1_reader.content_length())
        blob2_reader = upstream.blobs[1]
        self.assertEqual('blob2', blob2_reader.blob_uri().blob)
        self.assertEqual(len(b2), blob2_reader.content_length())

        self.assertEqual(0, self.s._tx_reads - prev_reads)

        from_db = self.s.get_transaction_cursor(rest_id='tx_rest_id')
        self.assertTrue(from_db.load())
        blob1_reader = from_db.blobs[0]
        self.assertEqual('blob1', blob1_reader.blob_uri().blob)
        self.assertEqual(len(b1), blob1_reader.content_length())
        blob2_reader = from_db.blobs[1]
        self.assertEqual('blob2', blob2_reader.blob_uri().blob)
        self.assertEqual(len(b2), blob2_reader.content_length())
        self.assertEqual(1, self.s._tx_reads - prev_reads)
        prev_reads = self.s._tx_reads


class StorageTestSqlite(StorageTestBase):
    def setUp(self):
        super().setUp()
        self.dir, self.db_url = sqlite_test_utils.create_temp_sqlite_for_test()
        self.s = self._connect()

    def tearDown(self):
        self.dir.cleanup()

    def _connect(self):
        return Storage.connect(self.db_url, session_uri='http://storage-test')


class StorageTestPostgres(StorageTestBase):
    pg : Optional[object] = None
    storage_yaml : Optional[dict] = None

    def setUp(self):
        super().setUp()

        self.s = self._connect()

    def _connect(self):
        if self.pg is None:
            self.storage_yaml = {}
            self.pg, self.pg_url = postgres_test_utils.setup_postgres()

        return Storage.connect(self.pg_url, session_uri='http://storage-test')


if __name__ == '__main__':
    unittest.main()
