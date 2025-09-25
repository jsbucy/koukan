# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import unittest
import logging
import time
from functools import partial
import copy

from koukan.executor import Executor
from koukan.version_cache import IdVersionMap

class FakeCursor:
    tx = None
    def __init__(self, version, final_attempt_reason = None):
        self.version = version
        self.final_attempt_reason = final_attempt_reason
    def copy_from(self, rhs):
        self.version = rhs.version
        self.final_attempt_reason = rhs.final_attempt_reason

class VersionCacheTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
            '%(message)s')
        self.executor = Executor(inflight_limit=10, watchdog_timeout=10)

    def tearDown(self):
        self.executor.shutdown(10)

    async def test_smoke(self):
        version_cache = IdVersionMap(ttl=1)
        id_version = version_cache.insert_or_update(
            db_id=2, rest_id="rest_id", version=2, cursor=FakeCursor(2),
            leased=True)
        tx_out = FakeCursor(1)
        self.assertTrue(id_version.wait(0, 0, tx_out))
        self.assertEqual(2, tx_out.version)
        self.assertEqual((True, False), await id_version.wait_async(
            version=0, timeout=0))

        def update(new_version):
            time.sleep(1)
            id_version.update(new_version)
        self.executor.submit(partial(update, 3))
        self.assertEqual((False, False), await id_version.wait_async(
            version=2, timeout=0.1))

        self.assertEqual((True, False), await id_version.wait_async(
            version=2, timeout=2))

        # no spurious wakeups
        self.executor.submit(partial(update, 3))
        self.assertEqual((False, False), await id_version.wait_async(
            version=3, timeout=2))

        self.assertEqual((False, False), id_version.wait(3, 0))
        idv = version_cache.insert_or_update(
            db_id=2, rest_id="rest_id", version=4,
            cursor=FakeCursor(4, final_attempt_reason='final'), leased=False)
        self.assertTrue(idv is id_version)

        cursor_out = FakeCursor(0)
        self.assertEqual((True, True), id_version.wait(3, 0, cursor_out))
        self.assertEqual(4, cursor_out.version)
        self.assertEqual('final', cursor_out.final_attempt_reason)

        idv = version_cache.get(db_id=2)
        self.assertTrue(idv is id_version)
        time.sleep(2)
        idv = version_cache.get(db_id=2)
        self.assertIsNone(idv)

if __name__ == '__main__':
    unittest.main()
