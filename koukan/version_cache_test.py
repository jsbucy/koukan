# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import unittest
import logging
import time
from functools import partial

from koukan.executor import Executor
from koukan.version_cache import IdVersionMap

class VersionCacheTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')
        self.executor = Executor(inflight_limit=10, watchdog_timeout=10)

    def tearDown(self):
        self.executor.shutdown(10)

    async def test_smoke(self):
        version_cache = IdVersionMap()
        id_version = version_cache.insert_or_update(1, "rest_id", 1)
        self.assertTrue(id_version.wait(0, 0))
        self.assertEqual((True, False), await id_version.wait_async(
            version=0, timeout=0))
        self.assertEqual((True, False), await id_version.wait_async(
            version=0, timeout=0))

        def update(new_version):
            time.sleep(1)
            id_version.update(new_version)
        self.executor.submit(partial(update, 2))
        self.assertEqual((False, False), await id_version.wait_async(
            version=1, timeout=0.1))

        self.assertEqual((True, False), await id_version.wait_async(
            version=1, timeout=2))

        # no spurious wakeups
        self.executor.submit(partial(update, 2))
        self.assertEqual((False, False), await id_version.wait_async(
            version=2, timeout=2))

        self.assertEqual((False, False), id_version.wait(2, 0))
        id_version = version_cache.insert_or_update(1, "rest_id", 3)
        self.assertEqual((True, False), id_version.wait(2, 0))

if __name__ == '__main__':
    unittest.main()
