import unittest
import logging
import time

from executor import Executor
from version_cache import IdVersionMap

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
        self.assertTrue(await id_version.wait_async(
            version=0, timeout=0))
        self.assertTrue(await id_version.wait_async(
            version=0, timeout=0))

        def update():
            time.sleep(1)
            id_version.update(2)
        self.executor.submit(update)
        self.assertFalse(await id_version.wait_async(
            version=1, timeout=0.1))

        self.assertTrue(await id_version.wait_async(
            version=1, timeout=2))

        self.assertFalse(id_version.wait(2, 0))
        id_version = version_cache.insert_or_update(1, "rest_id", 3)
        self.assertTrue(id_version.wait(2, 0))

if __name__ == '__main__':
    unittest.main()
