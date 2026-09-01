import logging
import unittest
from koukan.storage import Storage
from koukan.storage_test import StorageTestBase

import koukan.sqlite_test_utils as sqlite_test_utils

class StorageTestSqlite(StorageTestBase):
    def setUp(self):
        super().setUp()
        self.dir, self.db_url = sqlite_test_utils.create_temp_sqlite_for_test()
        self.s = self._connect()

    def tearDown(self):
        self.dir.cleanup()

    def _connect(self, session_uri='http://storage-test'):
        return Storage.connect(self.db_url, session_uri=session_uri)

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
        '%(message)s')

    unittest.main()
