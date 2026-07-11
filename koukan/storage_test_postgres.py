from typing import Optional
import logging
import unittest
from koukan.storage import Storage
from koukan.storage_test import StorageTestBase

import koukan.postgres_test_utils as postgres_test_utils

def setUpModule():
    postgres_test_utils.setUpModule()

def tearDownModule():
    postgres_test_utils.tearDownModule()

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
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
        '%(message)s')

    unittest.main()
