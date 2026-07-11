import logging
import unittest
from koukan.router_service_test import RouterServiceTest
import koukan.postgres_test_utils as postgres_test_utils

def setUpModule():
    postgres_test_utils.setUpModule()

def tearDownModule():
    postgres_test_utils.tearDownModule()

class RouterServiceTestFastApi(RouterServiceTest):
    def _setup_storage(self):
        self.pg, self.storage_url = postgres_test_utils.setup_postgres()

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,  # WARNING
        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
        '%(message)s')
    unittest.removeHandler()
    unittest.main(catchbreak=False)
