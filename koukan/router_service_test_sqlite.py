import logging
import unittest
from koukan.router_service_test import RouterServiceTest

import koukan.sqlite_test_utils as sqlite_test_utils

class RouterServiceTestSqlite(RouterServiceTest):
    router_yaml = 'router_service_test-router.yaml'
    def _setup_storage(self):
        self.dir, self.storage_url = sqlite_test_utils.create_temp_sqlite_for_test()

    # having multiple routers accessing the same sqlite causes
    # concurrency errors, probably not a useful configuration
    def test_multi_node(self):
        pass

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,  # WARNING
        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
        '%(message)s')

    unittest.removeHandler()
    unittest.main(catchbreak=False)
