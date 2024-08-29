
import unittest
import logging
import time

from executor import Executor
from threading import Semaphore

def raise_exception():
    raise Exception()

class ExecutorTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(message)s')

    def testReturn(self):
        ex = Executor(1, 1)
        fut = ex.submit(lambda: 3)
        self.assertEqual(fut.result(), 3)

    def testDebugFutures(self):
        ex = Executor(1, 10, debug_futures=True)
        self.assertIsNotNone(ex.submit(raise_exception))
        with self.assertRaises(Exception):
            ex.shutdown()

    def testNonBlocking(self):
        ex = Executor(1, 10)
        sem = Semaphore(0)
        fut = ex.submit(lambda: sem.acquire())
        fut2 = ex.submit(lambda: sem.acquire())
        fut3 = ex.submit(lambda: None, timeout=0)
        self.assertIsNone(fut3)
        self.assertTrue(fut2.cancel())
        self.assertTrue(fut2.cancelled())
        sem.release(1)
        fut.result()

    def testWatchdogTimeout(self):
        ex = Executor(1, watchdog_timeout=1)
        ex.submit(lambda: time.sleep(5))
        time.sleep(1)
        with self.assertRaises(Exception):
            ex.submit(lambda: None)
        with self.assertRaises(Exception):
            ex.check_watchdog()
        with self.assertRaises(Exception):
            ex.shutdown(1)


if __name__ == '__main__':
    unittest.main()
