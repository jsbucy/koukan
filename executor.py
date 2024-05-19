from typing import Dict, List, Optional
import logging
import time
import faulthandler
import sys

from threading import Lock, Condition, Thread, current_thread, BoundedSemaphore
from concurrent.futures import Future, ThreadPoolExecutor

# wrapper for ThreadPoolExecutor
# - blocks instead of unbounded queueing if pool is full
# - adds watchdog timer
# - test mode to save futures and check them all at shutdown to
# propagate uncaught excptions
class Executor:
    inflight_sem : BoundedSemaphore
    inflight : Dict[Thread, int]  # start time
    lock : Lock
    cv : Condition
    executor : ThreadPoolExecutor
    watchdog_timeout : int
    debug_futures : Optional[List[Future]] = None

    def __init__(self, inflight_limit, watchdog_timeout : int,
                 debug_futures=False):
        self.inflight = {}
        self.inflight_sem = BoundedSemaphore(inflight_limit)

        self.lock = Lock()
        self.cv = Condition(self.lock)
        self.executor = ThreadPoolExecutor(inflight_limit)
        self.watchdog_timeout = watchdog_timeout
        if debug_futures:
            self.debug_futures = []

    def _check_watchdog_locked(self):
        now = time.monotonic()
        for (thread, start) in self.inflight.items():
            runtime = now - start
            if runtime > self.watchdog_timeout:
                logging.critical(
                    'Executor thread watchdog timeout '
                    'tid %d %d 0x%x runtime %d',
                    thread.native_id, thread.ident, thread.ident, runtime)
                faulthandler.dump_traceback()
                assert False

    def check_watchdog(self):
        with self.lock:
            self._check_watchdog_locked()

    def submit(self, fn, timeout=None) -> Optional[Future]:
        with self.lock:
            start = time.monotonic()
            self._check_watchdog_locked()
            if not self.inflight_sem.acquire(
                    blocking=(timeout is None or timeout > 0),
                    timeout=(None if timeout == 0 else timeout)):
                return None

            fut = self.executor.submit(lambda: self._run(fn))
            if fut and (self.debug_futures is not None):
                self.debug_futures.append(fut)
            return fut

    def _run(self, fn):
        this_thread = current_thread()
        self.inflight[this_thread] = int(time.monotonic())
        try:
            fn()
        except Exception:
            logging.exception('Executor._run() exception')
            raise
        finally:
            with self.lock:
                self.inflight_sem.release(1)
                del self.inflight[this_thread]
                self.cv.notify_all()

    def shutdown(self, timeout=None) -> bool:
        ex = self.executor
        self.executor = None  # stop new work coming in
        with self.lock:
            for i in range(0, int(self.watchdog_timeout)):
                logging.debug('Executor.shutdown waiting on %d',
                              len(self.inflight))
                self._check_watchdog_locked()
                if self.cv.wait_for(lambda: len(self.inflight) == 0,
                                    timeout=1):
                    break

        ex.shutdown(wait=True, cancel_futures=True)
        if self.debug_futures:
            for fut in self.debug_futures:
                fut.result()  # propagate exceptions
        return True
