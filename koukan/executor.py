# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Dict, List, Optional
import logging
import time
import faulthandler
import sys
from functools import partial

from threading import (
    BoundedSemaphore,
    Condition,
    Lock,
    Thread,
    current_thread )
from concurrent.futures import Future

class Executor:
    inflight_sem : BoundedSemaphore
    inflight : Dict[Thread, int]  # start time
    lock : Lock
    cv : Condition
    watchdog_timeout : Optional[int] = None
    debug_futures : Optional[List[Future]] = None
    _shutdown : bool = False

    def __init__(self, inflight_limit,
                 watchdog_timeout : Optional[int] = None,
                 debug_futures=False):
        self.inflight = {}
        self.inflight_sem = BoundedSemaphore(inflight_limit)

        self.lock = Lock()
        self.cv = Condition(self.lock)
        self.watchdog_timeout = watchdog_timeout
        if debug_futures:
            self.debug_futures = []

    def _check_watchdog_locked(self, timeout = None) -> bool:
        if timeout is None:
            timeout = self.watchdog_timeout
        if timeout is None:
            return True
        now = time.monotonic()
        for (thread, start) in self.inflight.items():
            runtime = now - start
            if runtime > timeout:
                logging.critical(
                    'Executor thread watchdog timeout '
                    'tid %d %d 0x%x runtime %d',
                    thread.native_id, thread.ident, thread.ident, runtime)
                faulthandler.dump_traceback()
                return False
        return True

    def check_watchdog(self) -> bool:
        with self.lock:
            return self._check_watchdog_locked()

    def submit(self, fn, timeout=None) -> Optional[Future]:
        with self.lock:
            if self._shutdown:
                return None
            start = time.monotonic()
            if not self.inflight_sem.acquire(
                    blocking=(timeout is None or timeout > 0),
                    timeout=(None if timeout == 0 else timeout)):
                return None

            fut : Future = Future()
            t = Thread(target = partial(self._run, fut, fn),
                       daemon=True)
            self.inflight[t] = int(start)
        t.start()

        if fut and (self.debug_futures is not None):
            with self.lock:
                self.debug_futures.append(fut)
        return fut

    def _run(self, fut : Future, fn):
        this_thread = current_thread()
        assert this_thread in self.inflight
        try:
            return fut.set_result(fn())
        except Exception as e:
            logging.exception('Executor._run() exception')
            fut.set_exception(e)
        finally:
            with self.lock:
                self.inflight_sem.release(1)
                del self.inflight[this_thread]
                self.cv.notify_all()


    def ping_watchdog(self):
        with self.lock:
            t = current_thread()
            assert t in self.inflight
            self.inflight[t] = time.monotonic()
        return True

    def shutdown(self, timeout : Optional[int] = None) -> bool:
        with self.lock:
            # stop new work coming in
            self._shutdown = True

        watchdog_result = True
        if timeout:
            for i in range(0, int(timeout)):
                logging.debug('Executor.shutdown waiting on %s',
                              self.inflight)
                with self.lock:
                    watchdog_result = self._check_watchdog_locked(timeout)
                    if not watchdog_result:
                        break
                    if self.cv.wait_for(lambda: len(self.inflight) == 0,
                                        timeout=1):
                        break

        if self.debug_futures:
            for fut in self.debug_futures:
                fut.result()  # propagate exceptions
        return watchdog_result
