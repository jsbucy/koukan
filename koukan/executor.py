# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Dict, List, Optional
import logging
import time
import faulthandler
import sys
from functools import partial

from threading import (
    Condition,
    Lock,
    Thread,
    current_thread )
from concurrent.futures import Future

class Executor:
    INTERNAL_TIMEOUT = 30.0
    inflight_limit : int
    inflight : Dict[Thread, int]  # start time
    lock : Lock
    cv : Condition
    watchdog_timeout : Optional[float] = None
    _shutdown : bool = False
    worker_exception = False

    def __init__(self, inflight_limit,
                 watchdog_timeout : Optional[float] = None):
        self.inflight_limit = inflight_limit
        self.inflight = {}

        self.lock = Lock()
        self.cv = Condition(self.lock)
        self.watchdog_timeout = watchdog_timeout

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
        if not self.lock.acquire(timeout=self.INTERNAL_TIMEOUT):
            logging.error('Executor.check_watchdog() timed out acquiring '
                          'lock, possible deadlock?')
            return False
        try:
            return self._check_watchdog_locked()
        finally:
            self.lock.release()

    def submit(self, fn, timeout=None) -> Optional[Future]:
        with self.lock:
            if self._shutdown:
                return None
            start = time.monotonic()
            if not self.cv.wait_for(
                    lambda: len(self.inflight) < self.inflight_limit,
                    timeout):
                return None

            fut : Future = Future()
            t = Thread(target = partial(self._run, fut, fn),
                       daemon=True)
            self.inflight[t] = int(start)
        t.start()
        return fut

    def _run(self, fut : Future, fn):
        this_thread = current_thread()
        assert this_thread in self.inflight
        try:
            return fut.set_result(fn())
        except Exception as e:
            logging.exception('Executor._run() exception')
            with self.lock:
                self.worker_exception = True
            fut.set_exception(e)
        finally:
            with self.lock:
                del self.inflight[this_thread]
                self.cv.notify_all()


    def ping_watchdog(self):
        with self.lock:
            t = current_thread()
            assert t in self.inflight
            self.inflight[t] = time.monotonic()
        return True

    def shutdown(self, timeout : Optional[float] = None) -> bool:
        t_o = timeout if timeout is not None else self.INTERNAL_TIMEOUT
        if not self.lock.acquire(timeout=t_o):
            logging.error('Executor.shutdown() timed out acquiring '
                          'lock, possible deadlock?')
            return False

        try:
            # stop new work coming in
            self._shutdown = True

            watchdog_result = True
            if timeout:
                for i in range(0, int(timeout)):
                    logging.debug('Executor.shutdown waiting on %s',
                                  self.inflight)
                    watchdog_result = self._check_watchdog_locked(timeout)
                    if not watchdog_result:
                        break
                    if self.cv.wait_for(lambda: len(self.inflight) == 0,
                                        timeout=1):
                        break
            assert not self.worker_exception
        finally:
            self.lock.release()

        return watchdog_result

    def wait_empty(self, timeout):
        with self.lock:
            return self.cv.wait_for(lambda: not self.inflight, timeout)
