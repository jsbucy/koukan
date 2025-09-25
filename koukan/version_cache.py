# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Any, List, Optional, Set, Tuple
from threading import (
    Condition,
    Lock )
import logging
import time

from weakref import WeakValueDictionary

import asyncio
from functools import partial
from koukan.storage_schema import VersionConflictException

class IdVersion:
    id : int
    rest_id : str
    lock : Lock
    cv : Condition

    async_waiters : List[Tuple[asyncio.AbstractEventLoop,asyncio.Future,int]]

    version : int
    cursor : Optional[Any] = None
    leased = False
    last_update : float
    final = False
    ttl : Optional[int]

    def __init__(self, db_id : int, rest_id : str, version : int,
                 ttl : Optional[int] = None,
                 cursor : Optional[Any] = None,
                 leased = False):
        self.id = db_id
        self.rest_id = rest_id
        self.lock = Lock()
        self.cv = Condition(self.lock)
        self.ttl = ttl

        self.version = version

        self.async_waiters = []
        self.leased = leased
        #assert self.leased is True or cursor is None
        if self.leased and cursor is not None:
            assert cursor.version == version
            self.cursor = cursor
        self.last_update = time.monotonic()

    def _expired_unlocked(self):
        if self.ttl is None:
            return False
        age = time.monotonic() - self.last_update
        logging.debug(age)
        return not self.leased and (age > self.ttl)

    def expired(self):
        with self.lock:
            return self._expired_unlocked()


    def get(self):
        with self.lock:
            return self.version

    def _reusable(self):
        # Only reuse unleased if it has final_attempt_reason i.e. is
        # unlikely to change further. In a multi-node/cluster setting,
        # there is the possibility of a tx continuing to be mutated at
        # a different node after the initial attempt has tempfailed
        # and the cache getting stale which may lead to weird
        # "stuck states."
        return self.leased or (not self._expired_unlocked() and
                               self.cursor is not None and
                               self.cursor.final_attempt_reason is not None)

    def wait(self, version, timeout, tx_out : Optional[Any] = None
             ) -> Tuple[bool, bool]:
        with self.lock:
            logging.debug('IdVersion.wait %d %d %d %d',
                          id(self), self.id, self.version, version)
            # don't wait/early return if unleased?
            rv = self.cv.wait_for(lambda: self.version > version, timeout)
            logging.debug('IdVersion.wait done id=%d new=%d arg=%d rv=%s '
                          'leased=%s cursor=%s',
                          self.id, self.version, version, rv, self.leased,
                          self.cursor)
            clone = False
            # the update that sets leased to false should still be
            # able to reuse the tx?
            if self.cursor and self._reusable() and tx_out is not None:
                assert self.cursor.version is not None
                assert self.cursor.version == self.version
                assert self.cursor.tx is None or self.cursor.tx.version is None
                tx_out.copy_from(self.cursor)
                clone = True
            return rv, clone

    def update(self, version, cursor : Optional[Any] = None,
               leased : Optional[bool] = False):
        with self.lock:
            logging.debug('IdVersion.update %d id=%d self.version=%d '
                          'new version %d cursor=%s self.leased=%s leased=%s',
                          id(self), self.id, self.version, version, cursor,
                          self.leased, leased)
            if version < self.version:
                raise VersionConflictException()
            if version == self.version:
                return
            self.last_update = time.monotonic()
            self.version = version
            if leased is not None:
                self.leased = leased
            if cursor is not None:
                assert cursor.version == version
                assert cursor.tx is None or cursor.tx.version is None
                self.cursor = cursor
            else:
                self.cursor = None
            self.cv.notify_all()

            def done(afut, version, cursor):
                logging.debug('async wakeup done')
                # XXX currently throws InvalidStateError after waiter
                # timed out? this is benign?
                afut.set_result((version, cursor))

            for loop,future,waiter_version in self.async_waiters:
                assert version > waiter_version
                loop.call_soon_threadsafe(
                    partial(done, future, version,
                            self.cursor if self._reusable() else None))
                logging.debug('sched async wakeup done %s', self.cursor)

            self.async_waiters = []


    async def wait_async(self,
                         version : int,
                         timeout : Optional[float],
                         cursor_out : Optional[Any] = None
                         ) -> Tuple[bool, bool]:
        loop = asyncio.get_running_loop()
        afut = loop.create_future()

        with self.lock:
            if self.version > version:
                logging.debug('cache version %d version %d',
                              self.version, version)
                return True, False
            self.async_waiters.append((loop, afut, version))

        try:
            new_version, cursor = await asyncio.wait_for(afut, timeout)
            logging.debug('new_version %d version %d', new_version, version)
            assert new_version > version
            logging.debug('%s %s', cursor, cursor_out)
            clone = False
            if cursor is not None and cursor_out is not None:
                assert cursor.version == new_version
                cursor_out.copy_from(cursor)
                clone = True
            return True, clone
        except TimeoutError:
            with self.lock:
                for i,(loop,fut,version) in enumerate(self.async_waiters):
                    if fut == afut:
                        del self.async_waiters[i]
                        break
            return False, False


class IdVersionMap:
    lock : Lock
    # TODO I think this wants to become a more conventional ttl/lru
    # cache now since we want to be able to read the final tx result
    # from the cache after the OutputHandler has finished and it will
    # probably become unreferenced in the meantime. The minimum
    # interval between a tx being leased in different replicas is at
    # least the minimum of - min retry time ~60s
    # retry_params.min_attempt_time - session_ttl 10x
    # session_refresh_interval ~30s

    # db_id -> IdVersion
    id_version_map : WeakValueDictionary[int, IdVersion]
    # rest_id -> IdVersion
    rest_id_map : WeakValueDictionary[str, IdVersion]

    ttl_refs : Set[IdVersion]
    last_gc : float
    ttl : Optional[int] = None

    def __init__(self, ttl=None):
        self.id_version_map = WeakValueDictionary()
        self.rest_id_map = WeakValueDictionary()
        self.lock = Lock()
        self.ttl_refs = set()
        self.last_gc = time.monotonic()
        self.ttl = ttl

    def gc(self):
        if self.ttl is None:
            return
        now = time.monotonic()
        if now - self.last_gc < self.ttl:
            return
        self.last_gc = now
        to_delete = []
        for i in self.ttl_refs:
            if i.expired():
                to_delete.append(i)
        for i in to_delete:
            self.ttl_refs.remove(i)

    def insert_or_update(self, db_id : int, rest_id : str, version : int,
                         cursor : Optional[Any] = None,
                         leased : Optional[bool] = None
                         ) -> IdVersion:
        logging.debug('IdVersionMap.insert_or_update %d %s %d',
                      db_id, rest_id, version)
        with self.lock:
            id_version = self.id_version_map.get(db_id, None)
            if id_version is None:
                id_version = IdVersion(
                    db_id, rest_id, version,
                    ttl=self.ttl, cursor=cursor, leased=leased)
                self.id_version_map[db_id] = id_version
                self.rest_id_map[rest_id] = id_version
            else:
                id_version.update(version, cursor, leased)
            if self.ttl is not None:
                self.ttl_refs.add(id_version)
            return id_version

    def get(self, db_id : Optional[int] = None, rest_id : Optional[str] = None
            ) -> Optional[IdVersion]:
        with self.lock:
            self.gc()
            if db_id is not None:
                idv = self.id_version_map.get(db_id, None)
            elif rest_id is not None:
                idv = self.rest_id_map.get(rest_id, None)
            else:
                raise ValueError
            # will take a GC cycle for erasing from ttl to propagate
            # to weak value dict
            return idv if idv is not None and idv._reusable() else None
