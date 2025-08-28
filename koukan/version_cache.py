# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import List, Optional, Tuple
from threading import (
    Condition,
    Lock )
import logging

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

    def __init__(self, db_id, rest_id, version):
        self.id = db_id
        self.rest_id = rest_id
        self.lock = Lock()
        self.cv = Condition(self.lock)

        self.version = version

        self.async_waiters = []

    def get(self):
        with self.lock:
            return self.version

    def wait(self, version, timeout):
        with self.lock:
            logging.debug('IdVersion.wait %d %d %d %d',
                          id(self), self.id, self.version, version)
            rv = self.cv.wait_for(lambda: self.version > version, timeout)
            logging.debug('IdVersion.wait done %d %d %d %s',
                          self.id, self.version, version, rv)
            return rv

    def update(self, version):
        with self.lock:
            logging.debug('IdVersion.update %d id=%d version=%d new %d',
                          id(self), self.id, self.version, version)
            if version < self.version:
                raise VersionConflictException()
            if version == self.version:
                return
            self.version = version
            self.cv.notify_all()

            def done(afut, version):
                logging.debug('update done')
                # XXX currently throws InvalidStateError after waiter
                # timed out? this is benign?
                afut.set_result(version)

            for loop,future,waiter_version in self.async_waiters:
                assert version > waiter_version
                logging.debug('sched update done')
                loop.call_soon_threadsafe(partial(done, future, version))
            self.async_waiters = []


    async def wait_async(self,
                         version : int,
                         timeout : Optional[float]) -> bool:
        loop = asyncio.get_running_loop()
        afut = loop.create_future()

        with self.lock:
            if self.version > version:
                logging.debug('cache version %d version %d', self.version, version)
                return True
            self.async_waiters.append((loop, afut, version))

        try:
            new_version = await asyncio.wait_for(afut, timeout)
            logging.debug('new_version %d version %d', new_version, version)
            assert new_version > version
            return True
        except TimeoutError:
            with self.lock:
                for i,(loop,fut,version) in enumerate(self.async_waiters):
                    if fut == afut:
                        del self.async_waiters[i]
                        break
            return False


class IdVersionMap:
    lock : Lock
    # db_id -> IdVersion
    id_version_map : WeakValueDictionary[int, IdVersion]
    # rest_id -> IdVersion
    rest_id_map : WeakValueDictionary[str, IdVersion]

    def __init__(self):
        self.id_version_map = WeakValueDictionary()
        self.rest_id_map = WeakValueDictionary()
        self.lock = Lock()

    def insert_or_update(self, db_id : int, rest_id : str, version : int
                         ) -> IdVersion:
        logging.debug('IdVersionMap.insert_or_update %d %s %d',
                      db_id, rest_id, version)
        with self.lock:
            id_version = self.id_version_map.get(db_id, None)
            if id_version is None:
                id_version = IdVersion(db_id, rest_id, version)
                self.id_version_map[db_id] = id_version
                self.rest_id_map[rest_id] = id_version
            else:
                id_version.update(version)
            return id_version

    def get(self, db_id : Optional[int] = None, rest_id : Optional[str] = None
            ) -> Optional[IdVersion]:
        with self.lock:
            if db_id is not None:
                return self.id_version_map.get(db_id, None)
            elif rest_id is not None:
                return self.rest_id_map.get(rest_id, None)
            else:
                raise ValueError
