from typing import List, Tuple
from threading import (
    Condition,
    Lock )
import logging

import asyncio
from functools import partial

class IdVersion:
    id : int
    lock : Lock
    cv : Condition

    async_waiters : List[Tuple[object,asyncio.Future]]

    version : int
    waiters : set[object]
    def __init__(self, db_id, version):
        self.id = db_id
        self.lock = Lock()
        self.cv = Condition(self.lock)

        self.waiters = set()
        self.version = version

        self.async_waiters = []

    def wait(self, version, timeout):
        with self.lock:
            logging.debug('IdVersion.wait %d %d %d %d',
                          id(self), self.id, self.version, version)
            rv = self.cv.wait_for(lambda: self.version > version, timeout)
            logging.debug('IdVersion.wait done %d %d %d %s',
                          self.id, self.version, version, rv)
            return rv

    def add_async_waiter(self, loop, future):
        with self.lock:
            self.async_waiters.append((loop,future))

    def update(self, version):
        with self.lock:
            logging.debug('IdVersion.update %d id=%d version=%d new %d',
                          id(self), self.id, self.version, version)
            # There is an expected edge case case where a waiter reads
            # the db and updates this in between a write commiting to
            # the db and updating this so == is expected
            if version < self.version:
                logging.critical('IdVersion.update precondition failure '
                                 ' id=%d cur=%d new=%d',
                                 self.id, self.version, version)
                assert not 'IdVersion.update precondition failure '
            self.version = version
            self.cv.notify_all()

            def done(afut, version):
                logging.debug('update done')
                # XXX currently throws InvalidStateError after waiter
                # timed out? this is benign?
                afut.set_result(version)

            for loop,future in self.async_waiters:
                logging.debug('sched update done')
                loop.call_soon_threadsafe(partial(done, future, version))
            self.async_waiters = []


class Waiter:
    def __init__(self, parent, db_id, version):
        self.parent = parent
        self.db_id = db_id
        self.id_version = self.parent.get_id_version(db_id, version, self)
    def __enter__(self):
        return self.id_version
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.parent.del_waiter(self.db_id, self.id_version, self)

class IdVersionMap:
    lock : Lock
    id_version_map : dict[int,IdVersion]  # db-id

    def __init__(self):
        self.id_version_map = {}
        self.lock = Lock()

    def del_waiter(self, id, id_version, obj):
        with self.lock:
            id_version.waiters.remove(obj)
            if not id_version.waiters:
                del self.id_version_map[id]

    def update(self, db_id, version):
        with self.lock:
            if db_id not in self.id_version_map:
                logging.debug('IdVersionMap.update id=%d version %d no waiters',
                              db_id, version)
                return
            waiter = self.id_version_map[db_id]
            waiter.update(version)

    def get_id_version(self, db_id, version, obj) -> IdVersion:
        with self.lock:
            if db_id not in self.id_version_map:
                self.id_version_map[db_id] = IdVersion(db_id, version)
            id_version = self.id_version_map[db_id]
            id_version.waiters.add(obj)
            return id_version

    def wait(self, id, version, timeout=None) -> bool:
        with Waiter(self, id, version) as id_version:
            return id_version.wait(version, timeout)

    async def wait_async(self, id, version, timeout):
        loop = asyncio.get_running_loop()
        afut = loop.create_future()

        with Waiter(self, id, version) as id_version:
            id_version.add_async_waiter(loop, afut)

            try:
                await asyncio.wait_for(afut, timeout)
                return True
            except TimeoutError:
                return False
