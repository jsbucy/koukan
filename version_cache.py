from typing import List, Optional, Tuple
from threading import (
    Condition,
    Lock )
import logging

import asyncio
from functools import partial

class IdVersion:
    id : int
    rest_id : str
    lock : Lock
    cv : Condition

    async_waiters : List[Tuple[object,asyncio.Future]]

    version : int

    def __init__(self, db_id, rest_id, version):
        self.id = db_id
        self.rest_id = rest_id
        self.lock = Lock()
        self.cv = Condition(self.lock)

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


# class Waiter:
#     def __init__(self, parent, db_id, rest_id, version):
#         self.parent = parent
#         self.db_id = db_id
#         self.id_version = self.parent.get_id_version(
#             db_id, rest_id, version, self)
#     def __enter__(self):
#         return self.id_version
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         self.parent.del_waiter(self.db_id, self.id_version, self)

class IdVersionMap:
    lock : Lock
    id_version_map : dict[int,IdVersion]  # db-id
    rest_id_map : dict[str,int]  # db-id

    def __init__(self):
        self.id_version_map = {}
        self.rest_id_map = {}
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
                self.rest_id_map[rest_id] = db_id
            else:
                id_version.update(version)
            return id_version

    def update_and_unref(self, db_id : int, rest_id : str, version : int):
        return
        with self.lock:
            id_version = self.id_version_map.get(db_id)
            id_version.update(version)
            del self.id_version_map[db_id]
            del self.rest_id_map[id_version.rest_id]


    def wait(self, db_id : int, version : int, timeout : Optional[float] = None
             ) -> bool:
        # precondition: get_id/rest_id() returned non-none
        # (or previous call to insert_or_update())
        id_version = self.id_version_map.get(db_id)
        assert id_version is not None
        return id_version.wait(version, timeout)

    def get(self, db_id : Optional[int] = None, rest_id : Optional[str] = None
            ) -> Optional[IdVersion]:
        with self.lock:
            if db_id is None:
                if rest_id is not None:
                    db_id = self.rest_id_map.get(rest_id, None)
                else:
                    raise ValueError
            return self.id_version_map.get(db_id, None)

    async def wait_async(self,
                         version : int,
                         timeout : float,
                         db_id : Optional[int] = None,
                         rest_id : Optional[str] = None):
        loop = asyncio.get_running_loop()
        afut = loop.create_future()

        id_version = self.get(db_id, rest_id)
        assert id_version is not None  # precondition
        id_version.add_async_waiter(loop, afut)
        try:
            await asyncio.wait_for(afut, timeout)
            return True
        except TimeoutError:
            return False
