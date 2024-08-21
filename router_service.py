from typing import Any, Dict, List, Optional, Tuple
import time
import logging
from threading import Lock, Condition
import json
import os
from functools import partial
import asyncio
from datetime import timedelta

import rest_service
import fastapi_service
import hypercorn_main

from blob import InlineBlob

from storage import Storage, TransactionCursor
from rest_endpoint_adapter import (
    EndpointFactory,
    RestHandlerFactory )
from output_handler import OutputHandler
from response import Response
from executor import Executor
from config import Config
from filter import AsyncFilter, SyncFilter

from storage_writer_filter import StorageWriterFilter
from storage_schema import VersionConflictException
from version_cache import IdVersionMap

class StorageWriterFactory(EndpointFactory):
    def __init__(self, service : 'Service'):
        self.service = service

    def create(self, http_host : str) -> Optional[AsyncFilter]:
        return self.service.create_storage_writer(http_host)
    def get(self, rest_id : str) -> Optional[AsyncFilter]:
        return self.service.get_storage_writer(rest_id)


class Service:
    lock : Lock
    cv : Condition
    storage : Optional[Storage] = None
    version_cache : IdVersionMap
    last_gc = 0

    rest_handler_factory : Optional[RestHandlerFactory] = None
    endpoint_factory : Optional[EndpointFactory] = None

    _shutdown = False

    config : Optional[Config] = None

    started = False
    executor : Optional[Executor] = None
    # for long-running/housekeeping stuff
    daemon_executor : Optional[Executor] = None

    hypercorn_shutdown : Optional[List[asyncio.Future]] = None

    def __init__(self, config=None):
        self.lock = Lock()
        self.cv = Condition(self.lock)

        self.config = config

        if self.config:
            self.config.storage_writer_factory = partial(
                self.create_storage_writer, None)
        self.version_cache = IdVersionMap()

        if self.daemon_executor is None:
            self.daemon_executor = Executor(10, watchdog_timeout=300,
                                            debug_futures=True)

    def shutdown(self):
        if self._shutdown:
            return
        logging.info("router_service shutdown()")
        self._shutdown = True

        if self.hypercorn_shutdown:
            logging.debug('router service hypercorn shutdown')
            try:
                self.hypercorn_shutdown[0].set_result(True)
            except:
                pass

        # tests schedule main() on daemon executor
        if self.daemon_executor is not None:
            assert(self.daemon_executor.shutdown(timeout=10))

        if self.executor is not None:
            assert(self.executor.shutdown(timeout=10))

        self.storage._del_session()

    def wait_started(self, timeout=None):
        with self.lock:
            return self.cv.wait_for(lambda: self.started, timeout)

    def main(self, config_filename=None, alive=None):
        if config_filename:
            config = Config(
                storage_writer_factory=partial(
                    self.create_storage_writer, None))
            config.load_yaml(config_filename)
            self.config = config

        global_yaml = self.config.root_yaml.get('global', {})

        if self.executor is None:
            executor_yaml = global_yaml.get('executor', {})
            self.executor = Executor(
                executor_yaml.get('max_inflight', 10),
                executor_yaml.get('watchdog_timeout', 30),
                debug_futures=executor_yaml.get(
                    'testonly_debug_futures', False))
        # ick dependency cycle
        self.config.executor = self.executor

        # TODO move most/all of this to storage i.e. just pass the yaml
        storage_yaml = self.config.root_yaml['storage']
        engine = storage_yaml.get('engine', None)
        if engine == 'sqlite_memory':
            logging.warning("*** using in-memory/non-durable storage")
            self.storage = Storage.get_sqlite_inmemory_for_test(
                self.version_cache)
        elif engine == 'sqlite':
            self.storage = Storage.connect_sqlite(
                self.version_cache,
                storage_yaml['sqlite_db_filename'])
        elif engine == 'postgres':
            args = {}
            arg_map = {'postgres_user': 'db_user',
                       'postgres_db_name': 'db_name',
                       'unix_socket_dir': 'unix_socket_dir',
                       'port': 'port'}
            for (k,v) in arg_map.items():
                if k in storage_yaml:
                    args[v] = storage_yaml[k]
            if 'db_user' not in args:
                args['db_user'] = os.getlogin()
            self.storage = Storage.connect_postgres(self.version_cache, **args)

        self.storage.recover()

        self.config.set_storage(self.storage)

        if global_yaml.get('dequeue', True):
            self.daemon_executor.submit(
                partial(self.dequeue, self.daemon_executor))

        refresh = storage_yaml.get('session_refresh_interval', 30)
        self.daemon_executor.submit(
            partial(self.refresh_storage_session,
                    self.daemon_executor, refresh))

        if storage_yaml.get('gc_interval', None):
            self.daemon_executor.submit(partial(self.gc, self.daemon_executor))
        else:
            logging.warning('gc disabled')

        # top-level: http host -> endpoint

        self.endpoint_factory = StorageWriterFactory(self)
        self.rest_handler_factory = RestHandlerFactory(
            self.executor,
            endpoint_factory = self.endpoint_factory,
            rest_id_factory = self.config.rest_id_factory())

        with self.lock:
            self.started = True
            self.cv.notify_all()

        listener_yaml = self.config.root_yaml['rest_listener']
        if listener_yaml.get('use_fastapi', False):
            app = fastapi_service.create_app(self.rest_handler_factory)
        else:
            app = rest_service.create_app(self.rest_handler_factory)
        self.hypercorn_shutdown = []
        try:
            hypercorn_main.run(
                [listener_yaml['addr']],
                listener_yaml.get('cert', None),
                listener_yaml.get('key', None),
                app,
                self.hypercorn_shutdown,
                alive=alive)
        except:
            logging.exception('router service main: hypercorn_main exception')
            pass
        self.shutdown()

    def start_main(self):
        self.daemon_executor.submit(
            partial(self.main, alive=self.daemon_executor.ping_watchdog))

    def create_storage_writer(self, http_host : str
                              ) -> Optional[StorageWriterFilter]:
        writer = StorageWriterFilter(
            storage=self.storage,
            rest_id_factory=self.config.rest_id_factory(),
            create_leased=True)
        fut = self.executor.submit(
            lambda: self._handle_new_tx(writer))
        if fut is None:
            return None
        return writer

    def get_storage_writer(self, rest_id : str
                           ) -> Optional[StorageWriterFilter]:
        return StorageWriterFilter(
            storage=self.storage, rest_id=rest_id,
            rest_id_factory=self.config.rest_id_factory())

    def _handle_new_tx(self, writer : StorageWriterFilter):
        tx_rest_id = writer.get_rest_id()
        logging.debug('RouterService._handle_new_tx %s', tx_rest_id)
        while True:
            try:
                tx_cursor = self.storage.get_transaction_cursor()
                tx_cursor.load(rest_id=tx_rest_id, start_attempt=True)
                break
            except VersionConflictException:
                pass
        endpoint, endpoint_yaml = self.config.get_endpoint(tx_cursor.tx.host)
        self.handle_tx(tx_cursor, endpoint, endpoint_yaml)

    def handle_tx(self, storage_tx : TransactionCursor,
                  endpoint : SyncFilter,
                  endpoint_yaml):
        output_yaml = endpoint_yaml.get('output_handler', {})
        handler = OutputHandler(
            storage_tx, endpoint,
            downstream_env_timeout =
              output_yaml.get('downstream_env_timeout', 30),
            downstream_data_timeout =
              output_yaml.get('downstream_data_timeout', 60),
            notification_factory=lambda: self.config.notification_endpoint(),
            mailer_daemon_mailbox=self.config.root_yaml['global'].get(
                'mailer_daemon_mailbox', None),
            retry_params = output_yaml.get('retry_params', {}))
        handler.handle()
        # TODO wrap all of this in try...finally cursor.finalize_attempt()?

    def _dequeue(self, wait : bool = True) -> bool:
        storage_tx = None
        try:
            storage_tx = self.storage.load_one()
        except VersionConflictException:
            return False
        if storage_tx is None:
            return False

        endpoint, endpoint_yaml = self.config.get_endpoint(storage_tx.tx.host)
        logging.debug('_dequeue %s %s',
                      storage_tx.id, storage_tx.rest_id)

        # XXX this is the wrong workflow, load_one() starts attempt,
        # we shouldn't do that until we know we can schedule it
        # possibly handle_tx() should do this at the end? (need to
        # refresh watchdog timeout, similar logic only to do it if
        # it's been >1s or it succeeded last time not to busy wait on
        # db!)
        self.executor.submit(
            lambda: self.handle_tx(storage_tx, endpoint, endpoint_yaml))

        # TODO wrap all of this in try...finally cursor.finalize_attempt()?
        # otherwise tx won't be recovered until storage session expiration?
        return True

    def dequeue(self, executor):
        prev = True
        while not self._shutdown:
            executor.ping_watchdog()
            prev = self._dequeue(wait=(not prev))
            if not prev:
                with self.lock:
                    self.cv.wait_for(lambda: self._shutdown, 1)


    def gc(self, executor):
        storage_yaml = self.config.root_yaml['storage']
        ttl = storage_yaml.get('gc_ttl', 86400)
        interval = storage_yaml.get('gc_interval', 300)
        while not self.shutdown:
            executor.ping_watchdog()
            self._gc(ttl)
            with self.lock:
                self.cv.wait_for(lambda: self._shutdown, interval)

    def refresh_storage_session(self, executor, interval : int):
        while not self._shutdown:
            executor.ping_watchdog()
            assert self.storage._refresh_session()
            self.storage._gc_session(timedelta(seconds = 10 * interval))
            with self.lock:
                self.cv.wait_for(lambda: self._shutdown, interval)

    def _gc(self, gc_ttl=None):
        logging.info('router_service _gc %d', gc_ttl)
        count = self.storage.gc(gc_ttl)
        logging.info('router_service _gc deleted %d tx %d blobs',
                     count[0], count[1])
        return count
