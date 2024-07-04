from typing import Any, Dict, List, Optional, Tuple
import time
import logging
from threading import Lock, Condition, Thread
import json
import os
from functools import partial
import asyncio

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
    last_gc = 0

    rest_handler_factory : Optional[RestHandlerFactory] = None
    endpoint_factory : Optional[EndpointFactory] = None

    # dequeue watermark
    created_id : Optional[int] = None

    dequeue_thread : Optional[Thread] = None
    gc_thread : Optional[Thread] = None

    _shutdown = False

    config : Optional[Config] = None

    started = False
    executor : Optional[Executor] = None
    owned_executor : Optional[Executor] = None

    hypercorn_shutdown : Optional[List[asyncio.Future]] = None

    def __init__(self, config=None,
                 executor : Optional[Executor] = None):
        self.lock = Lock()
        self.cv = Condition(self.lock)

        self.config = config
        self.executor = executor

        if self.config:
            self.config.storage_writer_factory = partial(
                self.create_storage_writer, None)

    def shutdown(self):
        logging.info("router_service shutdown()")
        self._shutdown = True
        if self.dequeue_thread is not None:
            self.dequeue_thread.join()
        if self.gc_thread is not None:
            self.gc_thread.join()

        if self.owned_executor is not None:
            assert(self.owned_executor.shutdown(timeout=5))

        if self.hypercorn_shutdown:
            logging.debug('router service hypercorn shutdown')
            self.hypercorn_shutdown[0].set_result(True)

    def wait_started(self, timeout=None):
        with self.lock:
            return self.cv.wait_for(lambda: self.started, timeout)

    def main(self, config_filename=None):
        if config_filename:
            config = Config(
                storage_writer_factory=partial(
                    self.create_storage_writer, None))
            config.load_yaml(config_filename)
            self.config = config

        global_yaml = self.config.root_yaml.get('global', {})

        if self.executor is None:
            executor_yaml = global_yaml.get('executor', {})
            self.owned_executor = Executor(
                executor_yaml.get('max_inflight', 10),
                executor_yaml.get('watchdog_timeout', 30))
            self.executor = self.owned_executor
        # ick dependency cycle
        self.config.executor = self.executor

        # TODO move most/all of this to storage i.e. just pass the yaml
        storage_yaml = self.config.root_yaml['storage']
        engine = storage_yaml.get('engine', None)
        if engine == 'sqlite_memory':
            logging.warning("*** using in-memory/non-durable storage")
            self.storage = Storage.get_sqlite_inmemory_for_test()
        elif engine == 'sqlite':
            self.storage = Storage.connect_sqlite(
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
            self.storage = Storage.connect_postgres(**args)

        self.storage.recover()

        self.config.set_storage(self.storage)

        # TODO storage should manage this internally?
        if global_yaml.get('dequeue', True):
            self.dequeue_thread = Thread(target = lambda: self.dequeue(),
                                         daemon=True)
            self.dequeue_thread.start()

        if storage_yaml.get('gc_interval', None):
            self.gc_thread = Thread(target = lambda: self.gc(),
                                    daemon=True)
            self.gc_thread.start()
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
        if listener_yaml['use_fastapi']:
            app = fastapi_service.create_app(self.rest_handler_factory)
        else:
            app = rest_service.create_app(self.rest_handler_factory)
        self.hypercorn_shutdown = []
        hypercorn_main.run(
            [listener_yaml['addr']],
            listener_yaml.get('cert', None),
            listener_yaml.get('key', None),
            app,
            self.hypercorn_shutdown)

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
        #self.storage.wait_created(self.created_id, timeout=1 if wait else 0)
        try:
            storage_tx = self.storage.load_one()
        except VersionConflictException:
            return False
        logging.info("dequeued id=%s", storage_tx.id if storage_tx else None)
        if storage_tx is None:
            return False

        if self.created_id is None or (
                storage_tx.id > self.created_id):
            self.created_id = storage_tx.id

        endpoint, endpoint_yaml = self.config.get_endpoint(storage_tx.tx.host)
        logging.info('_dequeue %s %s', endpoint, endpoint_yaml)
        # xxx this could fail, finalize attempt (below)
        self.executor.submit(
            lambda: self.handle_tx(storage_tx, endpoint, endpoint_yaml))

        # TODO wrap all of this in try...finally cursor.finalize_attempt()?
        return True

    def dequeue(self):
        prev = True
        while not self._shutdown:
            logging.info("RouterService.dequeue")
            prev = self._dequeue(wait=(not prev))
            if not prev:
                time.sleep(1)

    def gc(self):
        storage_yaml = self.config.root_yaml['storage']
        while True:
            self._gc(storage_yaml.get('gc_ttl', 86400))
            with self.lock:
                if self.cv.wait_for(lambda: self.shutdown,
                                    storage_yaml.get('gc_interval', 300)):
                    break

    def _gc(self, gc_ttl=None):
        logging.info('router_service _gc %d', gc_ttl)
        count = self.storage.gc(gc_ttl)
        logging.info('router_service _gc deleted %d tx %d blobs',
                     count[0], count[1])
        return count
