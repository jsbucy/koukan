# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Dict, List, Optional, Tuple
import time
import logging
from threading import Lock, Condition
from functools import partial
import asyncio
from datetime import timedelta
import yaml
import secrets

import koukan.fastapi_service as fastapi_service
import koukan.hypercorn_main as hypercorn_main

from koukan.storage import Storage, TransactionCursor
from koukan.rest_handler import (
    EndpointFactory,
    RestHandlerFactory )
from koukan.output_handler import OutputHandler
from koukan.executor import Executor
from koukan.filter_chain_factory import FilterChainFactory
from koukan.filter_chain_wiring import FilterChainWiring
from koukan.filter import (
    AsyncFilter,
    SyncFilter,
    TransactionMetadata )
from koukan.storage_writer_filter import StorageWriterFilter
from koukan.deadline import Deadline

class StorageWriterFactory(EndpointFactory):
    def __init__(self, service : 'Service'):
        self.service = service

    def create(self, http_host : str) -> Optional[Tuple[AsyncFilter, dict]]:
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

    _shutdown = False

    filter_chain_factory : Optional[FilterChainFactory] = None

    started = False
    rest_executor : Optional[Executor] = None
    output_executor : Optional[Executor] = None
    # for long-running/housekeeping stuff
    daemon_executor : Optional[Executor] = None

    hypercorn_shutdown : Optional[asyncio.Event] = None
    _rest_id_entropy : int = 16
    root_yaml : Optional[dict] = None


    def __init__(self, root_yaml=None):
        self.lock = Lock()
        self.cv = Condition(self.lock)

        self.root_yaml = root_yaml

        if self.daemon_executor is None:
            self.daemon_executor = Executor(10, watchdog_timeout=300,
                                            debug_futures=True)

    def wait_shutdown(self, timeout : float, executor : Executor) -> bool:
        deadline = Deadline(timeout)
        while deadline.remaining():
            executor.ping_watchdog()
            with self.lock:
                self.cv.wait_for(lambda: self._shutdown,
                                 min(deadline.deadline_left(), 30))
                if self._shutdown:
                    return True
        return False

    def shutdown(self) -> bool:
        logging.info("router_service shutdown()")
        with self.lock:
            if self._shutdown:
                return
            self._shutdown = True
            self.cv.notify_all()

        if self.hypercorn_shutdown:
            logging.debug('router service hypercorn shutdown')
            try:
                self.hypercorn_shutdown.set()
            except:
                pass

        # tests schedule main() on daemon executor
        success = True
        for executor in [self.daemon_executor, self.rest_executor,
                         self.output_executor]:
            if not executor.shutdown(timeout=10):
                success = False

        self.storage._del_session()
        logging.info("router_service shutdown() done")
        return success

    def wait_started(self, timeout=None):
        with self.lock:
            return self.cv.wait_for(lambda: self.started, timeout)

    def main(self, config_filename=None, alive=None):
        if config_filename:
            with open(config_filename, 'r') as yaml_file:
                self.root_yaml = yaml.load(yaml_file, Loader=yaml.CLoader)
        self.filter_chain_factory = FilterChainFactory(self.root_yaml)
        self.wiring = FilterChainWiring(self.create_exploder_output)
        self.wiring.wire(self.root_yaml, self.filter_chain_factory)

        if 'global' in self.root_yaml:
            if 'rest_id_entropy' in self.root_yaml['global']:
                e = self.root_yaml['global']['rest_id_entropy']
                assert isinstance(e, int)
                self._rest_id_entropy = e

        logging_yaml = self.root_yaml.get('logging', None)
        if logging_yaml:
            logging.config.dictConfig(logging_yaml)

        global_yaml = self.root_yaml.get('global', {})

        executor_yaml = global_yaml.get('executor', {})
        def executor(yaml):
            return Executor(
                yaml.get('max_inflight', 10),
                yaml.get('watchdog_timeout', 30),
                debug_futures=yaml.get(
                    'testonly_debug_futures', False))
        self.rest_executor = executor(executor_yaml.get('rest', {}))
        self.output_executor = executor(executor_yaml.get('output', {}))

        storage_yaml = self.root_yaml['storage']
        listener_yaml = self.root_yaml['rest_listener']

        if self.storage is None:
            self.storage=Storage.connect(
                storage_yaml['url'], listener_yaml['session_uri'])

        session_refresh_interval = storage_yaml.get(
            'session_refresh_interval', 30)
        session_ttl = timedelta(seconds=(session_refresh_interval * 10))

        self.storage.recover(session_ttl=session_ttl)

        if global_yaml.get('dequeue', True):
            self.daemon_executor.submit(
                partial(self.dequeue, self.daemon_executor))

        self.daemon_executor.submit(
            partial(self.refresh_storage_session,
                    self.daemon_executor,
                    session_refresh_interval, session_ttl))

        if storage_yaml.get('gc_interval', None):
            self.daemon_executor.submit(partial(self.gc, self.daemon_executor))
        else:
            logging.warning('gc disabled')

        # top-level: http host -> endpoint

        self.endpoint_factory = StorageWriterFactory(self)
        self.rest_handler_factory = RestHandlerFactory(
            self.rest_executor,
            endpoint_factory = self.endpoint_factory,
            rest_id_factory = self.rest_id_factory,
            session_uri=listener_yaml.get('session_uri', None),
            service_uri=listener_yaml.get('service_uri', None))

        with self.lock:
            self.started = True
            self.cv.notify_all()

        app = fastapi_service.create_app(self.rest_handler_factory)
        self.hypercorn_shutdown = asyncio.Event()
        try:
            hypercorn_main.run(
                [listener_yaml['addr']],
                listener_yaml.get('cert', None),
                listener_yaml.get('key', None),
                app,
                self.hypercorn_shutdown,
                alive=alive if alive else self.heartbeat)
        except:
            logging.exception('router service main: hypercorn_main exception')
            pass
        logging.debug('router_service.Service.main() hypercorn_main done')
        self.shutdown()
        logging.debug('router_service.Service.main() done')

    def start_main(self):
        self.daemon_executor.submit(
            partial(self.main, alive=self.daemon_executor.ping_watchdog))

    def heartbeat(self):
        for ex in [self.rest_executor, self.output_executor,
                   self.daemon_executor]:
            if not self.daemon_executor.check_watchdog():
                self.hypercorn_shutdown.set()
                return False
        return True

    def rest_id_factory(self):
        return secrets.token_urlsafe(self._rest_id_entropy)

    def create_exploder_output(self, http_host : str, block_upstream
                               ) -> Optional[StorageWriterFilter]:
        if (endp := self.create_storage_writer(http_host, block_upstream)
            ) is None:
            return None
        return endp[0]

    def create_storage_writer(self, http_host : str,
                              block_upstream : bool = True
                              ) -> Optional[Tuple[StorageWriterFilter, dict]]:
        assert http_host is not None
        if (endp := self.filter_chain_factory.build_filter_chain(http_host)
            ) is None:
            return None
        endpoint, endpoint_yaml = endp

        writer = StorageWriterFilter(
            storage=self.storage,
            rest_id_factory=self.rest_id_factory,
            create_leased=True,
            http_host = http_host,
            endpoint_yaml = self.get_endpoint_yaml)
        fut = self.output_executor.submit(
            lambda: self._handle_new_tx(writer, endpoint, endpoint_yaml),
            0)
        if block_upstream and fut is None:
            # XXX leaves db tx leased?
            return None
        return writer, endpoint_yaml

    def get_endpoint_yaml(self, endpoint : str) -> Optional[dict]:
        try:
            return next(e for e in self.root_yaml['endpoint'] if e['name'] == endpoint)
        except StopIteration:
            return None

    def get_storage_writer(self, rest_id : str) -> StorageWriterFilter:
        return StorageWriterFilter(
            storage=self.storage, rest_id=rest_id,
            rest_id_factory=self.rest_id_factory,
            endpoint_yaml = self.get_endpoint_yaml)

    def _handle_new_tx(self, writer : StorageWriterFilter,
                       endpoint : SyncFilter,
                       endpoint_yaml : dict):
        tx_cursor = writer.release_transaction_cursor()
        if tx_cursor is None:
            logging.info('RouterService._handle_new_tx writer %s, '
                         'rest_id is None, downstream error?', writer)
            return
        tx_cursor.load(start_attempt=True)
        logging.debug('RouterService._handle_new_tx %s', tx_cursor.rest_id)
        self.handle_tx(tx_cursor, endpoint, endpoint_yaml)

    def _notification_endpoint(self):
        return StorageWriterFilter(
            self.storage,
            rest_id_factory=self.rest_id_factory,
            create_leased=False)

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
            notification_factory=self._notification_endpoint,
            mailer_daemon_mailbox=self.root_yaml['global'].get(
                'mailer_daemon_mailbox', None),
            retry_params = output_yaml.get('retry_params', {}))
        try:
            handler.handle()
        except Exception as e:
            logging.exception('Service.handle_tx(): OutputHandler.handle')
        finally:
            if storage_tx.in_attempt:
                logging.error(
                    'BUG: OutputHandler.handle() returned with open attempt')

    def _dequeue(self, deq : Optional[List[Optional[bool]]] = None) -> bool:
        storage_tx = self.storage.load_one()
        if deq is not None:
            with self.lock:
                deq[0] = storage_tx is not None
                self.cv.notify_all()

        if storage_tx is None:
            return False

        endpoint, endpoint_yaml = self.filter_chain_factory.build_filter_chain(
            storage_tx.tx.host)
        logging.debug('_dequeue %s %s',
                      storage_tx.id, storage_tx.rest_id)

        self.handle_tx(storage_tx, endpoint, endpoint_yaml)

        return True

    def dequeue(self, executor):
        while True:
            executor.ping_watchdog()
            deq = [None]
            # fine to wait forever on this submit()
            if (self.output_executor.submit(partial(self._dequeue, deq))
                is None):
                logging.error('unexpected executor overflow')
                if self.wait_shutdown(1, executor):
                    return
                continue
            with self.lock:
                # Wait 1s for _dequeue()
                self.cv.wait_for(
                    lambda: (deq[0] is not None) or self._shutdown, 1)
            # if we dequeued something, try again immediately in case
            # there's another
            if deq[0]:
                continue
            if self.wait_shutdown(1, executor):
                return

    def gc(self, executor):
        storage_yaml = self.root_yaml['storage']
        ttl = timedelta(seconds=storage_yaml.get('gc_ttl', 86400))
        interval = storage_yaml.get('gc_interval', 300)
        while True:
            executor.ping_watchdog()
            self._gc(ttl)
            if self.wait_shutdown(interval, executor):
                return

    def _refresh(self, ref : List[bool], session_ttl : timedelta):
        if self.storage._refresh_session():
            with self.lock:
                ref[0] = True
                self.cv.notify_all()
        self.storage._gc_session(session_ttl)

    def refresh_storage_session(self, executor, interval : int,
                                session_ttl : timedelta):
        last_refresh = time.monotonic()
        # start _refresh() on daemon_executor every interval
        # wait up to interval for it to succeed
        # if 5 consecutive failures/timeouts, abort program
        # another instance can gc our stale session after 10*interval
        while True:
            start = time.monotonic()
            delta = start - last_refresh
            if delta > (5 * interval):
                logging.error('stale storage session')
                self.shutdown()
                return
            executor.ping_watchdog()
            ref = [False]

            if self.daemon_executor.submit(
                    partial(self._refresh, ref, session_ttl)) is None:
                if self.wait_shutdown(1, executor):
                    return
                continue
            with self.lock:
                self.cv.wait_for(lambda: ref[0] or self._shutdown, interval)
                if ref[0]:
                    last_refresh = time.monotonic()
            if self.wait_shutdown(interval - (time.monotonic() - start),
                                  executor):
                return

    def _gc(self, gc_ttl : timedelta):
        logging.info('router_service _gc %s', gc_ttl)
        count = self.storage.gc(gc_ttl)
        logging.info('router_service _gc deleted %d tx %d blobs',
                     count[0], count[1])
        return count
