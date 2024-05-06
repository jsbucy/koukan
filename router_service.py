from typing import Dict, Tuple, Any, Optional
import time
import logging
from threading import Lock, Condition, Thread
import json
import os

from wsgiref.simple_server import make_server

import rest_service
import gunicorn_main
import hypercorn_main

from blobs import BlobStorage
from blob import InlineBlob

from storage import Storage, TransactionCursor
from transaction import RestServiceTransactionFactory
from output_handler import OutputHandler
from response import Response
from executor import Executor
from config import Config
from filter import Filter

class Service:
    lock : Lock
    cv : Condition
    storage : Optional[Storage] = None
    last_gc = 0

    rest_tx_factory : RestServiceTransactionFactory = None

    # dequeue watermark
    created_id : Optional[int] = None

    dequeue_thread : Optional[Thread] = None
    gc_thread : Optional[Thread] = None

    _shutdown = False

    config : Optional[Config] = None

    started = False
    executor : Optional[Executor] = None
    owned_executor : Optional[Executor] = None

    def __init__(self, config=None,
                 executor : Optional[Executor] = None):
        self.lock = Lock()
        self.cv = Condition(self.lock)

        self.blobs = None

        self.config = config
        self.executor = executor

    def shutdown(self):
        logging.info("router_service shutdown()")
        self._shutdown = True
        if self.dequeue_thread is not None:
            self.dequeue_thread.join()
        if self.gc_thread is not None:
            self.gc_thread.join()

        if self.owned_executor is not None:
            assert(self.owned_executor.shutdown(timeout=5))

        if self.wsgi_server:
            self.wsgi_server.shutdown()

    def wait_started(self, timeout=None):
        with self.lock:
            return self.cv.wait_for(lambda: self.started, timeout)

    def main(self, config_filename=None):
        if config_filename:
            config = Config()
            config.load_yaml(config_filename)
            self.config = config

        global_yaml = self.config.root_yaml.get('global', {})

        if self.executor is None:
            executor_yaml = global_yaml.get('executor', {})
            self.owned_executor = Executor(
                executor_yaml.get('max_inflight', 10),
                executor_yaml.get('watchdog_timeout', 30))
            self.executor = self.owned_executor

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

        self.blobs = BlobStorage()

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

        handler_factory = None
        self.rest_tx_factory = RestServiceTransactionFactory(
            self.storage, self.config.rest_id_factory())
        handler_factory = self.rest_tx_factory

        with self.lock:
            self.started = True
            self.cv.notify_all()

        flask_app = rest_service.create_app(handler_factory)
        listener_yaml = self.config.root_yaml['rest_listener']
        if listener_yaml.get('use_hypercorn', False):
            hypercorn_main.run(
                [listener_yaml['addr']],
                listener_yaml.get('cert', None),
                listener_yaml.get('key', None),
                flask_app)
        elif listener_yaml.get('use_gunicorn', False):
            # XXX gunicorn always forks, need to get all our startup
            # code into the worker e.g. post_worker_init() hook. May
            # be possible to run gunicorn.workers.ThreadWorker
            # directly?
            gunicorn_main.run(
                [listener_yaml['addr']],
                listener_yaml.get('cert', None),
                listener_yaml.get('key', None),
                flask_app)
        else:
            self.wsgi_server = make_server(
                listener_yaml['addr'][0],
                listener_yaml['addr'][1],
                flask_app)
            self.wsgi_server.serve_forever()


    def handle_tx(self, storage_tx : TransactionCursor,
                  endpoint : Filter,
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
        handler.cursor_to_endpoint()
        # TODO wrap all of this in try...finally cursor.finalize_attempt()?

    def _dequeue(self, wait : bool = True) -> bool:
        storage_tx = None
        self.storage.wait_created(self.created_id, timeout=1 if wait else 0)
        storage_tx = self.storage.load_one()
        logging.info("dequeued %s", storage_tx.id if storage_tx else None)
        if storage_tx is None:
            return False

        if self.created_id is None or (
                storage_tx.id > self.created_id):
            self.created_id = storage_tx.id

        endpoint, endpoint_yaml = self.config.get_endpoint(storage_tx.tx.host)
        msa = endpoint_yaml['msa']
        logging.info('_dequeue %s %s', endpoint, endpoint_yaml)
        self.executor.submit(
            lambda: self.handle_tx(storage_tx, endpoint, endpoint_yaml))

        # TODO wrap all of this in try...finally cursor.finalize_attempt()?
        return True

    def dequeue(self):
        prev = True
        while not self._shutdown:
            logging.info("RouterService.dequeue")
            prev = self._dequeue(wait=(not prev))

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
