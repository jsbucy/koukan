from typing import Dict, Tuple, Any, Optional
import time
import logging
from threading import Lock, Condition, Thread
import json
import os

from wsgiref.simple_server import make_server

import rest_service
import gunicorn_main

from blobs import BlobStorage
from blob import InlineBlob

from storage import Storage, TransactionCursor
from transaction import RestServiceTransactionFactory, cursor_to_endpoint
from response import Response
from tags import Tag
from executor import Executor
from config import Config

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

    def __init__(self, config=None):
        self.lock = Lock()
        self.cv = Condition(self.lock)

        self.executor = Executor(10, {
            Tag.LOAD: 1,
            Tag.MX: 3,
            Tag.MSA : 5,
            Tag.DATA: 10 })

        self.blobs = None

        self.config = config

    def shutdown(self):
        logging.info("router_service shutdown()")
        self._shutdown = True
        if self.dequeue_thread is not None:
            self.dequeue_thread.join()
        if self.gc_thread is not None:
            self.gc_thread.join()

        assert(self.executor.wait_empty(timeout=5))

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

        if self.config.root_yaml['global'].get('dequeue', True):
            self.dequeue_thread = Thread(target = lambda: self.dequeue(),
                                         daemon=True)
            self.dequeue_thread.start()

        if self.config.root_yaml['global'].get('gc_interval', None):
            self.gc_thread = Thread(target = lambda: self.gc(),
                                    daemon=True)
            self.gc_thread.start()
        else:
            logging.warning('idle gc disabled')

        # top-level: http host -> endpoint

        handler_factory = None
        self.rest_tx_factory = RestServiceTransactionFactory(self.storage)
        handler_factory = self.rest_tx_factory

        with self.lock:
            self.started = True
            self.cv.notify_all()

        flask_app = rest_service.create_app(handler_factory)
        listener_yaml = self.config.root_yaml['rest_listener']
        if listener_yaml.get('use_gunicorn', False):
            # XXX gunicorn always forks, need to get all our startup
            # code into the worker e.g. post_worker_init() hook. May
            # be possible to run gunicorn.workers.ThreadWorker
            # directly?
            # Alternatively, hypercorn appears to support configuring
            # num_workers=0 and running in the same process.
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


    def handle_tx(self, storage_tx : TransactionCursor, endpoint : object):
        cursor_to_endpoint(storage_tx, endpoint)

    def _dequeue(self, wait : bool = True) -> bool:
        storage_tx = None
        # xxx broken needs to loop?
        self.storage.wait_created(self.created_id, timeout=1 if wait else 0)
        storage_tx = self.storage.load_one()
        logging.info("dequeued %s", storage_tx.id if storage_tx else None)
        if storage_tx is None:
            return False

        if self.created_id is None or (
                storage_tx.id > self.created_id):
            self.created_id = storage_tx.id

        endpoint, msa = self.config.get_endpoint(storage_tx.tx.host)
        logging.info('_dequeue %s %s', endpoint, msa)
        tag = Tag.MSA if msa else Tag.MX
        self.executor.enqueue(
            tag, lambda: self.handle_tx(storage_tx, endpoint))
        return True

    def dequeue(self):
        prev = True
        while not self._shutdown:
            logging.info("RouterService.dequeue")
            prev = self._dequeue(wait=(not prev))

    def gc(self):
        while not self._shutdown:
            self._gc_inflight(
                self.config.root_yaml['global'].get('tx_idle_timeout', 5))
            # xxx wait for shutdown
            time.sleep(self.config.root_yaml['global'].get('gc_interval', 5))

    def _gc_inflight(self, idle_timeout=None):
        now = time.monotonic()
        logging.info('router_service _gc_inflight %d', idle_timeout)

        count = self.storage.gc_non_durable(idle_timeout)
        logging.info('router_service _gc_inflight aborted %d', count)
        return count

