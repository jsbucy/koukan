from typing import Dict, Tuple, Any, Optional
import time
import logging
from threading import Lock, Condition, Thread
import json

from wsgiref.simple_server import make_server

import rest_service
import gunicorn_main

from blobs import BlobStorage
from blob import InlineBlob
from rest_endpoint import BlobIdMap as RestBlobIdMap

from storage import Storage, Action, Status, TransactionCursor
from transaction import RestServiceTransactionFactory, cursor_to_endpoint
from response import Response
from tags import Tag
from executor import Executor
from config import Config

class Service:
    lock : Lock = None
    last_gc = 0

    rest_tx_factory : RestServiceTransactionFactory = None

    # dequeue watermark
    created_id : Optional[int] = None

    dequeue_thread : Optional[Thread] = None
    gc_thread : Optional[Thread] = None

    _shutdown = False

    config : Optional[Config] = None

    def __init__(self, config=None):
        self.lock = Lock()

        self.executor = Executor(10, {
            Tag.LOAD: 1,
            Tag.MX: 3,
            Tag.MSA : 5,
            Tag.DATA: 10 })

        self.storage = Storage()
        self.blobs = None

        self.config = config
        
        self.rest_blob_id_map = RestBlobIdMap()

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

    def main(self, config_filename=None):
        if config_filename:
            config = Config(rest_blob_id_map=self.rest_blob_id_map)
            config.load_yaml(config_filename)
            self.config = config

        db_filename = self.config.root_yaml['storage'].get(
            'db_filename', None)
        if not db_filename:
            logging.warning("*** using in-memory/non-durable storage")
            self.storage.connect(db=Storage.get_inmemory_for_test())
        else:
            self.storage.connect(filename=db_filename)


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

        flask_app = rest_service.create_app(handler_factory)
        listener_yaml = self.config.root_yaml['rest_listener']
        if listener_yaml.get('use_gunicorn', False):
            # XXX gunicorn always forks, need to get all our startup code
            # into the worker e.g. post_worker_init() hook
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
        self.storage.wait_created(self.created_id, timeout=1 if wait else 0)
        storage_tx = self.storage.load_one()
        if storage_tx is not None:
            if self.created_id is None or (
                    storage_tx.id > self.created_id):
                self.created_id = storage_tx.id
        else:
            return False

        #xxxtag = Tag.LOAD

        logging.info("dequeued %d", storage_tx.id)
        # XXX there is a race that this can be selected
        # between creation and writing the envelope
        storage_tx.wait_attr_not_none('host')
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

