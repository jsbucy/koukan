
from typing import Dict, Tuple, Any, Optional

import rest_service
from rest_endpoint_adapter import RestEndpointAdapterFactory, EndpointFactory
import gunicorn_main

from address_policy import AddressPolicy, PrefixAddr
from blobs import BlobStorage
from blob import InlineBlob
from local_domain_policy import LocalDomainPolicy
from dest_domain_policy import DestDomainPolicy
from router import Router
from rest_endpoint import RestEndpoint, BlobIdMap as RestBlobIdMap
from dkim_endpoint import DkimEndpoint

from storage import Storage, Action, Status, TransactionCursor

from transaction import RestServiceTransactionFactory, cursor_to_endpoint

from response import Response

from tags import Tag

import time
import logging

from threading import Lock, Condition, Thread

from mx_resolution_endpoint import MxResolutionEndpoint

import sys
from executor import Executor

import json

from wsgiref.simple_server import make_server

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
        
        self.wiring = None

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

    def main(self, wiring):
        if self.config is None:
            self.config = Config(filename=sys.argv[1])

        self.wiring = wiring
        wiring.setup(self.config,
                     rest_blob_id_map=self.rest_blob_id_map)

        db_filename = self.config.get_str('db_filename')
        if not db_filename:
            logging.warning("*** using in-memory/non-durable storage")
            self.storage.connect(db=Storage.get_inmemory_for_test())
        else:
            self.storage.connect(filename=db_filename)


        self.blobs = BlobStorage()

        if self.config.get_bool('dequeue', True):
            self.dequeue_thread = Thread(target = lambda: self.dequeue(),
                                         daemon=True)
            self.dequeue_thread.start()

        if self.config.get_int('gc_interval') is not None:
            self.gc_thread = Thread(target = lambda: self.gc(),
                                    daemon=True)
            self.gc_thread.start()
        else:
            logging.warning('idle gc disabled')


        # top-level: http host -> endpoint

        handler_factory = None
        if False:
            self.adapter_factory = RestEndpointAdapterFactory(
                self.endpoint_factory, self.blobs)
            handler_factory = self.adapter_factory
        else:
            self.rest_tx_factory = RestServiceTransactionFactory(self.storage)
            handler_factory = self.rest_tx_factory

        flask_app = rest_service.create_app(handler_factory)
        if self.config.get_bool('use_gunicorn'):
            gunicorn_main.run(
                'localhost', self.config.get_int('rest_port'),
                self.config.get_str('cert'),
                self.config.get_str('key'),
                flask_app)
        else:
            self.wsgi_server = make_server('localhost',
                                           self.config.get_int('rest_port'),
                                           flask_app)
            self.wsgi_server.serve_forever()


    # -> Endpoint, Tag, is-msa
    def get_endpoint(self, host) -> Optional[Tuple[Any, int, bool]]:
        endpoint, msa = self.wiring.get_endpoint(host)
        if not endpoint: return None
        tag = Tag.MSA if msa else Tag.MX
        return endpoint, tag, msa

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
        endpoint, msa = self.wiring.get_endpoint(storage_tx.host)
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
            self._gc_inflight(self.config.get_int('tx_idle_timeout', 5))
            # xxx wait for shutdown
            time.sleep(self.config.get_int('gc_interval'))

    def _gc_inflight(self, idle_timeout=None):
        now = time.monotonic()
        logging.info('router_service _gc_inflight %d', idle_timeout)

        count = self.storage.gc_non_durable(idle_timeout)
        logging.info('router_service _gc_inflight aborted %d', count)
        return count

