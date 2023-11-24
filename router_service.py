
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

from router_transaction import RouterTransaction, BlobIdMap
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

class RouterTransactionFactory(EndpointFactory):
    def __init__(self, parent):
        self.parent = parent
    def create(self, host):
        return self.parent.get_router_transaction(host)
    def get(self, rest_id):
        if rest_id in self.parent.inflight:
            return self.parent.inflight[rest_id]
        shim = ShimTransaction(self.parent)
        if not shim.read(rest_id):
            logging.info('RouterTransactionFactory.get shim read failed')
            return None
        return shim

# stand-in for RouterTransaction for rest reads of at-rest/stored transactions
class ShimTransaction:
    received_last = True  # XXX
    rest_id = None
    done = False

    def __init__(self, parent):
        self.parent = parent
        self.final_status = None

    def get_start_result(self):
        return None

    def get_final_status(self, timeout=None):
        return self.final_status

    def read(self, rest_id):
        logging.info('ShimTransaction.read %s', rest_id)
        self.rest_id = rest_id
        storage_tx = self.parent.storage.get_transaction_cursor()
        if not storage_tx.load(rest_id=rest_id):
            return False
        status = storage_tx.status
        if status != Status.DONE and status != Status.ONESHOT_DONE:
            return True

        actions = storage_tx.load_last_action(1)
        if len(actions) != 1:
            # more like INTERNAL, integrity problem if there aren't any
            return True
        time, action, response = actions[0]

        # The semantics of final_status is that it's present iff we
        # are done with it. async (msa/multi-mx) can only be DELIVERED
        # or PERM_FAIL, sync mx can be TEMP_FAIL. PERM_FAIL
        # ~corresponds to whether we would send a bounce

        if (action == Action.TEMP_FAIL and
            status != Status.ONESHOT_DONE):
            pass
        else:
            self.final_status = response

        logging.info('ShimTransaction.read %s status=%d action=%d', rest_id, status, action)
        if status == Status.DONE:
            self.done = True
        elif status == Status.ONESHOT_DONE and action != Action.TEMP_FAIL:
            self.done = True

        return True

    def set_durable(self):
        logging.info('ShimTransaction.set_durable %s done=%s',
                     self.rest_id, self.done)
        if self.done:
            return Response()  # no op
        return Response(500, 'failed precondition')

class Service:
    lock : Lock = None
    inflight : Dict[str, RouterTransaction] = None
    last_gc = 0
    stop_dequeue = False

    rest_tx_factory : RestServiceTransactionFactory = None

    def __init__(self, config=None):
        self.lock = Lock()
        self.inflight = {}

        self.executor = Executor(10, {
            Tag.LOAD: 1,
            Tag.MX: 3,
            Tag.MSA : 5,
            Tag.DATA: 10 })

        self.blob_id_map = BlobIdMap()
        self.storage = Storage()
        self.blobs = None

        self.config = config
        
        self.wiring = None

        self.rest_blob_id_map = RestBlobIdMap()

        self.endpoint_factory = RouterTransactionFactory(self)

    def shutdown(self):
        logging.info("router_service shutdown()")
        self.stop_dequeue = True
        self.dequeue_thread.join()

        with self.lock:
            txx = [tx for id,tx in self.inflight.items()]
            for tx in txx:
                tx.abort()
                tx.finalize()

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
            print("*** using in-memory/non-durable storage")
            self.storage.connect(db=Storage.get_inmemory_for_test())
        else:
            self.storage.connect(filename=db_filename)


        self.blobs = BlobStorage()

        self.dequeue_thread = Thread(target = lambda: self.load(),
                                     daemon=True)
        self.dequeue_thread.start()

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

    def get_router_transaction(self, host, storage_tx=None
                               ) -> Optional[RouterTransaction]:
        logging.info('get_router_transaction %s', host)
        e = self.get_endpoint(host)
        if e is None: return None
        endpoint, tag, msa = e
        tx = RouterTransaction(
            self.executor, self.storage, self.blob_id_map, self.blobs,
            endpoint, host, msa, tag, storage_tx)
        if storage_tx is None:
            tx.generate_rest_id(lambda: self.done(tx))
        # xxx ttl/gc of these to be provided by rest service?
        self._add_inflight(tx)

        return tx

    def done(self, tx):
        logging.info('done %s', tx.rest_id)
        self._del_inflight(tx)
        tx.finalize()

    # -> Endpoint, Tag, is-msa
    def get_endpoint(self, host) -> Optional[Tuple[Any, int, bool]]:
        endpoint, msa = self.wiring.get_endpoint(host)
        if not endpoint: return None
        tag = Tag.MSA if msa else Tag.MX
        return endpoint, tag, msa

    def handle(self, storage_tx):
        transaction = self.get_router_transaction(storage_tx.host, storage_tx)
        transaction.load()
        self._del_inflight(transaction)
        transaction.finalize()

    def handle_tx(self, storage_tx : TransactionCursor, endpoint : object):
        cursor_to_endpoint(storage_tx, endpoint)

    def load(self):
        created_id = None
        while not self.stop_dequeue:
            logging.info("dequeue %s", self.stop_dequeue)
            storage_tx = None
            if self.rest_tx_factory is not None:
                if self.storage.wait_created(created_id, timeout=1):
                    storage_tx = self.storage.load_one()
                if (storage_tx is not None and
                    storage_tx.status == Status.INSERT):  # XXX status??
                    created_id = storage_tx.id

            self._gc_inflight()

            tag = None
            # XXX config, backoff
            # XXX there is a race between RouterTransaction updating
            # the storage (which makes it selectable here) vs running
            # the done callback, if min_age is too low, this can clash
            # with inflight
            if storage_tx is None:
                # XXX this will load INSERT but we probably only want WAITING?
                storage_tx = self.storage.load_one(min_age=5)
                tag = Tag.LOAD
            if storage_tx:
                logging.info("dequeued %d", storage_tx.id)
                if self.rest_tx_factory:
                    endpoint, msa = self.wiring.get_endpoint(storage_tx.host)
                    assert(endpoint is not None)
                    tag = Tag.MSA if msa else Tag.MX
                    self.executor.enqueue(
                        tag, lambda: self.handle_tx(storage_tx, endpoint))
                else:
                    self.executor.enqueue(tag, lambda: self.handle(storage_tx))
            else:
                logging.info("dequeue idle")
                # xxx config
                time.sleep(1)

    def _add_inflight(self, tx):
        assert(tx.rest_id)
        with self.lock:
            assert(tx.rest_id not in self.inflight)
            self.inflight[tx.rest_id] = tx

    def _del_inflight(self, tx):
        assert(tx.rest_id)
        with self.lock:
            del self.inflight[tx.rest_id]

    def _gc_inflight(self):
        now = time.monotonic()
        if (now - self.last_gc) < self.config.get_int('tx_idle_gc', 5): return
        logging.info('router_service _gc_inflight')
        self.last_gc = now

        idle_timeout = self.config.get_int('tx_idle_timeout', 5)
        if self.rest_tx_factory:
            self.storage.gc_non_durable(idle_timeout)
        with self.lock:
            dele = []
            for rest_id, tx in self.inflight.items():
                aborted = tx.abort_if_idle(idle_timeout, now)
                if aborted:
                    dele.append(tx)
        for tx in dele:
            # recovered (which are run sync in handle) aren't eligible
            # for idle gc
            # rest-initiated have done_cb = _del_inflight()
            tx.finalize()
            self._del_inflight(tx)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')

    service = Service()

    from pysmtpgw_config import Config as Wiring
    wiring=Wiring()
    service.main(wiring)
