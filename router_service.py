
from typing import Dict, Tuple, Any

import rest_service
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
from storage import Storage, Action

from tags import Tag

import time
import logging

from threading import Lock, Condition, Thread

from mx_resolution_endpoint import MxResolutionEndpoint

import sys
from executor import Executor

import json

from pysmtpgw_config import Config as Wiring

class Config:
    def __init__(self, filename=None):
        js = {}
        if filename:
            config_json_file = open(filename, "r")
            self.js = json.load(config_json_file)

    def get_str(self, k):
        return self.js.get(k, None)
    def get_int(self, k):
        return int(self.js[k]) if k in self.js else None


class Service:
    def __init__(self):
        self.executor = Executor(10, {
            Tag.LOAD: 1,
            Tag.MX: 3,
            Tag.MSA : 5,
            Tag.DATA: 10 })

        self.blob_id_map = BlobIdMap()
        self.storage = Storage()
        self.blobs = None

        self.config = None
        self.wiring = None

        self.rest_blob_id_map = RestBlobIdMap()

    def main(self):
        self.config = Config(filename=sys.argv[1])

        self.wiring = Wiring(self.config,
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

        gunicorn_main.run(
            'localhost', self.config.get_int('rest_port'),
            self.config.get_str('cert'),
            self.config.get_str('key'),
            rest_service.create_app(
                lambda host: self.get_router_transaction(host),
                self.executor, self.blobs))

    def get_router_transaction(self, host):
        next, tag, msa = self.get_transaction(host)
        return RouterTransaction(
            self.executor, self.storage, self.blob_id_map, self.blobs,
            next, host, msa, tag), msa

    # -> Transaction, Tag, is-msa
    def get_transaction(self, host):
        endpoint, msa = self.wiring.get_endpoint(host)
        if not endpoint: return None, None, None
        tag = Tag.MSA if msa else Tag.MX
        return endpoint, tag, msa

    def handle(self, storage_tx):
        # TODO need to wire this into rest service resources
        transaction, msa = self.get_router_transaction(storage_tx.host)
        transaction.load(storage_tx)

    def load(self):
        while True:
            logging.info("dequeue")
            storage_tx = self.storage.load_one(min_age=10)
            if storage_tx:
                logging.info("dequeued %d", storage_tx.id)
                self.executor.enqueue(Tag.LOAD, lambda: self.handle(storage_tx))
            else:
                logging.info("dequeue idle")
                time.sleep(10)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')


    service = Service()
    service.main()
