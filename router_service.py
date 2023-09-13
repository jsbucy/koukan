
from typing import Dict

import rest_service
import gunicorn_main

from address_policy import AddressPolicy, PrefixAddr
from blobs import BlobStorage
from blob import InlineBlob
from local_domain_policy import LocalDomainPolicy
from dest_domain_policy import DestDomainPolicy
from router import Router
from rest_endpoint import RestEndpoint
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

class Service:
    def __init__(self):
        self.executor = Executor(10, {
            Tag.LOAD: 1,
            Tag.MX: 3,
            Tag.MSA : 5,
            Tag.DATA: 10 })

        self.blob_id_map = BlobIdMap()
        self.storage = Storage()

    def main(self):
        rest_port = int(sys.argv[1])
        gateway_port = int(sys.argv[2])
        cert = sys.argv[3]
        key = sys.argv[4]
        dkim_key = sys.argv[5]
        dkim_domain = sys.argv[6].encode('ascii')
        dkim_selector = sys.argv[7].encode('ascii')
        db_filename = sys.argv[8]

        if not db_filename:
            print("*** using in-memory/non-durable storage")
            self.storage.connect(db=Storage.get_inmemory_for_test())
        else:
            self.storage.connect(filename=db_filename)

        gw_base_url = 'http://localhost:%d/' % gateway_port


        self.blobs = BlobStorage()


        ### inbound

        # AddressPolicy passes the rcpt to the endpoint factory
        outbound_host = lambda _: RestEndpoint(
            gw_base_url, http_host='outbound',
            static_remote_host=('127.0.0.1', 3025),
            sync=True)  #'aspmx.l.google.com')

        local_addrs = AddressPolicy(
            [ PrefixAddr('u', delimiter='+', endpoint_factory=outbound_host),
              PrefixAddr('v', delimiter='+', endpoint_factory=outbound_host),
             ])
        local_addr_router = lambda: Router(local_addrs)
        local_domains = LocalDomainPolicy({'d': local_addr_router})
        self.local_domain_router = lambda: Router(local_domains)

        ### outbound
        outbound_mx = lambda: RestEndpoint(
            gw_base_url, http_host='outbound',
            static_remote_host=('127.0.0.1', 3025))
        mx_resolution = lambda: MxResolutionEndpoint(outbound_mx)
        next = mx_resolution

        if dkim_key:
            print('enabled dkim signing', dkim_key)
            next = lambda: DkimEndpoint(dkim_domain, dkim_selector, dkim_key,
                                        mx_resolution())

        dest_domain_policy = DestDomainPolicy(next)
        self.dest_domain_router = lambda: Router(dest_domain_policy)

        self.dequeue_thread = Thread(target = lambda: self.load(),
                                     daemon=True)
        self.dequeue_thread.start()

        # top-level: http host -> endpoint

        gunicorn_main.run(
            'localhost', rest_port, cert, key,
            rest_service.create_app(
                lambda host: self.get_router_transaction(host),
                self.executor, self.blobs))

    def get_router_transaction(self, host):
        next, tag, msa = self.get_transaction(host)
        return RouterTransaction(
            self.storage, self.blob_id_map, self.blobs, next, host, msa), tag, msa

    # -> Transaction, Tag, is-msa
    def get_transaction(self, host):
        if host == 'inbound-gw':
            return self.local_domain_router(), Tag.MX, False
        elif host == 'outbound-gw':
            return self.dest_domain_router(), Tag.MSA, True
        return None

    MAX_RETRY = 3 * 86400
    def handle(self, reader):
        # TODO need to wire this into rest service resources
        # TODO move most/all of this into RouterTransaction?
        transaction,tag,msa = self.get_transaction(reader.host)
        print("handle", reader.id, reader.mail_from, reader.rcpt_to)
        resp = transaction.start(reader.local_host, reader.remote_host,
                                 reader.mail_from, reader.transaction_esmtp,
                                 reader.rcpt_to, reader.rcpt_esmtp)
        print("handle start resp", reader.id, resp)
        if resp.ok():
            while d := reader.read_content():
                resp = transaction.append_data(last=False, blob=InlineBlob(d))
                if resp.err(): break
            if not resp.err():
                resp = transaction.append_data(last=True, blob=InlineBlob(b''))

        action = None
        if resp.ok():
            action = Action.DELIVERED
        elif resp.perm() or (time.time() - reader.creation) > self.MAX_RETRY:
            # permfail/bounce
            action = Action.PERM_FAIL
        else:
            action = Action.TEMP_FAIL

        self.storage.append_transaction_actions(reader.id, action)

    def load(self):
        while True:
            print("dequeue")
            reader = self.storage.load_one()
            if reader:
                self.executor.enqueue(Tag.LOAD, lambda: self.handle(reader))
            else:
                print("dequeue idle")
                time.sleep(10)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')


    service = Service()
    service.main()
