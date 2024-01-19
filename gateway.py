import sys

from rest_endpoint import RestEndpoint, BlobIdMap
from smtp_endpoint import Factory as SmtpFactory, SmtpEndpoint

from executor import Executor
from tags import Tag

from blobs import BlobStorage
from smtp_service import service as smtp_service
import rest_service
from rest_endpoint_adapter import RestEndpointAdapterFactory, EndpointFactory
import gunicorn_main

import logging

from typing import Dict

from threading import Thread

import time

from config import Config

from wsgiref.simple_server import make_server

class SmtpGateway(EndpointFactory):
    inflight : Dict[str, SmtpEndpoint]
    config = None
    shutdown_gc = False

    def __init__(self, config):
        self.config = config

        self.blob_id_map = BlobIdMap()

        self.router_base_url = config.root_yaml['rest_output']['endpoint']

        self.blobs = BlobStorage()
        self.smtp_factory = SmtpFactory()

        self.inflight = {}

        self.gc_thread = Thread(target = lambda: self.gc_inflight(),
                                daemon=True)
        self.gc_thread.start()

    def shutdown(self):
        if self.gc_thread:
            self.shutdown_gc = True
            self.gc_thread.join()
            self.gc_thread = None

    def mx_rest_factory(self):
        return RestEndpoint(self.router_base_url, http_host='inbound-gw',
                            blob_id_map=self.blob_id_map)

    def msa_rest_factory(self):
        return RestEndpoint(self.router_base_url, http_host='outbound-gw')

    # EndpointFactory
    def create(self, host):
        if host == 'outbound':
            endpoint = self.smtp_factory.new(
                ehlo_hostname=self.config.root_yaml['smtp_output']['ehlo_host'])
            self.inflight[endpoint.rest_id] = endpoint
            return endpoint
        else:
            return None

    # EndpointFactory
    def get(self, rest_id):
        return self.inflight.get(rest_id, None)

    def gc_inflight(self):
        last_gc = 0
        while not self.shutdown_gc:
            now = time.monotonic()
            delta = now - last_gc
            tx_idle_gc = self.config.root_yaml['rest_listener'].get('tx_idle_gc', 5)
            if delta < tx_idle_gc:
                time.sleep(tx_idle_gc - delta)
            last_gc = now
            dele = []
            for (rest_id, tx) in self.inflight.items():
                tx_idle = now - tx.idle_start if tx.idle_start else 0
                # xxx doesn't work, pre-yaml api
                if tx_idle > self.config.get_int('tx_idle_timeout', 5):
                    logging.info('SmtpGateway.gc_inflight shutdown idle %s',
                                 tx.rest_id)
                    tx._shutdown()
                    dele.append(rest_id)
            for d in dele:
                logging.info(d)
                del self.inflight[d]

    def main(self):
        for service_yaml in self.config.root_yaml['smtp_listener']['services']:
            factory = None
            msa = False
            if service_yaml['type'] == 'mx':
                factory = lambda: self.mx_rest_factory()
            elif service_yaml['type'] == 'msa':
                factory = lambda: self.msa_rest_factory()
                msa = True

            # cf router config.Config.exploder()
            rcpt_timeout=40
            data_timeout=310
            if msa:
                rcpt_timeout=15
                data_timeout=40

            addr = service_yaml['addr']
            smtp_service(
                factory, hostname=addr[0], port=addr[1],
                cert=service_yaml.get('cert', None),
                key=service_yaml.get('key', None),
                msa=msa,
                auth_secrets_path=service_yaml.get('auth_secrets', None),
                rcpt_timeout=service_yaml.get('rcpt_timeout', rcpt_timeout),
                data_timeout=service_yaml.get('data_timeout', data_timeout))

        self.adapter_factory = RestEndpointAdapterFactory(self, self.blobs)

        flask_app=rest_service.create_app(self.adapter_factory)
        if self.config.root_yaml['rest_listener']['use_gunicorn']:
            gunicorn_main.run(
                [self.config.root_yaml['rest_listener']['addr']],
                cert=None, key=None,
                app=flask_app)
        else:
            # xxx doesn't work, pre-yaml api
            self.wsgi_server = make_server('localhost',
                                           self.config.get_int('rest_port'),
                                           flask_app)
            self.wsgi_server.serve_forever()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')
    config = Config()
    config.load_yaml(sys.argv[1])
    gw = SmtpGateway(config)
    gw.main()
