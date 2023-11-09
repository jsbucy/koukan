import sys

from rest_endpoint import RestEndpoint, BlobIdMap
from smtp_endpoint import Factory as SmtpFactory, SmtpEndpoint

from executor import Executor
from tags import Tag

from blobs import BlobStorage
from smtp_service import service as smtp_service
import rest_service
import gunicorn_main

import logging

from typing import Dict

from threading import Thread

import time

from config import Config

from wsgiref.simple_server import make_server


class SmtpGateway:
    inflight : Dict[str, SmtpEndpoint]
    config = None
    shutdown_gc = False

    def __init__(self, config):
        self.config = config
        self.rest_port = config.get_int('rest_port')
        self.router_port = config.get_int('router_port')
        self.mx_port = config.get_int('mx_port')
        self.msa_port = config.get_int('msa_port')
        self.cert = config.get_str('cert')
        self.key = config.get_str('key')
        self.auth_secrets = config.get_str('auth_secrets')
        self.ehlo_host = config.get_str('ehlo_host')
        self.listen_host = config.get_str('listen_host')

        self.blob_id_map = BlobIdMap()

        self.router_base_url = 'http://localhost:%d/' % self.router_port

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
        return RestEndpoint(self.router_base_url, http_host='outbound-gw',
                            msa=True)

    # xxx EndpointFactory abc
    def create(self, host):
        if host == 'outbound':
            endpoint = self.smtp_factory.new(
                ehlo_hostname=self.ehlo_host)
            self.inflight[endpoint.rest_id] = endpoint
            return endpoint, False # msa
        else:
            return None

    def get(self, rest_id):
        return self.inflight.get(rest_id, None)

    def gc_inflight(self):
        last_gc = 0
        while not self.shutdown_gc:
            now = time.monotonic()
            delta = now - last_gc
            tx_idle_gc = self.config.get_int('tx_idle_gc', 5)
            if delta < tx_idle_gc:
                time.sleep(tx_idle_gc - delta)
            last_gc = now
            dele = []
            for (rest_id, tx) in self.inflight.items():
                tx_idle = now - tx.idle_start if tx.idle_start else 0
                if tx_idle > self.config.get_int('tx_idle_timeout', 5):
                    logging.info('SmtpGateway.gc_inflight shutdown idle %s',
                                 tx.rest_id)
                    tx._shutdown()
                    dele.append(rest_id)
            for d in dele:
                logging.info(d)
                del self.inflight[d]

    def main(self):
        smtp_service(lambda: self.mx_rest_factory(), port=self.mx_port,
                     cert=self.cert, key=self.key,
                     msa=False, hostname=self.listen_host)

        smtp_service(lambda: self.msa_rest_factory(),
                     port=self.msa_port,
                     cert=self.cert, key=self.key,
                     msa=True,
                     auth_secrets_path=self.auth_secrets,
                     hostname=self.listen_host)

        flask_app=rest_service.create_app(
            self,  # xxx EndpointFactory abc
            self.blobs)
        if self.config.get_bool('use_gunicorn'):
            gunicorn_main.run(
                'localhost', self.rest_port, cert=None, key=None,
                app=flask_app)
        else:
            self.wsgi_server = make_server('localhost',
                                           self.config.get_int('rest_port'),
                                           flask_app)
            self.wsgi_server.serve_forever()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')
    config = Config(filename=sys.argv[1])
    gw = SmtpGateway(config)
    gw.main()
