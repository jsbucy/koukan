from typing import Dict
import sys
import logging
from threading import Thread
import time

from rest_endpoint import RestEndpoint
from smtp_endpoint import Factory as SmtpFactory, SmtpEndpoint
from blobs import BlobStorage
from smtp_service import service as smtp_service
import rest_service
from rest_endpoint_adapter import RestEndpointAdapterFactory, EndpointFactory
import gunicorn_main
from config import Config

from wsgiref.simple_server import make_server

class SmtpGateway(EndpointFactory):
    inflight : Dict[str, SmtpEndpoint]
    config = None
    shutdown_gc = False

    def __init__(self, config):
        self.config = config

        rest_output  = config.root_yaml.get('rest_output', None)

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

    def rest_factory(self, yaml):
        return RestEndpoint(
            yaml['endpoint'],
            http_host=yaml['host'],
            timeout_start=yaml.get('rcpt_timeout', 30),
            timeout_data=yaml.get('data_timeout', 60))

    def rest_endpoint_yaml(self, name):
        for endpoint_yaml in self.config.root_yaml['rest_output']:
            if endpoint_yaml['name'] == name:
                return endpoint_yaml
        return None

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
            rest_yaml = self.config.root_yaml['rest_listener']
            now = time.monotonic()
            logging.debug('SmtpGateway.gc_inflight %f', now)
            delta = now - last_gc
            tx_idle_gc = rest_yaml.get('tx_idle_gc', 5)
            if delta < tx_idle_gc:
                time.sleep(tx_idle_gc - delta)
            last_gc = now
            dele = []
            for (rest_id, tx) in self.inflight.items():
                tx_idle = now - tx.idle_start if tx.idle_start else 0
                if tx_idle > rest_yaml.get('tx_idle_timeout', 5):
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
            endpoint_yaml = self.rest_endpoint_yaml(service_yaml['endpoint'])
            factory = lambda: self.rest_factory(endpoint_yaml)

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
            listener_yaml = self.config.root_yaml['rest_listener']
            self.wsgi_server = make_server(
                listener_yaml['addr'][0], listener_yaml['addr'][1],
                flask_app)
            self.wsgi_server.serve_forever()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')
    config = Config()
    config.load_yaml(sys.argv[1])
    gw = SmtpGateway(config)
    gw.main()
