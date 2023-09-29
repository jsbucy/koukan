import sys

from rest_endpoint import RestEndpoint, BlobIdMap
from smtp_endpoint import Factory as SmtpFactory

from executor import Executor
from tags import Tag

from blobs import BlobStorage
from smtp_service import service as smtp_service
import rest_service
import gunicorn_main

import logging

class SmtpGateway:
    def __init__(self):
        self.rest_port = int(sys.argv[1])
        self.router_port = int(sys.argv[2])
        self.mx_port = int(sys.argv[3])
        self.msa_port = int(sys.argv[4])
        self.cert = sys.argv[5]
        self.key = sys.argv[6]
        self.auth_secrets = sys.argv[7]
        self.ehlo_host = sys.argv[8]
        self.listen_host = sys.argv[9]


        self.blob_id_map = BlobIdMap()

        self.router_base_url = 'http://localhost:%d/' % self.router_port

        self.blobs = BlobStorage()
        self.smtp_factory = SmtpFactory()

    def mx_rest_factory(self):
        return RestEndpoint(self.router_base_url, http_host='inbound-gw',
                            blob_id_map=self.blob_id_map)

    def msa_rest_factory(self):
        return RestEndpoint(self.router_base_url, http_host='outbound-gw')

    def smtp_endpoint_factory(self, host):
        if host == 'outbound':
            return self.smtp_factory.new(
                ehlo_hostname=self.ehlo_host), False # msa
        else:
            return None

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

        gunicorn_main.run(
            'localhost', self.rest_port, cert=None, key=None,
            app=rest_service.create_app(
                lambda host: self.smtp_endpoint_factory(host), self.blobs))


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')
    gw = SmtpGateway()
    gw.main()
