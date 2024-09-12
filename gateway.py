from typing import Callable, Dict, List, Optional
import sys
import logging
from threading import Thread
import time
import secrets
from threading import Lock
import asyncio

from rest_endpoint import RestEndpoint
from smtp_endpoint import Factory as SmtpFactory, SmtpEndpoint
from smtp_service import service as smtp_service
import rest_service
import fastapi_service
from rest_endpoint_adapter import (
    SyncFilterAdapter,
    EndpointFactory,
    RestHandlerFactory )
import hypercorn_main
from config import Config
from executor import Executor


class SmtpGateway(EndpointFactory):
    inflight : Dict[str, SyncFilterAdapter]
    config = None
    shutdown_gc = False
    executor : Executor
    lock : Lock
    rest_id_factory : Optional[Callable[[], str]]
    hypercorn_shutdown : Optional[asyncio.Event] = None
    smtp_services : List[object]

    def __init__(self, config):
        self.config = config

        rest_output  = config.root_yaml.get('rest_output', None)

        self.smtp_factory = SmtpFactory()

        self.inflight = {}

        self.lock = Lock()

        global_yaml = self.config.root_yaml.get('global', {})
        executor_yaml = global_yaml.get('executor', {})

        self.executor = Executor(
            executor_yaml.get('max_inflight', 10),
            executor_yaml.get('watchdog_timeout', 3600))

        self.gc_thread = Thread(target = lambda: self.gc_inflight(),
                                daemon=True)
        self.gc_thread.start()

        rest_yaml = self.config.root_yaml['rest_listener']
        self.rest_id_factory = lambda: secrets.token_urlsafe(
            rest_yaml.get('rest_id_entropy', 16))
        self.smtp_services = []

    def shutdown(self) -> bool:
        logging.info("SmtpGateway.shutdown()")
        for service in self.smtp_services:
            service.stop()

        if self.hypercorn_shutdown:
            logging.debug('SmtpGateway hypercorn shutdown')
            try:
                self.hypercorn_shutdown.set()
            except:
                pass

        if self.gc_thread:
            self.shutdown_gc = True
            self.gc_thread.join()
            self.gc_thread = None

        success = self.executor.shutdown(timeout=10)

        logging.info("SmtpGateway.shutdown() done")
        return success

    def rest_factory(self, yaml):
        logging.debug('rest_factory %s', yaml)
        return RestEndpoint(
            yaml['endpoint'],
            static_http_host=yaml['host'],
            timeout_start=yaml.get('rcpt_timeout', 30),
            timeout_data=yaml.get('data_timeout', 60),
            verify=yaml.get('verify', True))

    def rest_endpoint_yaml(self, name):
        for endpoint_yaml in self.config.root_yaml['rest_output']:
            if endpoint_yaml['name'] == name:
                return endpoint_yaml
        return None

    # EndpointFactory
    def create(self, host):
        rest_yaml = self.config.root_yaml['rest_listener']

        # TODO possibly the http host for requests to the gw could
        # control which source_address to pass to smtplib.SMTP? or
        # select one of multiple smtp_output stanzas that specify ehlo
        # host, etc? Related: remote_host should be able to specify
        # use of smtps vs starttls?
        if host == 'outbound':
            # TODO no need to wire this down, could come from the
            # request? Possibly the only thing that would go here is
            # client cert?
            smtp_yaml = self.config.root_yaml['smtp_output']
            endpoint = self.smtp_factory.new(
                ehlo_hostname=smtp_yaml['ehlo_host'],
                # 1h (default watchdog timeout) - 5min
                timeout=smtp_yaml.get('timeout', 55*60))

            with self.lock:
                rest_id = self.rest_id_factory()
                if rest_id in self.inflight:
                    return None

                executor = SyncFilterAdapter(self.executor, endpoint, rest_id)
                self.inflight[rest_id] = executor

            return executor
        else:
            return None

    # EndpointFactory
    def get(self, rest_id):
        return self.inflight.get(rest_id, None)

    def _gc_inflight(self, now : float, ttl : int, done_ttl : int):
        with self.lock:
            dele = []
            for (rest_id, tx) in self.inflight.items():
                # TODO policy should be something like
                # not inflight upstream and (idle or done)
                # grace period/lower but nonzero idle timeout for done
                # to GET again, cost is ~responses in memory?
                if not tx.idle(time.monotonic(), ttl, done_ttl):
                    continue

                logging.info('SmtpGateway.gc_inflight shutdown idle %s',
                             tx.rest_id)
                assert isinstance(tx.filter, SmtpEndpoint)
                tx.filter._shutdown()
                dele.append(rest_id)

        for d in dele:
            del self.inflight[d]

    def gc_inflight(self):
        last_gc = 0
        while not self.shutdown_gc:
            rest_yaml = self.config.root_yaml['rest_listener']
            now = time.monotonic()
            logging.debug('SmtpGateway.gc_inflight %f', now)
            delta = now - last_gc
            gc_interval = rest_yaml.get('gc_interval', 5)
            if delta < gc_interval:
                time.sleep(gc_interval - delta)
            last_gc = now
            self._gc_inflight(now, rest_yaml.get('gc_tx_ttl', 600),
                              rest_yaml.get('gc_done_ttl', 10))

    def main(self, alive=None):
        for service_yaml in self.config.root_yaml['smtp_listener']['services']:
            factory = None
            msa = False
            endpoint_yaml = self.rest_endpoint_yaml(service_yaml['endpoint'])
            factory = lambda y=endpoint_yaml: self.rest_factory(y)

            # cf router config.Config.exploder()
            rcpt_timeout=40
            data_timeout=310
            if msa:
                rcpt_timeout=15
                data_timeout=40

            addr = service_yaml['addr']
            self.smtp_services.append(smtp_service(
                factory, hostname=addr[0], port=addr[1],
                cert=service_yaml.get('cert', None),
                key=service_yaml.get('key', None),
                auth_secrets_path=service_yaml.get('auth_secrets', None),
                rcpt_timeout=service_yaml.get('rcpt_timeout', rcpt_timeout),
                data_timeout=service_yaml.get('data_timeout', data_timeout)))

        self.adapter_factory = RestHandlerFactory(
            self.executor, endpoint_factory=self,
            rest_id_factory=self.rest_id_factory)

        rest_listener_yaml = self.config.root_yaml['rest_listener']
        if rest_listener_yaml.get('use_fastapi', False):
            app = fastapi_service.create_app(self.adapter_factory)
        else:
            app=rest_service.create_app(self.adapter_factory)

        cert = rest_listener_yaml.get('cert', None)
        key = rest_listener_yaml.get('key', None)

        self.hypercorn_shutdown = asyncio.Event()
        hypercorn_main.run(
            [rest_listener_yaml['addr']],
            cert=cert, key=key,
            app=app,
            shutdown=self.hypercorn_shutdown,
            alive=alive if alive else self.heartbeat)
        logging.debug('SmtpGateway.main() done')

    def heartbeat(self):
        if self.executor.check_watchdog():
            return True
        self.hypercorn_shutdown.set()
        return False

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s [%(thread)d] %(message)s')
    logging.getLogger('hpack').setLevel(logging.INFO)

    config = Config()
    config.load_yaml(sys.argv[1])
    gw = SmtpGateway(config)
    gw.main()
