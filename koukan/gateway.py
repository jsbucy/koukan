# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Callable, Dict, List, Optional, Tuple
from types import ModuleType
import sys
import logging
import logging.config
import time
import secrets
from threading import Condition, Lock, Thread
import asyncio
from functools import partial
import importlib

from koukan.rest_endpoint import RestEndpoint, RestEndpointClientProvider
from koukan.smtp_endpoint import Factory as SmtpFactory, SmtpEndpoint
from koukan.smtp_service import (
    ControllerTls,
    SmtpHandler,
    service as smtp_service )
import koukan.fastapi_service as fastapi_service
from koukan.filter import AsyncFilter, TransactionMetadata
from koukan.filter_chain import BaseFilter, FilterChain
from koukan.sync_filter_adapter import SyncFilterAdapter
from koukan.rest_handler import (
    EndpointFactory,
    RestHandlerFactory )
import koukan.uvicorn_main as uvicorn_main
import yaml
from koukan.executor import Executor



class SmtpGateway(EndpointFactory):
    inflight : Dict[str, SyncFilterAdapter]
    config_yaml : Optional[dict] = None
    shutdown_gc = False
    executor : Executor
    lock : Lock
    cv: Condition
    rest_id_factory : Optional[Callable[[], str]]
    http_server : Optional[uvicorn_main.Server] = None
    smtp_services : List[ControllerTls]
    smtp_factory : Dict[str, SmtpFactory]

    smtplib : Optional[ModuleType] = None
    rest_endpoint_clients : List[Tuple[dict, RestEndpointClientProvider]]
    loop : asyncio.AbstractEventLoop
    gc_thread : Optional[Thread] = None

    def __init__(self, config_yaml : Optional[dict] = None):
        self.config_yaml = config_yaml

        self.smtp_factory = {}

        self.inflight = {}

        self.lock = Lock()
        self.cv = Condition(self.lock)
        self.smtp_services = []
        self.rest_endpoint_clients = []
        self.loop = asyncio.new_event_loop()

    def shutdown(self) -> bool:
        logging.info("SmtpGateway.shutdown()")
        for service in self.smtp_services:
            service.stop()

        if self.http_server:
            logging.debug('SmtpGateway https erver shutdown')
            try:
                self.http_server.shutdown()
            except:
                pass

        if self.gc_thread is not None:
            with self.lock:
                self.shutdown_gc = True
                self.cv.notify_all()
            self.gc_thread.join()
            self.gc_thread = None

        success = self.executor.shutdown(timeout=10)

        logging.info("SmtpGateway.shutdown() done")
        return success

    def rest_factory(self, yaml) -> FilterChain:
        logging.debug('rest_factory %s', yaml)

        client_args = { 'verify': yaml.get('verify', True) }
        for c in self.rest_endpoint_clients:
            if c[0] == client_args:
                client = c[1]
                break
        else:
            client = RestEndpointClientProvider(**client_args)
            self.rest_endpoint_clients.append((client_args, client))

        endpoint = RestEndpoint(
            static_base_url=yaml['endpoint'],
            timeout_start=yaml.get('rcpt_timeout', 30),
            timeout_data=yaml.get('data_timeout', 60),
            client_provider=client,
            chunk_size=yaml.get('chunk_size', 2**16))
        assert isinstance(endpoint, BaseFilter)
        return FilterChain([endpoint], self.loop)

    def rest_endpoint_yaml(self, name):
        for endpoint_yaml in self.config_yaml['rest_output']:
            if endpoint_yaml['name'] == name:
                return endpoint_yaml
        return None

    # EndpointFactory
    def create(self, sender, tag : Optional[str]
               ) -> Optional[Tuple[AsyncFilter, dict]]:
        assert self.config_yaml is not None
        assert self.rest_id_factory is not None
        rest_yaml = self.config_yaml['rest_listener']
        if tag is None:
            return None
        if (factory := self.smtp_factory.get(tag, None)) is None:
            return None

        # The ehlo_host comes from the yaml and not the request
        # because it typically needs to align to the source IP
        # rdns. This stanza could select among multiple IPs in the
        # future, etc.
        endpoint = factory.new()
        chain = FilterChain([endpoint], self.loop)

        with self.lock:
            rest_id = self.rest_id_factory()
            if rest_id in self.inflight:
                return None

            executor = SyncFilterAdapter(self.executor, chain, rest_id)
            self.inflight[rest_id] = executor

            return executor, {'rest_lro': False}

    # EndpointFactory
    def get(self, rest_id):
        return self.inflight.get(rest_id, None)

    def _gc_inflight(self, now : float, ttl : int, done_ttl : int):
        with self.lock:
            dele = []
            for (rest_id, adapter) in self.inflight.items():
                # TODO policy should be something like
                # not inflight upstream and (idle or done)
                # grace period/lower but nonzero idle timeout for done
                # to GET again, cost is ~responses in memory?
                if not adapter.idle(time.monotonic(), ttl, done_ttl):
                    continue

                logging.info('SmtpGateway.gc_inflight shutdown idle %s',
                             adapter.rest_id)
                tx = adapter.get()
                assert tx is not None
                delta = TransactionMetadata(cancelled=True)
                tx.merge_from(delta)
                adapter.update(tx, delta)
                dele.append(rest_id)

        for d in dele:
            del self.inflight[d]

    def gc_inflight(self):
        last_gc = 0
        while not self.shutdown_gc:
            rest_yaml = self.config_yaml['rest_listener']
            now = time.monotonic()
            delta = now - last_gc
            gc_interval = rest_yaml.get('gc_interval', 5)
            if delta < gc_interval:
                with self.lock:
                    self.cv.wait_for(lambda: self.shutdown_gc,
                                     gc_interval - delta)
            last_gc = now
            self._gc_inflight(now, rest_yaml.get('gc_tx_ttl', 600),
                              rest_yaml.get('gc_done_ttl', 10))

    def smtp_handler_factory(self, **kwargs):
        return SmtpHandler(**kwargs)

    def main(self, argv : List[str] = [], alive=None):
        if self.config_yaml is None:
            with open(argv[1], 'r') as yaml_file:
                self.config_yaml = yaml.load(yaml_file, Loader=yaml.CLoader)

        global_yaml = self.config_yaml.get('global', {})
        executor_yaml = global_yaml.get('executor', {})

        self.executor = Executor(
            executor_yaml.get('max_inflight', 10),
            executor_yaml.get('watchdog_timeout', 3600))

        self.gc_thread = Thread(target = partial(self.gc_inflight),
                                daemon=True)
        self.gc_thread.start()

        rest_listener_yaml = self.config_yaml['rest_listener']
        self.rest_id_factory = partial(secrets.token_urlsafe,
            rest_listener_yaml.get('rest_id_entropy', 16))

        root_yaml = self.config_yaml
        logging_yaml = root_yaml.get('logging', None)
        if logging_yaml:
            logging.config.dictConfig(logging_yaml)

        smtp_yaml = self.config_yaml['smtp_output']
        smtplib_module = 'koukan_cpython_smtplib.smtplib'
        if smtp_yaml.get('use_system_smtplib', False):
            smtplib_module = 'smtplib'
        self.smtplib = importlib.import_module(smtplib_module)

        for host,smtp_host_yaml in smtp_yaml['hosts'].items():
            self.smtp_factory[host] = SmtpFactory(
                self.smtplib,
                ehlo_hostname = smtp_host_yaml['ehlo_host'],
                # 1h (default watchdog timeout) - 5min
                timeout = smtp_host_yaml.get('timeout', 55*60),
                protocol = smtp_host_yaml.get('protocol', 'smtp'),
                enable_bdat = smtp_host_yaml.get('enable_bdat', False),
                chunk_size = smtp_host_yaml.get('chunk_size', 2**16))

        for service_yaml in root_yaml['smtp_listener']['services']:
            factory = None
            msa = False
            endpoint_yaml = self.rest_endpoint_yaml(service_yaml['endpoint'])
            assert endpoint_yaml is not None

            # cf router config.Config.exploder()
            rcpt_timeout=40
            data_timeout=310
            if msa:
                rcpt_timeout=15
                data_timeout=40

            addr = service_yaml['addr']
            handler_factory = partial(
                self.smtp_handler_factory,
                chain_factory=partial(self.rest_factory, endpoint_yaml),
                executor=Executor(inflight_limit=100, watchdog_timeout=3600),
                timeout_rcpt=service_yaml.get('rcpt_timeout', rcpt_timeout),
                timeout_data=service_yaml.get('data_timeout', data_timeout),
                chunk_size=endpoint_yaml.get('chunk_size', 2**20),
                refresh_interval=endpoint_yaml.get('refresh_interval', 30),
                tag=service_yaml['tag'])

            self.smtp_services.append(smtp_service(
                hostname=addr[0], port=addr[1],
                cert=service_yaml.get('cert', None),
                key=service_yaml.get('key', None),
                auth_secrets_path=service_yaml.get('auth_secrets', None),
                proxy_protocol_timeout=
                  service_yaml.get('proxy_protocol_timeout', None),
                smtp_handler_factory=handler_factory,
                enable_bdat=service_yaml.get('enable_bdat', False),
                chunk_size=service_yaml.get('chunk_size', None),
                smtps=service_yaml.get('smtps', False)
            ))

        session_uri=rest_listener_yaml.get('session_uri', None)
        self.adapter_factory = RestHandlerFactory(
            self.executor, endpoint_factory=self,
            rest_id_factory=self.rest_id_factory,
            session_url=session_uri,
            # for gw the service_url is always the same as the
            # session_url since tx are local to each node
            service_url=session_uri,
            chunk_size=rest_listener_yaml.get('chunk_size', None))

        rest_listener_yaml = root_yaml['rest_listener']
        app = fastapi_service.create_app(self.adapter_factory)

        cert = rest_listener_yaml.get('cert', None)
        key = rest_listener_yaml.get('key', None)

        self.http_server = uvicorn_main.Server(
            app,
            rest_listener_yaml['addr'],
            cert=cert, key=key,
            alive=alive if alive else self.heartbeat)
        self.http_server.run()
        logging.debug('SmtpGateway.main() done')

    def heartbeat(self):
        if self.executor.check_watchdog():
            return True
        if self.http_server is not None:
            self.http_server.shutdown()
        return False

