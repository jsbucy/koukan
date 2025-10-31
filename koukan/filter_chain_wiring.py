# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Callable, Dict, List, Optional, Tuple
import logging
import importlib
from functools import partial
import inspect

from koukan.filter_chain_factory import FilterChainFactory

from koukan.address_list_policy import AddressListPolicy
from koukan.dest_domain_policy import DestDomainPolicy
from koukan.recipient_router_filter import (
    Destination,
    RecipientRouterFilter,
    RoutingPolicy )
from koukan.recipient_router_factory import RecipientRouterFactory
from koukan.rest_endpoint import RestEndpoint, RestEndpointClientProvider
from koukan.dkim_endpoint import DkimEndpoint
from koukan.mx_resolution import DnsResolutionFilter
from koukan.message_parser_filter import MessageParserFilter
from koukan.filter import (
    AsyncFilter,
    HostPort,
    Resolution )
from koukan.filter_chain import BaseFilter, FilterChain
from koukan.exploder import Exploder
from koukan.remote_host_filter import RemoteHostFilter
from koukan.received_header_filter import ReceivedHeaderFilter
from koukan.relay_auth_filter import RelayAuthFilter
from koukan.async_filter_wrapper import AsyncFilterWrapper
from koukan.add_route_filter import AddRouteFilter
from koukan.message_builder_filter import MessageBuilderFilter
from koukan.sender import Sender

StorageWriterFactory = Callable[[Sender, bool],Optional[AsyncFilter]]

class FilterChainWiring:
    exploder_output_factory : Optional[StorageWriterFactory] = None
    router_factory : Optional[RecipientRouterFactory] = None
    filter_chain_factory : Optional[FilterChainFactory] = None
    rest_endpoint_clients : List[Tuple[dict, RestEndpointClientProvider]]

    def __init__(
            self,
            exploder_output_factory : Optional[StorageWriterFactory] = None):
        self.exploder_output_factory = exploder_output_factory
        self.rest_endpoint_clients = []

    def __del__(self):
        for c in self.rest_endpoint_clients:
            c[1].close()

    def wire(self, yaml, factory : FilterChainFactory):
        self.filter_chain_factory = factory
        self.router_factory = RecipientRouterFactory()
        self.router_factory.load_policies(yaml)

        factory.add_filter('rest_output', self.rest_output)
        factory.add_filter('dkim', self.dkim)
        factory.add_filter('message_parser', self.message_parser)
        factory.add_filter('remote_host', self.remote_host)
        factory.add_filter('received_header', self.received_header)
        factory.add_filter('relay_auth', self.relay_auth)
        factory.add_filter('dns_resolution', self.dns_resolution)

        # exploder_output_factory / router handle_new_tx()
        factory.add_filter('exploder', self.exploder)
        factory.add_filter('add_route', self.add_route)

        factory.add_filter('router', self.router_factory.build_router)
        factory.add_filter('message_builder', self.message_builder)
        factory.add_filter('exploder_upstream', self.exploder_upstream_yaml)

    def exploder_upstream(self, sender : Sender,
                          rcpt_timeout : float,
                          data_timeout : float,
                          store_and_forward : bool,
                          block_upstream : bool,
                          notify : bool,
                          retry : bool):
        assert self.exploder_output_factory is not None
        upstream : Optional[AsyncFilter] = self.exploder_output_factory(
            sender, block_upstream)
        if upstream is None:
            return None
        return AsyncFilterWrapper(
            upstream, rcpt_timeout, store_and_forward=store_and_forward,
            notify=notify, retry=retry)

    def exploder(self, yaml):
        msa = msa=yaml.get('msa', False)
        rcpt_timeout = 30
        data_timeout = 300
        if msa:
            rcpt_timeout = 5
            data_timeout = 30
        # if one wanted to store&forward on executor overflow
        # (i.e. pass block_upstream=False below),
        # exploder_output_factory probably needs to return an extra
        # bool here to tell you that that happened to set these
        # timeouts to 0 like add-route.
        # cf exploder.Recipient.first_update()
        return Exploder(
            yaml['sender'],
            tag=yaml.get('tag', None),
            upstream_factory=partial(
                self.exploder_upstream,
                Sender(yaml['sender'], yaml['tag']),
                rcpt_timeout, data_timeout, store_and_forward=msa,
                block_upstream=True, notify=True, retry=True),
            rcpt_timeout=yaml.get('rcpt_timeout', rcpt_timeout),
            data_timeout=yaml.get('data_timeout', data_timeout))

    def exploder_upstream_yaml(self, yaml):
        return self.exploder_upstream(
            Sender(yaml['sender'], yaml.get('tag', None)),
            yaml['rcpt_timeout'],
            yaml['data_timeout'],
            yaml['store_and_forward'],
            yaml['block_upstream'],
            yaml['notify'],
            yaml['retry'])

    def add_route(self, yaml):
        if 'output_chain' not in yaml:
            return None
        if yaml.get('store_and_forward', None):
            # we configure AsyncFilterWrapper *not* to toggle
            # retry/notify upstream; it gets that from the upstream
            # chain
            upstream_yaml = {
                'chain': [{
                    'sender': yaml['sender'],
                    'filter': 'exploder_upstream',
                    'rcpt_timeout': 0,
                    'data_timeout': 0,  # 0 upstream timeout ~ effectively swallow errors
                    'store_and_forward': True,
                    'block_upstream': False,
                    'notify': False,
                    'retry': False
                }]
            }
            if tag := yaml.get('tag', None):
                upstream_yaml['tag'] = tag
            add_route, unused_yaml = (
                self.filter_chain_factory.build_filter_chain(
                    Sender(yaml['sender']), upstream_yaml))
        else:
            output = self.filter_chain_factory.build_filter_chain(
                Sender(yaml['sender'], yaml.get('tag', None)))
            if output is None:
                return None
            add_route, output_yaml = output
        return AddRouteFilter(add_route, yaml['sender'], yaml.get('tag', None))

    def rest_output(self, yaml):
        logging.debug('Config.rest_output %s', yaml)
        chunk_size = yaml.get('chunk_size', None)
        static_remote_host_yaml = yaml.get('static_remote_host', None)
        static_remote_host = (HostPort.from_yaml(static_remote_host_yaml)
                              if static_remote_host_yaml else None)
        logging.info('Factory.rest_output %s', static_remote_host)
        rcpt_timeout = 30
        data_timeout = 300
        # cache httpx.Client by params
        client_args = { 'verify': yaml.get('verify', True) }
        for c in self.rest_endpoint_clients:
            if c[0] == client_args:
                client = c[1]
                break
        else:
            client = RestEndpointClientProvider(**client_args)
            self.rest_endpoint_clients.append((client_args, client))

        return RestEndpoint(
            static_base_url = yaml.get('static_endpoint', None),
            timeout_start=yaml.get('rcpt_timeout', rcpt_timeout),
            timeout_data=yaml.get('data_timeout', data_timeout),
            client_provider=client,
            chunk_size=chunk_size)

    def dkim(self, yaml):
        if 'key' not in yaml:
            return None
        return DkimEndpoint(
            yaml['domain'], yaml['selector'], yaml['key'])

    def message_parser(self, yaml):
        return MessageParserFilter()

    def remote_host(self, yaml):
        return RemoteHostFilter()

    def received_header(self, yaml):
        return ReceivedHeaderFilter(yaml.get('received_hostname', None))

    def relay_auth(self, yaml):
        return RelayAuthFilter(smtp_auth = yaml.get('smtp_auth', False))

    def dns_resolution(self, yaml):
        host_list = yaml.get('static_hosts', None)
        static_resolution = None
        if host_list:
            static_resolution = Resolution(
                [HostPort.from_yaml(h) for h in host_list])
        # TODO add option for mx resolution (vs just A)
        return DnsResolutionFilter(
            static_resolution=static_resolution,
            suffix=yaml.get('suffix', None),
            literal=yaml.get('literal', None))

    def message_builder(self, yaml):
        return MessageBuilderFilter()
