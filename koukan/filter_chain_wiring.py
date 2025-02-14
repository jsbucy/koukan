# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Callable, Dict, Optional, Tuple
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
from koukan.rest_endpoint import RestEndpoint
from koukan.dkim_endpoint import DkimEndpoint
from koukan.mx_resolution import DnsResolutionFilter
from koukan.message_parser_filter import MessageParserFilter
from koukan.filter import (
    AsyncFilter,
    HostPort,
    Resolution,
    SyncFilter )
from koukan.exploder import Exploder
from koukan.remote_host_filter import RemoteHostFilter
from koukan.received_header_filter import ReceivedHeaderFilter
from koukan.relay_auth_filter import RelayAuthFilter
from koukan.async_filter_wrapper import AsyncFilterWrapper
from koukan.add_route_filter import AddRouteFilter

StorageWriterFactory = Callable[[str, bool],Optional[AsyncFilter]]

class FilterChainWiring:
    exploder_output_factory : Optional[StorageWriterFactory] = None
    router_factory : Optional[RecipientRouterFactory] = None
    filter_chain_factory : Optional[FilterChainFactory] = None

    def __init__(
            self,
            exploder_output_factory : Optional[StorageWriterFactory] = None):
        self.exploder_output_factory = exploder_output_factory

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

    def exploder_upstream(self, http_host : str,
                          rcpt_timeout : float,
                          data_timeout : float,
                          store_and_forward : bool,
                          notification : Optional[dict],
                          retry : Optional[dict],
                          block_upstream : bool):
        upstream : Optional[AsyncFilter] = self.exploder_output_factory(
            http_host, block_upstream)
        if upstream is None:
            return None
        return AsyncFilterWrapper(
            upstream,
            rcpt_timeout,
            store_and_forward=store_and_forward,
            default_notification=notification, retry_params=retry)

    def exploder(self, yaml, next):
        assert next is None
        msa = msa=yaml.get('msa', False)
        rcpt_timeout = 30
        data_timeout = 300
        if msa:
            rcpt_timeout = 5
            data_timeout = 30
        notification = yaml.get('default_notification', None)
        # if one wanted to store&forward on executor overflow
        # (i.e. pass block_upstream=False below),
        # exploder_output_factory probably needs to return an extra
        # bool here to tell you that that happened to set these
        # timeouts to 0 like add-route.
        # cf exploder.Recipient.first_update()
        return Exploder(
            yaml['output_chain'],
            partial(self.exploder_upstream, yaml['output_chain'],
                    rcpt_timeout, data_timeout, msa, notification,
                    retry={}, block_upstream=True),
            rcpt_timeout=yaml.get('rcpt_timeout', rcpt_timeout),
            data_timeout=yaml.get('data_timeout', data_timeout),
            default_notification=notification)

    def add_route(self, yaml, next):
        if yaml.get('store_and_forward', None):
            add_route = self.exploder_upstream(
                yaml['output_chain'],
                0, 0,  # 0 upstream timeout ~ effectively swallow errors
                store_and_forward=True,
                notification=yaml.get('notification', None),
                retry=yaml.get('retry_params', None),
                block_upstream=False)
        else:
            output = self.filter_chain_factory.build_filter_chain(
                yaml['output_chain'])
            if output is None:
                return None
            add_route, output_yaml = output
        return AddRouteFilter(add_route, yaml['output_chain'], next)

    def rest_output(self, yaml, next):
        logging.debug('Config.rest_output %s', yaml)
        assert next is None
        chunk_size = yaml.get('chunk_size', None)
        static_remote_host_yaml = yaml.get('static_remote_host', None)
        static_remote_host = (HostPort.from_yaml(static_remote_host_yaml)
                              if static_remote_host_yaml else None)
        logging.info('Factory.rest_output %s', static_remote_host)
        rcpt_timeout = 30
        data_timeout = 300
        return RestEndpoint(
            static_base_url = yaml.get('static_endpoint', None),
            static_http_host = yaml.get('http_host', None),
            timeout_start=yaml.get('rcpt_timeout', rcpt_timeout),
            timeout_data=yaml.get('data_timeout', data_timeout),
            verify=yaml.get('verify', True),
            chunk_size=chunk_size)

    def dkim(self, yaml, next):
        if 'key' not in yaml:
            return None
        return DkimEndpoint(
            yaml['domain'], yaml['selector'], yaml['key'], next)

    def message_parser(self, yaml, next):
        return MessageParserFilter(next)

    def remote_host(self, yaml, next):
        return RemoteHostFilter(next)

    def received_header(self, yaml, next):
        return ReceivedHeaderFilter(next, yaml.get('received_hostname', None))

    def relay_auth(self, yaml, next):
        return RelayAuthFilter(next, smtp_auth = yaml.get('smtp_auth', False))

    def dns_resolution(self, yaml, next):
        host_list = yaml.get('static_hosts', None)
        static_resolution = None
        if host_list:
            static_resolution = Resolution(
                [HostPort.from_yaml(h) for h in host_list])
        # TODO add option for mx resolution (vs just A)
        return DnsResolutionFilter(
            next,
            static_resolution=static_resolution,
            suffix=yaml.get('suffix', None),
            literal=yaml.get('literal', None))

