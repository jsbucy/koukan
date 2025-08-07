# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional, Tuple
import dns.resolver
import logging

from koukan.filter import TransactionMetadata
from koukan.filter_chain import Filter
from koukan.response import Response

from koukan.dns_wrapper import NotFoundExceptions, Resolver, ServFailExceptions

class RemoteHostFilter(Filter):
    def __init__(self, resolver : Optional[Resolver] = None):
        self.resolver = resolver if resolver else Resolver()

    async def on_update(self, tx_delta : TransactionMetadata, upstream):
        if tx_delta.mail_from is not None:
            if (err := self._resolve()) is not None:
                self.downstream.fill_inflight_responses(err)
                return

        await upstream()

    def _resolve(self) -> Optional[Response]:
        tx = self.downstream
        if (tx.remote_host is None or
            not tx.remote_host.host):
            return None
        ans = None
        try:
            ans = self.resolver.resolve_address(tx.remote_host.host)
        except ServFailExceptions:
            return Response(450, 'RemoteHostFilter ptr err')
        except NotFoundExceptions:
            pass

        if not(ans) or not ans[0].target:
            tx.remote_hostname = ''
            tx.fcrdns = False
            return None
        tx.remote_hostname = str(ans[0].target)

        all_failed = True
        for rrtype in ['a', 'aaaa']:
            ans = None
            try:
                ans = self.resolver.resolve(
                    tx.remote_hostname, rrtype)
                all_failed = False
            except ServFailExceptions:
                pass
            except NotFoundExceptions:
                all_failed = False

            if ans is None:
                continue
            for a in ans:
                logging.debug('RemoteHostFilter._resolve %s %s %s',
                              rrtype, str(a), tx.remote_host.host)
                if str(a) == tx.remote_host.host:
                    tx.fcrdns = True
                    break
            if tx.fcrdns:
                break
        else:
            tx.fcrdns = False

        if all_failed:
            return Response(450, 'RemoteHostFilter fwd err')

        logging.debug('RemoteHostFilter._resolve() '
                      'remote_hostname=%s fcrdns=%s',
                      tx.remote_hostname, tx.fcrdns)

    def abort(self):
        pass
