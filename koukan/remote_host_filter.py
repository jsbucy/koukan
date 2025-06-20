# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional, Tuple
import dns.resolver
import logging

from koukan.filter import (
    SyncFilter,
    TransactionMetadata )
from koukan.response import Response

from koukan.dns_wrapper import NotFoundExceptions, Resolver, ServFailExceptions

class RemoteHostFilter(SyncFilter):
    upstream : SyncFilter
    delta : Optional[TransactionMetadata] = None

    def __init__(self, upstream : SyncFilter,
                 resolver : Optional[Resolver] = None):
        self.upstream = upstream
        self.resolver = resolver if resolver else Resolver()

    def on_update(self, tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:
        upstream_delta = None
        done = False
        if self.delta is None and tx_delta.mail_from is not None:
            err, self.delta = self._resolve(tx)
            if err is not None:
                upstream_delta = TransactionMetadata()
                tx.fill_inflight_responses(err, upstream_delta)
                tx.merge_from(upstream_delta)
                return upstream_delta
            done = True

        upstream_tx = tx.copy()
        upstream_delta = tx_delta.copy()
        if self.delta is not None:
            assert upstream_tx.merge_from(self.delta)
            if done:
                assert upstream_delta.merge_from(self.delta)
        if self.upstream is not None:
            upstream_delta = self.upstream.on_update(
                upstream_tx, upstream_delta)
        else:
            upstream_delta = TransactionMetadata()
        assert tx.merge_from(upstream_delta) is not None
        return upstream_delta

    def _resolve(self, tx : TransactionMetadata
                 ) -> Tuple[Optional[Response],  # err
                            # added fields to send upstream
                            Optional[TransactionMetadata]]:
        if (tx.remote_host is None or
            not tx.remote_host.host):
            return None, None
        ans = None
        try:
            ans = self.resolver.resolve_address(tx.remote_host.host)
        except ServFailExceptions:
            return Response(450, 'RemoteHostFilter ptr err'), None
        except NotFoundExceptions:
            pass

        res = TransactionMetadata()
        if not(ans) or not ans[0].target:
            res.remote_hostname = ''
            res.fcrdns = False
            return None, res
        res.remote_hostname = str(ans[0].target)

        all_failed = True
        for rrtype in ['a', 'aaaa']:
            ans = None
            try:
                ans = self.resolver.resolve(
                    res.remote_hostname, rrtype)
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
                    res.fcrdns = True
                    break
            if res.fcrdns:
                break
        else:
            res.fcrdns = False

        if all_failed:
            return Response(450, 'RemoteHostFilter fwd err'), None

        logging.debug('RemoteHostFilter._resolve() '
                      'remote_hostname=%s fcrdns=%s',
                      res.remote_hostname, res.fcrdns)
        return None, res

    def abort(self):
        pass
