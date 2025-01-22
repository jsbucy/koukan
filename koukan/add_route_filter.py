# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional
import logging

from koukan.filter import (
    SyncFilter,
    TransactionMetadata,
    WhichJson )
from koukan.response import Response

def _err(r : Optional[Response]):
    return r is not None and r.err()

# AddRouteFilter forks a message to another SyncFilter in addition to the
# primary/upstream. There are 2 likely configurations: chain with...
# 1: RestEndpoint if you want sync behavior, block on the add-route endpoint
#    use this if the additional route is local, you own the availability, etc.
# 2: StorageWriterFilter + AsyncFilterWrapper (a la Exploder) for
#    async, if you don't want to block on the add-route endpoint. This
#    of course has the possibility of failing after the fact in which
#    case you must decide whether to bounce and to where.

# TODO most use of store-and-forward here should probably rewrite mail_from?
# TODO AsyncFilterWrapper store_and_forward only upgrades timeout/temp
# err, perm errors that are reported within the timeout will be
# returned here and block the primary route. Probably that indicates a
# major misconfiguration. Probably there needs to be an option for
# AsyncFilterWrapper to upgrade perm errors in addition to
# temp/timeout?
class AddRouteFilter(SyncFilter):
    add_route : SyncFilter
    upstream : SyncFilter
    create = True
    add_route_tx : Optional[TransactionMetadata] = None

    def __init__(self, add_route : SyncFilter,
                 host : str,
                 upstream : SyncFilter):
        self.add_route = add_route
        self.upstream = upstream
        self.host = host

    def on_update(self, tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:
        add_route_delta = tx_delta.copy_valid(WhichJson.ADD_ROUTE)
        if self.add_route_tx is None:
            self.add_route_tx = tx.copy_valid(WhichJson.ADD_ROUTE)
            self.add_route_tx.host = add_route_delta.host = self.host
        else:
            assert self.add_route_tx.merge_from(add_route_delta) is not None
        add_route_upstream_delta = self.add_route.on_update(
            self.add_route_tx, add_route_delta)
        logging.debug(self.add_route_tx)
        # TODO tx.resp_err()?
        if (_err(add_route_upstream_delta.mail_response) or
            any([_err(r) for r in add_route_upstream_delta.rcpt_response]) or
            _err(add_route_upstream_delta.data_response)):

            tx.merge_from(add_route_upstream_delta)
            return add_route_upstream_delta

        return self.upstream.on_update(tx, tx_delta)
