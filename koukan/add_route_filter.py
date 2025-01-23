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
# TODO move to TransactionMetadata?
def _resp_err(tx : TransactionMetadata) -> bool:
    if _err(tx.mail_response):
        return True
    if any([r.err() for r in tx.rcpt_response]):
        return True
    if _err(tx.data_response):
        return True
    return False

# AddRouteFilter forks a message to another SyncFilter in addition to the
# primary/upstream. There are 2 likely configurations: chain with...
# 1: RestEndpoint if you want sync behavior, block on the add-route endpoint
#    use this if the additional route is local, you own the availability, etc.
# 2: StorageWriterFilter + AsyncFilterWrapper (a la Exploder) for
#    async, if you don't want to block on the add-route endpoint.

# NOTE store&forward configurations:
# 1: AsyncFilterWrapper should be configured with 0 upstream timeouts
# 2: Think carefully about notification/retry params. Depending on the
# use case, it may make more sense to retry forever (and effectively
# never bounce) and use monitoring to detect if that is persistently
# failing.
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
        # post-exploder output chain/single-rcpt only for now
        assert len(tx.rcpt_to) <= 1
        add_route_delta = tx_delta.copy_valid(WhichJson.ADD_ROUTE)
        if self.add_route_tx is None:
            self.add_route_tx = tx.copy_valid(WhichJson.ADD_ROUTE)
            self.add_route_tx.host = add_route_delta.host = self.host
        else:
            assert self.add_route_tx.merge_from(add_route_delta) is not None
        add_route_upstream_delta = self.add_route.on_update(
            self.add_route_tx, add_route_delta)
        logging.debug(self.add_route_tx)
        if not tx.cancelled and _resp_err(self.add_route_tx):
            # NOTE this returns any error from the add route
            # downstream verbatim, it's possible this might contain
            # debugging information internal to the site that you
            # don't want to return externally
            tx.merge_from(add_route_upstream_delta)
            return add_route_upstream_delta

        return self.upstream.on_update(tx, tx_delta)
