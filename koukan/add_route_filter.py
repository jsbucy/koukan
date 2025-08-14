# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional
import logging

from koukan.filter import (
    TransactionMetadata,
    WhichJson )
from koukan.filter_chain import FilterChain, FilterResult, Filter
from koukan.response import Response

def _err(r : Optional[Response]) -> Optional[Response]:
    if r is None or r.ok():
        return None
    return Response(r.code, r.message + ' (AddRouteFilter upstream)')

# AddRouteFilter forks a message to another Filter in addition to the
# primary/upstream. There are 2 likely configurations: chain with...
# 1: RestEndpoint if you want sync/cutthrough behavior, block on the
#    add-route endpoint use this if the additional route is local, you
#    own the availability, etc.
# 2: StorageWriterFilter + AsyncFilterWrapper (a la Exploder) for
#    async/store&forward if you don't want to block on the add-route endpoint.

# NOTE store&forward configurations:
# 1: AsyncFilterWrapper should be configured with 0 upstream timeouts
# 2: Think carefully about notification/retry params. Depending on the
# use case, it may make more sense to retry forever (and effectively
# never bounce) and use monitoring to detect if that is persistently
# failing.
class AddRouteFilter(Filter):
    add_route : FilterChain
    host : str

    def __init__(self, add_route : FilterChain, host : str):
        self.add_route = add_route
        self.host = host

    def _resp_err(self):
        if mail_err := _err(self.add_route.tx.mail_response):
            self.downstream.mail_response = mail_err
        rcpt_err = None
        if len(self.add_route.tx.rcpt_response) == 1:  # cf assert in on_update()
            if rcpt_err := _err(self.add_route.tx.rcpt_response[0]):
                self.downstream.rcpt_response = [rcpt_err]
        if data_err := _err(self.add_route.tx.data_response):
            self.downstream.data_response = data_err

    def on_update(self, tx_delta : TransactionMetadata):
        # post-exploder output chain/single-rcpt only for now
        assert len(self.downstream.rcpt_to) <= 1
        add_route_delta = tx_delta.copy_valid(WhichJson.ADD_ROUTE)
        if self.add_route.tx is None:
            self.add_route.init(TransactionMetadata())
            add_route_delta.host = self.host
        self.add_route.tx.merge_from(add_route_delta)
        self.add_route.update()
        self._resp_err()
        # NOTE this returns any error from the add route
        # downstream verbatim, it's possible this might contain
        # debugging information internal to the site that you
        # don't want to return externally
        return FilterResult()
