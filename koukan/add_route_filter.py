# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional, Tuple
import logging

from koukan.filter import (
    TransactionMetadata,
    WhichJson )
from koukan.filter_chain import FilterChain, FilterResult, Filter
from koukan.response import Response
from koukan.sender import Sender

def _err(r : Optional[Response]) -> Tuple[bool, Response]:
    if r is None or r.ok():
        return False, Response(250, 'ok (AddRouteFilter noop)')
    return True, Response(r.code, r.message + ' (AddRouteFilter upstream)')

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
    sender : str
    tag : Optional[str]

    def __init__(self, add_route : FilterChain, sender,
                 tag : Optional[str] = None):
        self.add_route = add_route
        self.sender = sender
        self.tag = tag

    def _resp_err(self):
        atx = self.add_route.tx
        logging.debug(atx)
        mail_err, mail_resp = _err(atx.mail_response)

        rcpt_resp = (
            atx.rcpt_response[0]
            if len(atx.rcpt_response) == 1  # cf assert in on_update()
            else None)
        rcpt_err, rcpt_resp = _err(rcpt_resp)
        data_err, data_resp = _err(atx.data_response)
        if mail_err or rcpt_err or data_err:
            dtx = self.downstream_tx
            if not dtx.mail_response:
                dtx.mail_response = mail_resp
            if not dtx.rcpt_response:
                dtx.rcpt_response = [rcpt_resp]
            if not dtx.data_response:
                dtx.data_response = data_resp
            logging.debug(dtx)

    def on_update(self, tx_delta : TransactionMetadata):
        # post-exploder output chain/single-rcpt only for now
        assert self.downstream_tx is not None
        assert len(self.downstream_tx.rcpt_to) <= 1
        add_route_delta = tx_delta.copy_valid(WhichJson.ADD_ROUTE)
        if self.add_route.tx is None:
            self.add_route.init(TransactionMetadata())
            add_route_delta.sender = Sender(self.sender, self.tag)
        assert self.add_route.tx is not None
        self.add_route.tx.merge_from(add_route_delta)
        self.add_route.update()
        self._resp_err()
        # NOTE this returns any error from the add route
        # downstream verbatim, it's possible this might contain
        # debugging information internal to the site that you
        # don't want to return externally
        return FilterResult()
