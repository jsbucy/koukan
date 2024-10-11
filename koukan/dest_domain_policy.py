# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional, Tuple
import copy

from koukan.address import domain_from_address
from koukan.response import Response
from koukan.recipient_router_filter import Destination, RoutingPolicy
from koukan.filter import HostPort

# sets Destination.remote_host to rhs of rcpt addr

class DestDomainPolicy(RoutingPolicy):
    dest : Destination
    dest_port : int

    def __init__(self, dest : Destination, dest_port = 25):
        self.dest = dest
        self.dest_port = dest_port

    # called on the first recipient in the transaction
    def endpoint_for_rcpt(self, rcpt) -> Tuple[
            Optional[Destination], Optional[Response]]:
        domain = domain_from_address(rcpt)
        if domain is None:
            return None, None
        dest = copy.copy(self.dest)
        dest.remote_host = [HostPort(domain, self.dest_port)]
        return dest, None
