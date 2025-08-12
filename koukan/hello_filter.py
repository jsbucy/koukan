# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional
from koukan.filter import TransactionMetadata
from koukan.filter_chain import FilterResult, OneshotFilter
import logging

class HelloFilter(OneshotFilter):
    def __init__(self):
        pass
    def on_update(self, tx_delta : TransactionMetadata):
        logging.debug('HelloFilter.on_update %s', self.downstream)
        return FilterResult()

def factory(yaml) -> OneshotFilter:
    return HelloFilter()
