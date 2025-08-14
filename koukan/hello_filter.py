# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional
from koukan.filter import TransactionMetadata
from koukan.filter_chain import FilterResult, Filter
import logging

class HelloFilter(Filter):
    def __init__(self):
        pass
    def on_update(self, tx_delta : TransactionMetadata):
        logging.debug('HelloFilter.on_update %s', self.downstream)
        return FilterResult()

def factory(yaml) -> Filter:
    return HelloFilter()
