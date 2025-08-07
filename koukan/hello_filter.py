# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional
from koukan.filter import TransactionMetadata
from koukan.filter_chain import Filter, TransactionMetadata
import logging

class HelloFilter(Filter):
    def __init__(self):
        pass
    async def on_update(self, tx_delta : TransactionMetadata, upstream):
        logging.debug('HelloFilter.on_update %s', self.downstream)
        await upstream()

def factory(yaml) -> Filter:
    return HelloFilter()
