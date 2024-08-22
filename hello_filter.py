from typing import Optional
from filter import SyncFilter, TransactionMetadata
import logging

class HelloFilter(SyncFilter):
    def __init__(self, next):
        self.next = next
    def on_update(
            self, tx : TransactionMetadata, tx_delta : TransactionMetadata
    ) -> Optional[TransactionMetadata]:
        logging.debug('HelloFilter.on_update %s', tx)
        return self.next.on_update(tx, tx_delta)

def factory(yaml, next):
    return HelloFilter(next)
