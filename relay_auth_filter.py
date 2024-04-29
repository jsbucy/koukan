from typing import Optional
from filter import Filter, Response, TransactionMetadata

# Filter that fails MAIL in the absence of a positive signal to
# authorize relaying.
class RelayAuthFilter(Filter):
    next : Optional[Filter] = None
    smtp_auth : Optional[bool] = False

    def __init__(self, next : Optional[Filter] = None,
                 # allow relaying if smtp auth present
                 smtp_auth : Optional[bool] = False):
        self.next = next
        self.smtp_auth = smtp_auth

    def _check(self, tx : TransactionMetadata):
        if tx.mail_from is None:
            return

        if (self.smtp_auth and
            tx.smtp_meta is not None and
            tx.smtp_meta.get('auth', False)):
            return

        tx.mail_response = Response(550, '5.7.1 not authorized')

    def on_update(self, tx : TransactionMetadata):
        self._check(tx)
        if tx.mail_response is None and self.next:
            self.next.on_update(tx)

    def abort(self):
        pass
