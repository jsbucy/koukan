# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import (
    Awaitable,
    Callable,
    Optional )
import logging

from koukan.response import Response
from koukan.filter import (
    Mailbox,
    TransactionMetadata )
from koukan.filter_chain import CoroutineProxyFilter, FilterResult

class MailOkFilter(CoroutineProxyFilter):
    downstream_mail : Optional[Mailbox] = None
    downstream_mail_resp = False

    def __init__(self):
        pass

    async def on_update(
            self, tx_delta : TransactionMetadata,
            upstream : Callable[[], Awaitable[TransactionMetadata]]
    ) -> None:
        assert self.downstream_tx is not None
        assert self.upstream_tx is not None

        if self.downstream_mail is not None and tx_delta.rcpt_to:
            logging.debug('restore mail')
            tx_delta.mail_from = self.downstream_mail
            self.downstream_mail = None
        elif ((tx_delta.mail_from is not None) and
            (tx_delta.mail_response is None) and
            not tx_delta.rcpt_to):
            logging.debug('save mail')
            self.downstream_mail = tx_delta.mail_from
            tx_delta.mail_from = None

        assert self.upstream_tx.merge_from(tx_delta) is not None
        upstream_delta = await upstream()
        if self.downstream_mail is not None:
            self.downstream_tx.mail_response = Response(
                250, 'mail ok (mail_ok_filter)')
            self.downstream_mail_resp = True
        elif self.downstream_mail_resp:
            if (upstream_delta.mail_response is not None and
                upstream_delta.mail_response.err()):
                # replace rcpt responses in upstream_delta (which are
                # probably 503-5.1.1 failed precondition) with
                # mail_resp which is the real error
                upstream_delta.rcpt_response = (
                    [upstream_delta.mail_response] *
                    len(upstream_delta.rcpt_response))
        logging.debug(upstream_delta)
        assert self.downstream_tx.merge_from(upstream_delta) is not None
