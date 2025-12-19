
import logging

from koukan.filter import TransactionMetadata
from koukan.filter_chain import Filter, FilterResult
from koukan.response import Response

from koukan.remote_host_filter import RemoteHostFilter, RemoteHostFilterResult
from koukan.received_header_filter import ReceivedHeaderFilter, ReceivedHeaderFilterResult

# this should go last before exploder in the downstream chain
# need one for upstream chain?
class IngressPolicy(Filter):
    max_received_headers : int

    def __init__(self, max_received_headers : int = 30):
        self.max_received_headers = max_received_headers

    def on_update(self, tx_delta : TransactionMetadata):
        tx = self.downstream_tx
        assert tx is not None
        out = tx.filter_output
        assert out is not None
        if tx_delta.mail_from:
            rh = out.get(RemoteHostFilter.fullname(), None)
            if (rh is None) or (not isinstance(rh, RemoteHostFilterResult)):
                tx.mail_response = Response(
                    450, 'internal error: expected remote host filter result')
                return FilterResult()
            remote_host : RemoteHostFilterResult = rh
            if not remote_host.fcrdns:
                tx.mail_response = Response(550, 'fcrdns required')
                return FilterResult()
            if not remote_host.remote_hostname:
                tx.mail_response = Response(550, 'ptr error?')
                return FilterResult()

            if tx.smtp_meta is None or (
                    ehlo := tx.smtp_meta.get('ehlo_host', None)) is None:
                tx.mail_response = Response(
                    450, 'internal error: expected smtp meta ehlo')
                return FilterResult()

            # XXX case
            # XXX should RemoteHostFilter drop trailing dot from dns?
            if ehlo != remote_host.remote_hostname.rstrip('.'):
                tx.mail_response = Response(
                    550, 'ehlo must match remote hostname')
                return FilterResult()

        body = tx_delta.maybe_body_blob()
        if body and body.finalized():
            rec = tx.get_filter_output(ReceivedHeaderFilter.fullname())
            if rec is None or rec.received_header_count is None:
                logging.debug(rec)
                if rec:
                    logging.debug(rec.received_header_count)
                tx.data_response = Response(
                    450, 'internal error: expected received header filter result')
                return FilterResult()
            if rec.received_header_count > self.max_received_headers:
                tx.data_response = Response(
                    550, '5.4.6 message has too many received: '
                    'headers and is likely looping')
                return FilterResult()

        return FilterResult()


