from typing import Callable, List
import logging

from filter import Filter, Mailbox, TransactionMetadata
from blob import Blob
from response import Response

class Exploder(Filter):
    output_chain : str
    factory : Callable[[str], Filter]

    upstream_chain : List[Filter]
    upstream_tx = List[TransactionMetadata]
    rcpt_ok = False
    mail_from : Mailbox = None
    ok_rcpt = False
    ok_rcpts : List[bool]
    upstream_data_resp = List[Response]

    def __init__(self, output_chain, factory : Callable[[], Filter]):
        self.output_chain = output_chain
        self.factory = factory
        self.upstream_chain = []
        self.upstream_tx = []
        self.ok_rcpts = []
        self.upstream_data_resp = []

    def on_update(self, delta):
        logging.info('Exploder.on_update %s', delta.to_json())

        # because of the way SMTP works, you have to return a response
        # to MAIL FROM before you know who the RCPT is so if they
        # didn't pipeline those together, we just accept MAIL and hope
        # for the best
        # TODO possibly move this to a separate filter
        if delta.mail_from:
            self.mail_from = delta.mail_from
            if not delta.rcpt_to:
                delta.mail_response = Response(250)
                return

        upstream_tx = []
        for i,rcpt in enumerate(delta.rcpt_to):
            logging.info('Exploder.on_update rcpt %s', rcpt)

            # just send the envelope, we'll deal with any data below
            tx_i = TransactionMetadata(host = self.output_chain,
                                       mail_from = self.mail_from,
                                       rcpt_to = [rcpt])
            endpoint_i = self.factory()
            # fan this out if there were multiple though gw/smtplib
            # doesn't do pipelining so it will only send us one at
            # a time
            # XXX this needs to be able to time out
            endpoint_i.on_update(tx_i)
            rcpt_resp = tx_i.rcpt_response[0]
            logging.info('Exploder.on_updated %s %s',
                         tx_i.mail_response, tx_i.rcpt_response)

            self.upstream_chain.append(endpoint_i)
            upstream_tx.append(tx_i)

            if delta.mail_from and not delta.mail_response:
                delta.mail_response = tx_i.mail_response
            # hopefully rare
            if tx_i.mail_response.err():
                delta.rcpt_response.append(tx_i.mail_response)
                self.ok_rcpts.append(False)
                continue
            delta.rcpt_response.append(rcpt_resp)
            self.ok_rcpts.append(rcpt_resp.ok())
            if rcpt_resp.ok():
                self.ok_rcpt = True

    def append_data(self, last : bool, blob : Blob) -> Response:
        logging.info('Exploder.append_data %s', last)
        assert self.ok_rcpt
        if not self.upstream_data_resp:
            self.upstream_data_resp = len(self.ok_rcpts) * [None]
        for i,endpoint_i in enumerate(self.upstream_chain):
            logging.debug('Exploder.append_data %d %s %s', i,
                          self.ok_rcpts[i], self.upstream_data_resp[i])
            if (not self.ok_rcpts[i] or
                self.upstream_data_resp[i] is not None):
                continue
            resp = endpoint_i.append_data(last, blob)
            logging.info('Exploder.append_data %d %s', i, resp)
            if resp is not None:
                self.upstream_data_resp[i] = resp

        if not last:  # and any upstream_data_resp is None
            return None

        # if prdr
        #   return upstream_data_resp directly

        r0 = None
        for i,ok_rcpt in enumerate(self.ok_rcpts):
            if not ok_rcpt:
                continue
            data_resp = self.upstream_data_resp[i]
            if r0 is None:
                r0 = data_resp
                continue
            if data_resp.code/100 != r0.code/100:
                break
        else:
            return r0

        # set_durable()
        # return Response(250, 'accepted async mixed upstream responses')


    def abort(self):
        pass
