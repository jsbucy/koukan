from aiosmtpd.controller import Controller
import asyncio
import time
import logging

import requests

from typing import Optional
from typing import Tuple

from response import ok_resp, to_smtp_resp

class SmtpHandler:
    def __init__(self, endpoint_factory):
        self.endpoint_factory = endpoint_factory

    async def handle_EHLO(
            self, server, session, envelope, hostname, responses):
        # TODO no hook for connect, can override SMTP.connection_made()
        if not hasattr(session, "connection"):
            session.endpoint = self.endpoint_factory()
            greeting = session.endpoint.on_connect(session.peer, None)
            if greeting.err():
                return [greeting.to_smtp_resp()]
        connection = session.endpoint
        ehlo_resp, esmtp = connection.on_ehlo(hostname)
        if ehlo_resp.err():
            return [ehlo_resp.to_smtp_resp()]

        session.host_name = hostname
        # TODO esmtp
        return responses

    async def handle_MAIL(
            self, server, session, envelope, address, options):
        connection = session.endpoint
        resp, _ = connection.start_transaction(
            address, options, forward_path=[])
        if resp.ok():
            envelope.mail_from = address
            envelope.mail_options.extend(options)
        print(resp)
        return resp.to_smtp_resp()


    async def handle_RCPT(
            self, server, session, envelope, address, rcpt_options):
        resp = session.endpoint.add_rcpt(address, rcpt_options)
        if resp.ok():
            envelope.rcpt_tos.append(address)
        return resp.to_smtp_resp()

    async def handle_DATA(self, server, session, envelope):
        # no hook for data chunks but could override handler in SMTP
        # object to get at it

        data_resp, chunk_id = session.endpoint.append_data(last=True)
        if data_resp.err():
            return data_resp.to_smtp_resp()
        chunk_resp,result_len = session.endpoint.append_data_chunk(
            chunk_id, offset=0,
            d=envelope.content, last=True)
        if chunk_resp.err():
            return chunk_resp.to_smtp_resp()
        elif result_len < len(envelope.content):
            return Resposne(400, "didn't put all data")
        return session.endpoint.get_transaction_status().to_smtp_resp()


def service(endpoint, hostname="localhost", port=9025):
    logging.basicConfig(level=logging.DEBUG)

    from aiosmtpd.controller import Controller
    controller = Controller(SmtpHandler(endpoint),
                            hostname=hostname, port=port)
    controller.start()
    time.sleep(1000000)
