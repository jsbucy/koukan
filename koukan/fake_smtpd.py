# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import List, Optional

from aiosmtpd.controller import Controller
from aiosmtpd.smtp import SMTP
from aiosmtpd.lmtp import LMTP

import asyncio
import time
import logging

from sys import argv

class InMemoryHandler:
    ehlo : Optional[str] = None
    mail_from : Optional[str] = None
    mail_options : Optional[List[str]] = None
    rcpt_to : List[str]
    rcpt_options : List[List[str]]
    data : Optional[bytes] = None
    protocol : str

    def __init__(self, protocol = 'smtp'):
        self.rcpt_to = []
        self.rcpt_options = []
        assert protocol in ['smtp', 'lmtp']
        self.protocol = protocol

    def __repr__(self):
        out = ''
        if self.ehlo:
            out += 'ehlo ' + self.ehlo + '\n'
        if self.mail_from:
            out += 'mail_from ' + self.mail_from + '\n'
        if self.mail_options:
            out += 'mail_options ' + str(self.mail_options) + '\n'
        if self.rcpt_to:
            out += 'rcpt_to ' + str(self.rcpt_to) + '\n'
        if self.rcpt_options:
            out += 'rcpt_options ' + str(self.rcpt_options) + '\n'
        if self.data:
            out += 'data ' + self.data.decode('utf-8') + '\n'
        return out

    # aiosmtpd wires this to HELO/EHLO or LHLO per protocol
    async def handle_EHLO(self, server, session, envelope, hostname, responses
                          ) -> list[str]:
        logging.debug('InMemoryHandler.handle_EHLO %s', hostname)
        self.ehlo = hostname
        session.host_name = hostname
        return responses

    async def handle_PROXY(self, server, session, envelope, proxy_data) -> bool:
        logging.debug('InMemoryHandler.handle_PROXY ', proxy_data)

        session.proxy_data = proxy_data
        return True

    async def handle_MAIL(self, server, session, envelope, address, options
                          ) -> str:
        self.mail_from = address
        self.mail_options = options
        logging.debug('InMemoryHandler.handle_MAIL %s %s', address, options)
        envelope.mail_from = address
        envelope.mail_options.extend(options)
        return '250 ok'

    async def handle_RCPT(self, server, session, envelope, address, options
                          ) -> str:
        self.rcpt_to.append(address)
        self.rcpt_options.append(options)

        logging.debug('InMemoryHandler.handle_RCPT %s %s',
                      address, options)
        if address.startswith('rcpttemp'):
            return '450 rcpt temp'
        elif address.startswith('rcptperm'):
            return '550 rcpt perm'
        elif address.startswith('rcpttimeout'):
            await asyncio.sleep(3600)

        envelope.rcpt_tos.append(address)
        return '250 OK'

    async def handle_DATA(self, server, session, envelope) -> str:
        self.data = envelope.content

        logging.debug('InMemoryHandler.handle_DATA %d bytes ',
                      len(envelope.content))

        if len(envelope.rcpt_tos) == 1:
            address = envelope.rcpt_tos[0]
            if address.startswith('datatemp'):
                return '450 data temp'
            elif address.startswith('dataperm'):
                return '550 data perm'
            elif address.startswith('datatimeout'):
                await asyncio.sleep(3600)

        for ln in envelope.content.decode('utf8', errors='replace').splitlines():
            logging.debug(f'> {ln}'.strip())
        print()
        print('End of message')
        return '250 Message accepted for delivery'

class FakeSmtpdController(Controller):
    def __init__(self, host, port, handler_factory, protocol : str):
        self.handler_factory = handler_factory
        self.protocol = protocol
        super(Controller, self).__init__(
            handler=None, hostname=host, port=port)
    def factory(self):
        handler = self.handler_factory()
        if self.protocol == 'smtp':
            return SMTP(handler)
        elif self.protocol == 'lmtp':
            return LMTP(handler)
        else:
            raise ValueError()

class FakeSmtpd:
    protocol : str
    def __init__(self, host : str, port : str,
                 protocol : str = 'smtp'):
        assert protocol in ['smtp', 'lmtp']
        self.protocol = protocol
        self.controller = FakeSmtpdController(
            host=host, port=port, handler_factory=self.handler_factory,
            protocol=protocol)
        self.handlers = []

    def handler_factory(self):
        handler = InMemoryHandler(self.protocol)
        self.handlers.append(handler)
        return handler

    def start(self):
        self.controller.start()
    def stop(self):
        self.controller.stop()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')
    smtpd = FakeSmtpd("localhost", argv[1])
    smtpd.start()
    time.sleep(1000000)
