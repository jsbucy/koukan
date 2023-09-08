
from aiosmtpd.controller import Controller
import asyncio
import time
import logging

import requests

from sys import argv

class InMemoryHandler:
    async def handle_EHLO(self, server, session, envelope, hostname, responses) -> list[str]:
        session.host_name = hostname
        print(responses)
        return responses

    async def handle_PROXY(self, server, session, envelope, proxy_data) -> bool:
        session.proxy_data = proxy_data
        return True

    async def handle_MAIL(self, server, session, envelope, address, options) -> str:
        envelope.mail_from = address
        envelope.mail_options.extend(options)
        return '250 ok'

    async def handle_RCPT(self, server, session, envelope, address, rcpt_options) -> str:
        if address.startswith('rcpttemp@'):
            return b'450 rcpt temp'
        elif address.startswith('rcptperm@'):
            return b'550 rcpt perm'
        envelope.rcpt_tos.append(address)
        return '250 OK'

    async def handle_DATA(self, server, session, envelope) -> str:
        if len(envelope.rcpt_tos) == 1:
            address = envelope.rcpt_tos[0]
            if address.startswith('datatemp@'):
                return b'450 data temp'
            elif address.startswith('dataperm@'):
                return b'550 data perm'

        print('proxy data ', session.proxy_data)
        print('Message from %s' % envelope.mail_from)
        print('Message for %s' % envelope.rcpt_tos)
        print('Message data:\n')
        for ln in envelope.content.decode('utf8', errors='replace').splitlines():
            print(f'> {ln}'.strip())
        print()
        print('End of message')
        return '250 Message accepted for delivery'

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(message)s')

from aiosmtpd.controller import Controller
controller = Controller(InMemoryHandler(), hostname="localhost", port=argv[1])
# proxy_protocol_timeout=30)
controller.start()
time.sleep(1000000)
