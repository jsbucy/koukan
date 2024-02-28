
from aiosmtpd.controller import Controller
import asyncio
import time
import logging

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
        if address.startswith('rcpttemp'):
            return '450 rcpt temp'
        elif address.startswith('rcptperm'):
            return '550 rcpt perm'
        elif address.startswith('rcpttimeout'):
            await asyncio.sleep(3600)

        envelope.rcpt_tos.append(address)
        return '250 OK'

    async def handle_DATA(self, server, session, envelope) -> str:
        if len(envelope.rcpt_tos) == 1:
            address = envelope.rcpt_tos[0]
            if address.startswith('datatemp'):
                return '450 data temp'
            elif address.startswith('dataperm'):
                return '550 data perm'
            elif address.startswith('datatimeout'):
                await asyncio.sleep(3600)

        print('proxy data ', session.proxy_data)
        print('Message from %s' % envelope.mail_from)
        print('Message for %s' % envelope.rcpt_tos)
        print('Message data:\n')
        for ln in envelope.content.decode('utf8', errors='replace').splitlines():
            print(f'> {ln}'.strip())
        print()
        print('End of message')
        return '250 Message accepted for delivery'

class FakeSmtpd:
    def __init__(self, port):
        self.controller = Controller(InMemoryHandler(), hostname="localhost",
                                     port=port)
        # proxy_protocol_timeout=30)

    def start(self):
        self.controller.start()
    def stop(self):
        self.controller.stop()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')
    smtpd = FakeSmtpd(argv[1])
    smtpd.start()
    time.sleep(1000000)
