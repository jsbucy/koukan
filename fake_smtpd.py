
from aiosmtpd.controller import Controller
import asyncio
import time
import logging

from sys import argv

class InMemoryHandler:
    async def handle_EHLO(self, server, session, envelope, hostname, responses
                          ) -> list[str]:
        session.host_name = hostname
        logging.debug('InMemoryHandler.handle_EHLO %s', hostname)
        return responses

    async def handle_PROXY(self, server, session, envelope, proxy_data) -> bool:
        logging.debug('InMemoryHandler.handle_PROXY ', proxy_data)

        session.proxy_data = proxy_data
        return True

    async def handle_MAIL(self, server, session, envelope, address, options
                          ) -> str:
        logging.debug('InMemoryHandler.handle_MAIL %s %s', address, options)
        envelope.mail_from = address
        envelope.mail_options.extend(options)
        return '250 ok'

    async def handle_RCPT(self, server, session, envelope, address, options
                          ) -> str:
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
