
import unittest
import logging
from koukan.filter import Mailbox
from koukan.response import Response
from koukan.filter_chain import (
    Filter,
    FilterChain,
    ProxyFilter )

from koukan.filter import TransactionMetadata

class Sink(Filter):
    async def on_update(self, delta, upstream):
        logging.debug('Sink.update %s', self.downstream)
        logging.debug(delta)
        # self.downstream['sink'] = 'sink'
        assert delta.mail_response is None
        if delta.mail_from:
            self.downstream.mail_response = Response(201)
        for i in range(len(self.downstream.rcpt_response), len(self.downstream.rcpt_to)):
            self.downstream.rcpt_response.append(Response(202))

class AddDownstream(Filter):
    async def on_update(self, delta, upstream):
        logging.debug('AddDownstream.start')
        # self.downstream['downstream'] = 'downstream'
        await upstream()
        logging.debug('AddDownstream.done')

class AddUpstream(Filter):
    async def on_update(self, delta, upstream):
        logging.debug('AddUpstream.start')
        await upstream()
        # self.downstream['upstream'] = 'upstream'
        logging.debug('AddUpstream.done')

class Proxy(ProxyFilter):
    async def on_update(self, delta, upstream):
        logging.debug(self.downstream)
        logging.debug(delta)
        logging.debug(self.upstream)
        assert self.upstream.merge_from(delta) is not None
        # self.upstream['proxy_downstream'] = 'x'
        delta = await upstream()
        assert self.downstream.merge_from(delta) is not None
        # self.downstream['proxy_upstream'] = 'y'



class FilterChainTest(unittest.TestCase):
    def test_smoke(self):
        tx = TransactionMetadata()
        sink = Sink()
        chain = FilterChain(
            [AddDownstream(),
             Proxy(), AddUpstream(),
             sink])
        chain.init(tx)
        tx.mail_from = Mailbox('alice')
        chain.update()
        logging.debug(tx)
        logging.debug(sink.downstream)
        #self.assertEqual('alice', sink.downstream.mail_from.mailbox)
        self.assertEqual(201, tx.mail_response.code)

        tx.rcpt_to.append(Mailbox('bob'))
        chain.update()
        self.assertEqual([202], [r.code for r in tx.rcpt_response])

        tx.rcpt_to.append(Mailbox('bob2'))
        chain.update()
        self.assertEqual([202, 202], [r.code for r in tx.rcpt_response])

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
        '%(message)s')
    unittest.main()
