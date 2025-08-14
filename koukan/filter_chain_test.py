
import unittest
import logging
from koukan.filter import Mailbox
from koukan.response import Response
from koukan.filter_chain import (
    CoroutineFilter,
    CoroutineProxyFilter,
    FilterChain,
    FilterResult,
    OneshotFilter,
    OneshotProxyFilter )
from koukan.blob import InlineBlob
from koukan.filter import TransactionMetadata
import asyncio

class Sink(CoroutineFilter):
    async def on_update(self, delta, upstream):
        logging.debug('Sink.on_update %s', self.downstream)
        logging.debug(delta)
        # self.downstream['sink'] = 'sink'
        assert delta.mail_response is None
        if delta.mail_from:
            self.downstream.mail_response = Response(201)
        for i in range(len(self.downstream.rcpt_response), len(self.downstream.rcpt_to)):
            self.downstream.rcpt_response.append(Response(202))

class AddDownstream(CoroutineFilter):
    async def on_update(self, delta, upstream):
        logging.debug('AddDownstream.start')
        # self.downstream['downstream'] = 'downstream'
        await upstream()
        logging.debug('AddDownstream.done')

class AddUpstream(CoroutineFilter):
    async def on_update(self, delta, upstream):
        logging.debug('AddUpstream.start')
        await upstream()
        # self.downstream['upstream'] = 'upstream'
        logging.debug('AddUpstream.done')

class Proxy(CoroutineProxyFilter):
    async def on_update(self, delta, upstream):
        logging.debug(self.downstream)
        logging.debug(delta)
        logging.debug(self.upstream)
        self.upstream.merge_from(delta)
        # self.upstream['proxy_downstream'] = 'x'
        delta = await upstream()
        self.downstream.merge_from(delta)
        # self.downstream['proxy_upstream'] = 'y'


class OneshotProxyDownstream(OneshotProxyFilter):
    def on_update(self, delta):
        body = delta.body
        delta.body = None
        self.upstream.merge_from(delta)
        return FilterResult(TransactionMetadata(data_response=Response(501)))

class OneshotProxyDownstreamNone(OneshotProxyFilter):
    def on_update(self, delta):
        body = delta.body
        delta.body = None
        self.upstream.merge_from(delta)
        return FilterResult()

class RejectMail(OneshotFilter):
    def on_update(self, delta):
        logging.debug('RejectMail.on_update')
        if delta.mail_from:
            assert self.downstream.mail_response is None
            self.downstream.mail_response = Response(550, 'bad')
        return FilterResult()

class FilterChainTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def test_smoke(self):
        tx = TransactionMetadata()
        sink = Sink()
        chain = FilterChain(
            [AddDownstream(),
             Proxy(), AddUpstream(),
             sink],
            self.loop)
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

    def test_filter_result(self):
        tx = TransactionMetadata()
        sink = Sink()
        chain = FilterChain([OneshotProxyDownstream(), sink],
                            self.loop)
        chain.init(tx)
        delta = TransactionMetadata(
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob')],
            body = InlineBlob(b'hello, world!', last=True))
        tx.merge_from(delta)
        chain.update()
        self.assertEqual(201, tx.mail_response.code)
        self.assertEqual(501, tx.data_response.code)

    def test_filter_result_none(self):
        tx = TransactionMetadata()
        sink = Sink()
        chain = FilterChain([OneshotProxyDownstreamNone(), sink],
                            self.loop)
        chain.init(tx)
        delta = TransactionMetadata(
            mail_from = Mailbox('alice'))
        tx.merge_from(delta)
        chain.update()
        self.assertEqual(201, tx.mail_response.code)

    def test_fail_mail(self):
        tx = TransactionMetadata()
        sink = Sink()
        chain = FilterChain([RejectMail(), sink],
                            self.loop)
        chain.init(tx)
        delta = TransactionMetadata(
            mail_from = Mailbox('alice'),
            rcpt_to = [Mailbox('bob1'), Mailbox('bob2')])
        tx.merge_from(delta)
        chain.update()
        self.assertEqual(550, tx.mail_response.code)
        self.assertEqual([503,503], [r.code for r in tx.rcpt_response])
        tx.cancelled = True
        chain.update()

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d '
        '%(message)s')
    unittest.main()
