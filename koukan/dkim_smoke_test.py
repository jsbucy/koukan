
import koukan.fake_endpoints as fake_endpoints
import koukan.dkim_endpoint as dkim_endpoint

p = fake_endpoints.PrintEndpoint()
d = dkim_endpoint.DkimEndpoint(b'example.com', b'selector', 'dk.key', p)
d.start(None, None,
        'alice@example.com', None, 'bob@example.com', None)
d.append_data(d = b'Received: from somewhere\r\n', last=False)
d.append_data(last=True,
              d=b'Subject: hello\r\n\r\nworld!\r\n')
d.get_status()
