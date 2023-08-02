
import rest_service

from rest_endpoint import RestEndpoint

from fake_endpoints import PrintEndpoint

from threading import Thread

from blobs import BlobStorage, BlobDerefEndpoint

import time


blobs = BlobStorage()

def endpoints(host):
    return BlobDerefEndpoint(blobs, PrintEndpoint())


def rest_service_main():
    print('starting rest service')
    app = rest_service.create_app(endpoints, blobs)
    app.run('localhost', 9999)

rest_service_thread = Thread(target=lambda: rest_service_main(),
                             daemon=False)
rest_service_thread.start()
time.sleep(0.1)
print('started')

endpoint = RestEndpoint('http://localhost:9999', http_host='inbound-gw')
endpoint.start(None, None,
               'alice@origin.com', None,
               'bob@recipient.com', None)

resp, uri = endpoint.append_data(False,
                                 b'bcc: bob\r\n'
                                 b'subject: hello\r\n\r\n')
resp, uri = endpoint.append_data(True, b'\xde\xad\xbe\xef')
print('bob chunk 2 uri', uri)

endpoint.start(None, None,
               'alice@origin.com', None,
               'carol@recipient.com', None)

resp, _ = endpoint.append_data(False,
                                 b'bcc: carol\r\n'
                                 b'subject: hello\r\n\r\n')
resp, uri = endpoint.append_data(True, d=None, blob_id=uri)
