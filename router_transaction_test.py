
from router_transaction import RouterTransaction, BlobIdMap
from blobs import BlobStorage
from fake_endpoints import PrintEndpoint

from storage import Storage

storage = Storage()
storage.connect(db=Storage.get_inmemory_for_test())
blobs = BlobStorage()
blob_id_map = BlobIdMap()

print_endpoint = PrintEndpoint()

t1 = RouterTransaction(
    storage, blob_id_map, blobs, lambda: print_endpoint)

t1.start('local_host', 'remote_host',
        'alice@example.com', None,
        'bob@example.com', None)

t1.append_data(last=False, d=b'Received: ...for bob')

blob_id = blobs.create(lambda s,i: None)
blobs.append(blob_id, 0, b'qrs', last=True)
t1.append_data(last=True, blob_id=blob_id)

transaction_id = t1.set_durable()

t2 = RouterTransaction(
    storage, blob_id_map, blobs, lambda: print_endpoint)

t2.start('local_host', 'remote_host',
        'alice@example.com', None,
        'carol@example.com', None)
t2.append_data(last=False, d=b'Received: ...for carol')
t2.append_data(last=True, blob_id=blob_id)
t2.set_durable()


reader = storage.get_transaction_reader()
reader.start(transaction_id)
print(reader.mail_from)
while b := reader.read_content():
    print(b)

for l in storage.db.iterdump():
    print(l)
