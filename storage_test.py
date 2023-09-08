
from storage import Storage, Action

s = Storage()
s.connect(db=Storage.get_inmemory_for_test())

writer = s.get_transaction_writer()
writer.start('local_host', 'remote_host', 'alice', None, 'bob', None, 'host')
writer.append_data('abc')
writer.append_data('xyz')
print(writer.append_blob('blob_id'))

blob_writer = s.get_blob_writer()
print(blob_writer.start())
blob_writer.append_data('blob1')
blob_writer.append_data('blob2')
blob_writer.finalize()

print(writer.append_blob('blob_id'))
writer.append_data('qrs')

writer.finalize()

reader = s.load_one()
print(reader.id)
print(reader.mail_from, reader.rcpt_to)
while d := reader.read_content():
    print(d)

r2 = s.load_one()
assert(r2 is None)

s.append_transaction_actions(reader.id, Action.TEMP_FAIL)

r2 = s.load_one()
assert(r2 is not None)
print(r2.id)

s.append_transaction_actions(r2.id, Action.DELIVERED)

r2 = s.load_one()
assert(r2 is None)

for l in s.db.iterdump():
    print(l)
