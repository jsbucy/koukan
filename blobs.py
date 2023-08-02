
from typing import Any, Callable, Dict, List, Optional


class Blob:
    d : bytes
    done = False
    # pass blob id and len on done
    waiters : List[Callable[[str, int], None]]

# TODO needs ttl/gc
class BlobStorage:
    next = 0
    blobs : Dict[str, Blob] = {}

    def __init__(self):
        pass

    def create(self, on_done : Callable[[str, int],None]):  #-> id
        b = Blob()
        b.d = bytes()
        b.waiters = [on_done]
        id = str(self.next)
        self.next += 1
        self.blobs[id] = b
        return id

    def append(self, id, offset : int, d : bytes, last) -> int:
        assert(id in self.blobs)
        blob = self.blobs[id]
        blob_len = len(blob.d)
        if offset > blob_len:
            return blob_len
        blob.d += d[offset - blob_len:]
        if last:
            blob.done = True
            for cb in blob.waiters:
                cb(id, len(blob.d))
        return len(blob.d)

    def get(self, id):
        return self.blobs[id] if id in self.blobs else None

    def add_waiter(self, id, cb : Callable[[str, int],None]):
        if id not in self.blobs: return False
        b = self.blobs[id]
        if b.done:
            cb(id, len(b.d))
        b.waiters.append(cb)
        return True


class BlobDerefEndpoint:
    def __init__(self, blobs, next):
        self.blobs = blobs
        self.next = next

    def start(self,
              local_host, remote_host,
              mail_from, transaction_esmtp,
              rcpt_to, rcpt_esmtp):
        return self.next.start(
            local_host, remote_host,
            mail_from, transaction_esmtp, rcpt_to, rcpt_esmtp)

    def append_data(self,
                    last : bool,
                    d : Optional[bytes] = None,
                    blob_id : Optional[Any] = None):
        print('BlobDerefEndpoint.append_data ', last, d, blob_id)
        if blob_id:
            blob = self.blobs.get(blob_id)
            assert(blob)
            d = blob.d
        return self.next.append_data(last=last, d=d, blob_id=None)

    def get_status(self):
        return self.next.get_status()
