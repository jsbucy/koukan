
from typing import Any, Callable, Dict, List, Optional

from blob import Blob, InlineBlob

class InflightBlob:
    d : bytes
    b : InlineBlob
    # pass blob id and len on done
    waiters : List[Callable[[str, int], None]]

# TODO needs ttl/gc
class BlobStorage:
    next = 0
    blobs : Dict[str, InflightBlob] = {}

    def __init__(self):
        pass

    def create(self, on_done : Callable[[Blob],None]) -> int:
        b = InflightBlob()
        b.d = bytes()
        b.waiters = [on_done]
        id = str(self.next)
        self.next += 1
        self.blobs[id] = b
        return id

    def append(self, id : int, offset : int, d : bytes, last) -> Optional[int]:
        if id not in self.blobs:
            return None
        blob = self.blobs[id]
        blob_len = len(blob.d)
        if offset > blob_len:
            return blob_len
        blob.d += d[offset - blob_len:]
        if last:
            blob.b = InlineBlob(blob.d, str(id))
            blob.d = None
            for cb in blob.waiters:
                cb(blob.b)
        return blob.b.len()

    def get(self, id) -> Blob:
        if not id in self.blobs or not self.blobs[id].b:
            return None
        return self.blobs[id].b

    def add_waiter(self, id, cb : Callable[[Blob],None]):
        if id not in self.blobs: return False
        b = self.blobs[id]
        if not b.b:
            b.waiters.append(cb)
        else:
            cb(b.b)
        return True
