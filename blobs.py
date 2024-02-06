
from typing import Any, Callable, Dict, List, Optional

from blob import Blob, InlineBlob

class InflightBlob:
    d : bytes
    b : InlineBlob
    waiters : List[Callable[[Blob], None]]
    parent = None

    refs : set[Any]

    def __init__(self, id, parent):
        self.refs = set()
        self.id = id
        self.parent = parent

    def ref(self, x):
        assert(x not in self.refs)
        self.refs.add(x)

    def unref(self, x):
        assert(x in self.refs)
        self.refs.remove(x)
        if not self.refs:
            self.parent.unref(id, self)

# TODO needs idle ttl/gc
# TODO this should just use a temp file for now
class BlobStorage:
    next = 0
    blobs : Dict[str, InflightBlob] = {}

    def __init__(self):
        pass

    def create(self, on_done : Callable[[Blob],None]) -> str:
        id = str(self.next)
        self.next += 1

        b = InflightBlob(id, self)
        b.d = bytes()
        b.waiters = [on_done]
        self.blobs[id] = b
        return id

    def append(self, id : str, offset : int, d : bytes, last) -> Optional[int]:
        if id not in self.blobs:
            return None
        blob = self.blobs[id]
        blob_len = len(blob.d)
        if offset > blob_len:
            return blob_len
        blob.d += d[offset - blob_len:]
        if not last:
            return len(blob.d)

        blob.b = InlineBlob(blob.d, str(id))
        blob.d = None
        for cb in blob.waiters:
            cb(blob.b)
        return blob.b.len()

    def get(self, id) -> Optional[Blob]:
        if not id in self.blobs or not self.blobs[id].b:
            return None
        return self.blobs[id].b

    def add_waiter(self, id, ref, cb : Callable[[Blob],None]):
        if id not in self.blobs: return False
        b = self.blobs[id]
        b.ref(ref)
        if not b.b:
            b.waiters.append(cb)
        else:
            cb(b.b)
        return True

    def unref(self, id, b : InflightBlob):
        assert(id in self.blobs)
        assert(self.blobs[id] == b)
        del self.blobs[id]
