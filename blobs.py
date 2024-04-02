
from typing import Any, Callable, Dict, List, Optional
import time

from blob import Blob, InlineBlob

class InflightBlob:
    d : bytes
    b : InlineBlob
    parent = None
    last_update = None

    def __init__(self, id, parent):
        self.refs = set()
        self.id = id
        self.parent = parent
        self.ping()

    def ping(self):
        self.last_update = time.monotonic()

# TODO this should just use a temp file for now
class BlobStorage:
    next = 0
    blobs : Dict[str, InflightBlob] = {}

    def __init__(self):
        pass

    def create(self, on_done : Optional[Callable[[Blob],None]] = None) -> str:
        id = str(self.next)
        self.next += 1

        b = InflightBlob(id, self)
        b.d = bytes()
        self.blobs[id] = b
        return id

    def append(self, id : str, offset : int, d : bytes, last) -> Optional[int]:
        if not (blob := self.blobs.get(id, None)):
            return None
        blob.ping()
        blob_len = len(blob.d)

        if offset != blob_len:
            return blob_len
        blob.d += d
        if not last:
            return len(blob.d)

        blob.b = InlineBlob(blob.d, None, str(id))
        blob.d = None
        return blob.b.len()

    def get(self, id) -> Optional[Blob]:
        if not (inflight_blob := self.blobs.get(id, None)):
            return None
        inflight_blob.ping()
        return inflight_blob.b

    def gc(self, ttl):
        for (k,v) in [(k,v) for (k,v) in self.blobs.items()]:
            if (time.monotonic() - v.last_update) < ttl:
                continue
            del self.blobs[k]
