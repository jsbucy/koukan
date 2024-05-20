from typing import Any, Callable, Dict, List, Optional, Tuple
import time
import logging
from threading import Lock

from blob import Blob, BlobStorage, InlineBlob, WritableBlob

class InflightBlob(WritableBlob):
    d : bytes
    b : Optional[InlineBlob] = None
    parent = None
    last_update = None
    content_length : Optional[int] = None

    def __init__(self, id, parent):
        self.refs = set()
        self.id = id
        self.parent = parent
        self.d = bytes()
        self.ping()

    def ping(self):
        self.last_update = time.monotonic()

    # WritableBlob

    def append_data(self, offset : int, d : bytes, content_length=None
                    ) -> Tuple[bool, int, Optional[int]]:
        assert content_length is None or (
            content_length == (offset + len(d)))
        if offset != len(self.d):
            return False, len(self.d), self.content_length
        if (self.content_length is not None) and (
                (content_length is None) or
                (content_length != self.content_length)):
            return False, len(self.d), self.content_length
        self.content_length = content_length

        self.d += d
        if self.content_length is None or len(self.d) != content_length:
            return True, len(self.d), self.content_length

        self.b = InlineBlob(self.d, None, str(id))
        self.d = None
        return True, self.b.len(), self.content_length

class InMemoryBlobStorage(BlobStorage):
    blobs : Dict[str, InflightBlob]
    mu : Lock

    def __init__(self):
        self.blobs = {}
        self.mu = Lock()

    def _create(self, rest_id : str) -> Optional[InflightBlob]:
        with self.mu:
            if rest_id in self.blobs:
                return None
            b = InflightBlob(rest_id, self)
            self.blobs[rest_id] = b
        return b

    def _get(self, id) -> Optional[InflightBlob]:
        if not (inflight_blob := self.blobs.get(id, None)):
            return None
        inflight_blob.ping()
        return inflight_blob

    def gc(self, ttl):
        for (k,v) in [(k,v) for (k,v) in self.blobs.items()]:
            if (time.monotonic() - v.last_update) < ttl:
                continue
            del self.blobs[k]

    # BlobStorage
    def create(self, rest_id : str) -> Optional[WritableBlob]:
        return self._create(rest_id)

    # BlobStorage
    def get_for_append(self, rest_id) -> Optional[WritableBlob]:
        return self._get(rest_id)

    # BlobStorage
    def get_finalized(self, rest_id) -> Optional[Blob]:
        inflight = self._get(rest_id)
        if inflight is None:
            return None
        return inflight.b
