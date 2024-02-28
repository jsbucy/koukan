from typing import List, Optional
from abc import ABC, abstractmethod

class Blob(ABC):
    @abstractmethod
    def len(self) -> int:
        pass

    # TODO this id is vestigal?
    def id(self) -> Optional[str]:
        return None

    def unref(self, Any) -> None:
        return None

    @abstractmethod
    def read(offset, len=None) -> bytes:
        # pytype doesn't flag len() (above) but does flag this?!
        raise NotImplementedError()

    @abstractmethod
    def content_length(self) -> Optional[int]:
        pass

class InlineBlob(Blob):
    d : bytes
    _content_length : Optional[int] = None

    # TODO this id is vestigal?
    def __init__(self, d : bytes,
                 content_length : Optional[int] = None,
                 id : Optional[str] = None):
        self.d = d
        self._content_length = content_length
        self.blob_id = id

    def len(self):
        return len(self.d)

    def id(self):
        return self.blob_id

    def read(self, offset, len=None):
        return self.d[offset : offset + len if len is not None else None]

    def content_length(self):
        return self._content_length if self._content_length is not None else len(self.d)

    def append(self, dd : bytes):
        self.d += dd
        assert self.len() <= self.content_length()

class Chunk:
    # byte offset in CompositeBlob
    offset : int
    # offset of this view into self.blob
    blob_offset : int
    length : int
    blob : Blob

    def __init__(self, blob, offset, blob_offset, length):
        self.blob = blob
        self.offset = offset
        self.blob_offset = blob_offset
        self.length = length

    def read(self, offset, length):
        offset -= self.offset
        offset += self.blob_offset
        if length is not None:
            length = min(length, self.length - offset + self.blob_offset)
        else:
            length = self.length
        return self.blob.read(offset, length)

class CompositeBlob(Blob):
    chunks : List[Chunk]
    last = False

    def __init__(self):
        self.chunks = []

    def append(self, blob, blob_offset, length, last : Optional[bool] = False):
        assert not self.last
        if last:
            self.last = True
        offset = 0
        if self.chunks:
            last_chunk = self.chunks[-1]
            offset = last_chunk.offset + last_chunk.length
        self.chunks.append(Chunk(blob, offset, blob_offset, length))

        # TODO coalesce contiguous ranges into the same blob
        # TODO for bonus points, if isinstance(blob, CompositeBlob)
        # copy the chunks directly

    def read(self, offset, length=None) -> bytes:
        out = bytes()
        for chunk in self.chunks:
            if offset > (chunk.offset + chunk.length):
                continue
            if length is not None and ((offset + length) < chunk.offset):
                break

            d = chunk.read(offset, length)
            out += d
            offset += len(d)
            if length:
                length -= len(d)
                if length == 0:
                    break
        return out

    def len(self):
        if not self.chunks:
            return 0
        last_chunk = self.chunks[-1]
        return last_chunk.offset + last_chunk.length

    def content_length(self):
        if not self.last:
            return None
        return self.len()
