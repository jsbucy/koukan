# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import List, Optional, Tuple
from abc import ABC, abstractmethod
import os
from io import IOBase
import os

class Blob(ABC):
    @abstractmethod
    def len(self) -> int:
        pass

    def rest_id(self) -> Optional[str]:
        return None

    def unref(self, Any) -> None:
        return None

    @abstractmethod
    def pread(self, offset, len=None) -> Optional[bytes]:
        # pytype doesn't flag len() (above) but does flag this?!
        raise NotImplementedError()

    @abstractmethod
    def content_length(self) -> Optional[int]:
        pass

    def finalized(self):
        cl = self.content_length()
        return cl is not None and cl == self.len()


class WritableBlob(ABC):
    # write at offset which must be the current end
    # bool: whether offset was correct/write was applied, resulting range
    @abstractmethod
    def append_data(self, offset : int, d : bytes,
                    content_length : Optional[int] = None
                    ) -> Tuple[bool, int, Optional[int]]:
        pass

    def rest_id(self) -> Optional[str]:
        return None

    # returns session uri if different from this process
    def session_uri(self) -> Optional[str]:
        return None


class InlineBlob(Blob, WritableBlob):
    d : bytes
    _content_length : Optional[int] = None
    _rest_id : Optional[str] = None

    def __init__(self, d : bytes,
                 content_length : Optional[int] = None,
                 rest_id : Optional[str] = None,
                 last = False):
        assert not (last and (content_length is not None))
        self.d = d
        self._content_length = len(d) if last else content_length
        self._rest_id = rest_id

    def len(self):
        return len(self.d)

    def rest_id(self):
        return self._rest_id

    def pread(self, offset, len=None):
        return self.d[offset : offset + len if len is not None else None]

    def content_length(self):
        return self._content_length

    def append(self, dd : bytes):
        self.d += dd
        assert self.len() <= self.content_length()

    def __repr__(self):
        return 'length=%d content_length=%s' % (
            self.len(), self.content_length())

    def append_data(self, offset : int, d : bytes,
                    content_length : Optional[int] = None
                    ) -> Tuple[bool, int, Optional[int]]:
        req = content_length is not None
        upstream = self._content_length is not None
        if (offset != len(self.d) or
            (upstream and not req) or
            (upstream and req and (self._content_length != content_length))):
            return False, len(self.d), self._content_length
        self.d += d
        if self._content_length is None:
            self._content_length = content_length
        return True, len(self.d), content_length

# already finalized
class FileLikeBlob(Blob, WritableBlob):
    _content_length : Optional[int] = None
    f : IOBase
    _rest_id : Optional[str] = None

    def __init__(self, f : IOBase,
                 rest_id : Optional[str] = None,
                 finalized = False):
        self.f = f
        self._rest_id = rest_id
        if finalized:
            stat = os.stat(f.fileno())
            self._len = stat.st_size
            self._content_length = self._len

    def rest_id(self):
        return self._rest_id

    def pread(self, offset, len=None) -> bytes:
        self.f.seek(offset)
        return self.f.read(len)

    def content_length(self) -> Optional[int]:
        return self._content_length

    def len(self) -> int:
        if self._content_length is not None:
            return self._content_length
        return self.f.tell()

    def append_data(self, offset : int, d : bytes,
                    content_length : Optional[int] = None
                    ) -> Tuple[bool, int, Optional[int]]:
        if offset != self.f.tell():
            raise ValueError
        self.f.write(d)
        finalized = False
        if content_length is not None:
            assert self.f.tell() <= content_length
            assert (self._content_length is None or
                    self._content_length == content_length)
            self._content_length = content_length
            finalized = True
        return finalized, self.f.tell(), self._content_length

    def __del__(self):
        if self.f:
            self.f.close()
        self.f = None

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

    def pread(self, offset, length):
        offset -= self.offset
        offset += self.blob_offset
        if length is not None:
            length = min(length, self.length - offset + self.blob_offset)
        else:
            length = self.length
        return self.blob.pread(offset, length)

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

    def pread(self, offset, length=None) -> bytes:
        out = bytes()
        for chunk in self.chunks:
            if offset > (chunk.offset + chunk.length):
                continue
            if length is not None and ((offset + length) < chunk.offset):
                break

            d = chunk.pread(offset, length)
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


class BlobReader(IOBase):
    blob : Blob
    offset : int =  0

    def __init__(self, blob : Blob):
        self.blob = blob

    def read(self, req_len : Optional[int] = None):
        rv = self.blob.pread(self.offset, req_len)
        if rv:
            self.offset += len(rv)
        return rv

    def tell(self):
        return self.offset

    def seek(self, offset : int, whence=os.SEEK_SET):
        if whence == os.SEEK_SET:
            rel = 0
        elif whence == os.SEEK_CUR:
            rel = self.offset
        elif whence == os.SEEK_END:
            rel = self.blob.len()
        else:
            raise ValueError()
        if rel + offset < 0 or rel + offset > self.blob.len():
            raise ValueError()
        self.offset = rel + offset
        return self.offset
