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

    # returns True if rhs is a proper superset of self,
    # False if they are the same, else None
    def delta(self, rhs) -> Optional[bool]:
        raise NotImplementedError()

class WritableBlob(ABC):
    @abstractmethod
    def len(self) -> int:
        pass

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


    def append_blob(self, src : Blob, start : int = 0, length : int = 0,
                    set_content_length = True,
                    chunk_size : int = 2**16) -> int:
        if length == 0:
            length = src.content_length()
        off = start
        dstart = doff = self.len()
        while off < (length + start):
            left = ((start + length) - off)
            last = False
            if left < chunk_size:
                last = True
            else:
                left = chunk_size
            d = src.pread(off, left)
            content_length = None
            if set_content_length and last:
                content_length = off + len(d)
            appended, dlength, content_length_out = self.append_data(
                doff, d, content_length)
            if (not appended or dlength != (doff + len(d)) or
                content_length_out != content_length):
                return dlength - dstart
            off += (dlength - doff)
            doff = self.len()
        return (off - start)

# InlineBlob stores exactly one contiguous byte range aligned to the
# end of a possibly larger address space. It supports appending
# exactly at the current end and truncating from the beginning. This
# is used as a trivial FIFO buffer in SmtpHandler -> RestEndpoint for
# chunked uploads.
# e.g.
# b = InlineBlob('hello, ')
# b.pread(0) -> 'hello, '
# b.append('world!')
# b.pread(0) -> 'hello, world!'
# b.trim_front(7)
# b.pread(7) -> 'world!'
class InlineBlob(Blob, WritableBlob):
    d : bytes
    _content_length : Optional[int] = None
    _rest_id : Optional[str] = None
    _offset : int = 0

    def __init__(self, d : bytes,
                 content_length : Optional[int] = None,
                 rest_id : Optional[str] = None,
                 last = False):
        assert not (last and (content_length is not None))
        self.d = d
        self._content_length = len(d) if last else content_length
        self._rest_id = rest_id

    def delta(self, rhs) -> Optional[bool]:
        if not isinstance(rhs, InlineBlob):
            return None
        # XXX fix before merge
        # if not rhs.d.startswith(self.d):
        #     return None
        return rhs.d != self.d

    def len(self):
        return self._offset + len(self.d)

    def rest_id(self):
        return self._rest_id

    def pread(self, offset, len=None):
        if offset < self._offset:
            raise ValueError()
        offset -= self._offset
        return self.d[offset : offset + len if len is not None else None]

    def content_length(self):
        return self._content_length

    def append(self, dd : bytes, last : bool = False):
        # if not last:
        #     assert self._content_length is None
        self.d += dd
        assert self.content_length() is None or (self.len() <= self.content_length())
        if last:
            self._content_length = self._offset + len(self.d)

    def __repr__(self):
        return 'length=%d content_length=%s offset=%d' % (
            self.len(), self.content_length(), self._offset)

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

    # the semantics here are similar to fallocate(2) with FALLOC_FL_PUNCH_HOLE
    def trim_front(self, offset : int):
        if offset < self._offset:
            raise ValueError()
        if offset > self._offset + len(self.d):
            raise ValueError()
        off = offset - self._offset
        self.d = self.d[off:]
        self._offset = offset


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
        # TODO bisect
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
