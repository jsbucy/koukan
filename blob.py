from typing import List, Optional

class Blob:
    def len(self) -> int:
        raise NotImplementedError

    def id(self) -> Optional[str]:
        return None

    def unref(self, Any) -> None:
        return None

    def read(offset, len=None) -> bytes:
        pass

class InlineBlob(Blob):
    def __init__(self, d : bytes, id : Optional[str] = None):
        self.d = d
        self.blob_id = id

    def len(self): return len(self.d)

    def id(self): return self.blob_id

    def read(self, offset, len=None):
        return self.d[offset : offset + len if len is not None else None]

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
        print('chunk init %d %d %d' % (self.offset, self.blob_offset, length))

    def read(self, offset, length):
        print('read %d %d %d %s' % (self.offset, self.blob_offset, offset, length))
        offset -= self.offset
        offset += self.blob_offset
        if length is not None:
            length = min(length, self.length - offset + self.blob_offset)
        else:
            length = self.length
        print('read %d %s' % (offset, length))
        return self.blob.read(offset, length)

class CompositeBlob(Blob):
    chunks : List[Chunk]

    def __init__(self):
        self.chunks = []

    def append(self, blob, blob_offset, length):
        offset = 0
        if self.chunks:
            last = self.chunks[-1]
            offset = last.offset + last.length
        self.chunks.append(Chunk(blob, offset, blob_offset, length))

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
