from typing import Optional

# eventually file-like object (read-only)
class Blob:
    def contents(self) -> bytes:
        raise NotImplementedError

    def len(self) -> int:
        raise NotImplementedError

    def id(self) -> Optional[str]:
        return None

    def unref(self, Any) -> None:
        return None

class InlineBlob(Blob):
    def __init__(self, d : bytes, id : Optional[str] = None):
        self.d = d
        self.blob_id = id

    def contents(self): return self.d

    def len(self): return len(self.d)

    def id(self): return self.blob_id
