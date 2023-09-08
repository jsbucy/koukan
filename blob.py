from typing import Optional

# eventually file-like object (read-only)
class Blob:
    def contents(self) -> bytes:
        pass

    def len(self) -> int:
        pass

    def id(self) -> Optional[str]:
        return None

class InlineBlob(Blob):
    def __init__(self, d : bytes):
        self.d = d

    def contents(self): return self.d

    def len(self): return len(self.d)
