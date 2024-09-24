# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional
import time

class Deadline:
    deadline : Optional[float] = None
    def __init__(self, timeout : Optional[float] = None):
        if timeout is not None:
            self.deadline = time.monotonic() + timeout
    def deadline_left(self) -> Optional[float]:
        if self.deadline is None:
            return None
        r = self.deadline - time.monotonic()
        return r

    # is deadline_left() >= x (or None)
    def remaining(self, x : float = 0.0) -> bool:
        return self.deadline is None or (self.deadline_left() >= x)
