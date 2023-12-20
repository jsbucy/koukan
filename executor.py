from threading import Lock, Condition, Thread

from typing import Dict

class Executor:
    def __init__(self, inflight_limit, queue_limits : Dict[int, int]):
        self.queue_limits = queue_limits
        self.queues = { tag:[] for tag in queue_limits }
        self.tags = sorted(queue_limits.keys(), reverse=True)

        self.inflight = 0
        self.inflight_limit = inflight_limit

        self.lock = Lock()
        self.cv = Condition(self.lock)

    # needs a timeout: None -> wait forever
    def enqueue(self, tag, fn) -> bool:
        with self.lock:
            if self.inflight < self.inflight_limit:
                self.inflight += 1
                self.detach(fn)
                return True
            else:
                self.cv.wait_for(lambda: len(self.queues[tag]) < self.queue_limits[tag])
                self.queues[tag].append(fn)
                return True

    def detach(self, fn):
        # XXX don't want daemon here? swallows exceptions?
        t = Thread(target=lambda: self.run(fn), daemon=True)
        t.start()

    # TODO add watchdog timer here
    def run(self, fn):
        try:
            fn()
        except:
            pass
        with self.lock:
            self.inflight -= 1
            self.cv.notify()

    def empty(self):
        for tag,queue in self.queues.items():
            if len(queue): return False
        return True

    def wait_empty(self, timeout=None) -> bool:
        with self.lock:
            return self.cv.wait_for(lambda: self.empty(), timeout)

    def dequeue(self):
        with self.lock:
            self.cv.wait_for(lambda: (self.inflight < self.inflight_limit) and
                             (not self.empty()))
            self.inflight += 1
            for tag in self.tags:
                if len(self.queues[tag]):
                    fn = self.queues[tag][0]
                    self.queues[tag] = self.queues[tag][1:]
                    self.detach(fn)
