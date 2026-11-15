"""Ring buffer used by the flush loop."""
from __future__ import annotations


class RingBuffer:
    """Fixed-size ring buffer for pending keys.

    The buffer tracks how many slots are still writable in the current wrap so
    the flush loop can assert a single wrap per flush.
    """

    def __init__(self, capacity: int):
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self._slots = [None] * capacity
        self._head = 0
        self._tail = 0
        self._count = 0
        self._wraps = 0

    def __len__(self):
        return self._count

    def full(self) -> bool:
        return self._count == self.capacity

    def empty(self) -> bool:
        return self._count == 0

    def push(self, key) -> None:
        if self.full():
            raise BufferError("ring buffer is full")
        self._slots[self._head] = key
        self._head = (self._head + 1) % self.capacity
        if self._head == 0:
            self._wraps += 1
        self._count += 1

    def pop(self):
        if self.empty():
            raise BufferError("ring buffer is empty")
        key = self._slots[self._tail]
        self._slots[self._tail] = None
        self._tail = (self._tail + 1) % self.capacity
        self._count -= 1
        return key

    def assert_single_wrap_per_flush(self, flush_wraps: int) -> None:
        """Guard against double wraps inside one flush pass."""
        assert flush_wraps <= 1, "ring buffer wrapped more than once in a flush"
        self._wraps = 0

    def drain(self, limit: int = None):
        items = []
        limit = len(self) if limit is None else limit
        while items.__len__() < limit and not self.empty():
            items.append(self.pop())
        return items
