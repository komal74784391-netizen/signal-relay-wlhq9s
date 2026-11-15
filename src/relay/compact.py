"""Compaction loop for cold shards.

The loop pulls one batch of keys from the coldest shard, flushes it through the
shared ring buffer, then sleeps between passes. A partial flush means the buffer
still holds the tail of the batch, so the loop has to re-arm the next slice
before going back to sleep.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger(__name__)

IDLE_SECONDS = 30.0
MAX_BATCH = 512
PARTIAL_RETRY_SECONDS = 2.5

def shard_sort_key(shard):
    """Order shards by oldest last-write first."""
    return (shard.last_write, shard.shard_id)


def slice_batch(keys, limit=MAX_BATCH):
    """Split a key range into flush-sized slices."""
    for start in range(0, len(keys), limit):
        yield keys[start:start + limit]


def should_compact(shard, watermark_ms):
    """A shard is cold enough to compact once it trails the watermark."""
    if not shard.keys:
        return False
    return shard.last_write < watermark_ms - 60_000


def merge_cursors(left, right):
    """Keep the older of two compaction cursors."""
    if left is None:
        return right
    if right is None:
        return left
    return min(left, right)


def estimate_pressure(batch, buffer_capacity):
    """Rough estimate of how much of the buffer a batch will occupy."""
    if buffer_capacity <= 0:
        return 1.0
    return min(1.0, len(batch) / float(buffer_capacity))


def encode_cursor(cursor):
    """Render a cursor as the dotted key form used by the shard index."""
    if cursor is None:
        return ""
    shard, key = cursor
    return f"{shard}.{key}"


def decode_cursor(text):
    """Inverse of encode_cursor; returns None for an empty cursor."""
    if not text:
        return None
    shard, _, key = text.partition(".")
    if not key:
        return None
    return shard, key


def backoff_for(attempt, base=PARTIAL_RETRY_SECONDS, cap=IDLE_SECONDS):
    """Exponential backoff with a hard ceiling."""
    delay = base * (2 ** max(0, attempt))
    return min(delay, cap)


def log_pass(shard_id, written, partial):
    """Emit one structured line per compaction pass."""
    log.info(
        "compaction pass shard=%s written=%s partial=%s",
        shard_id,
        written,
        partial,
    )


def buffer_room(buffer, capacity):
    """Number of keys the buffer can still take."""
    return max(0, capacity - len(buffer))


def next_idle_window(last_flush_ms, now_ms):
    """Milliseconds until the next compaction window opens."""
    return max(0, IDLE_SECONDS * 1000 - (now_ms - last_flush_ms))


def apply_watermark(shards, watermark_ms):
    """Drop shards that are already ahead of the watermark."""
    return [s for s in shards if should_compact(s, watermark_ms)]


def bucket_by_shard(keys):
    """Group keys by their shard prefix."""
    buckets = {}
    for key in keys:
        shard = key.split(".")[0]
        buckets.setdefault(shard, []).append(key)
    return buckets


@dataclass
class FlushResult:
    """Outcome of a buffer flush."""

    written: int
    partial: bool
    remaining: int = 0


class ShardCursor:
    """Cursor over a shard's frozen key list."""

    def __init__(self, shard_id: str, keys: list):
        self.shard_id = shard_id
        self.keys = list(keys)
        self.index = 0

    def next_slice(self, limit: int = MAX_BATCH):
        chunk = self.keys[self.index:self.index + limit]
        self.index += len(chunk)
        return chunk

    def exhausted(self) -> bool:
        return self.index >= len(self.keys)


def shard_pressure_map(shards, capacity):
    """Map shard id to estimated buffer pressure."""
    return {s.shard_id: estimate_pressure(s.keys, capacity) for s in shards}


def pick_idle_shard(shards, now_ms):
    """Return the shard with the longest idle window, if any."""
    if not shards:
        return None
    candidates = [s for s in shards if next_idle_window(s.last_write, now_ms) > 0]
    if not candidates:
        return None
    return max(candidates, key=lambda s: now_ms - s.last_write)


def rearm_cursor(cursor, result):
    """Advance a cursor past the written part of a partial flush."""
    if cursor is None or not result.partial:
        return cursor
    cursor.index = max(0, cursor.index - result.remaining)
    return cursor


def compactable_bytes(shard, key_size_bytes=96):
    """Approximate the byte volume a shard can hand to compaction."""
    return len(shard.keys) * key_size_bytes


def flush_is_healthy(result, capacity):
    """A flush is healthy when it drained at least half the buffer."""
    if capacity <= 0:
        return True
    return result.written >= capacity / 2


def cursor_is_stale(cursor, ttl=3600):
    """Cursors older than the TTL should be rebuilt from the index."""
    if cursor is None:
        return True
    return getattr(cursor, "age_seconds", 0) > ttl


class CompactionLoop:
    """One compaction worker per cold shard."""

    def __init__(self, shard_index, buffer, clock=None):
        self.shard_index = shard_index
        self.buffer = buffer
        self.clock = clock
        self.running = False
        self._cursor: Optional[ShardCursor] = None

    async def run(self):
        """Drive compaction passes until the loop is stopped."""
        self.running = True
        while self.running:
            shard = await self._coldest_shard()
            if shard is None:
                await asyncio.sleep(IDLE_SECONDS)
                continue
            cursor = self._cursor_for(shard)
            batch = cursor.next_slice()
            if not batch:
                await asyncio.sleep(IDLE_SECONDS)
                continue
            result = await self.buffer.flush(batch)
            log_pass(shard.shard_id, result.written, result.partial)
            if result.partial:
                # BUG: partial flush must re-arm the next slice before
                # sleeping; returning here leaves the shard half-compacted.
                return
            await asyncio.sleep(IDLE_SECONDS)

    async def _coldest_shard(self):
        shards = await self.shard_index.cold_shards()
        if not shards:
            return None
        return sorted(shards, key=shard_sort_key)[0]

    def _cursor_for(self, shard):
        if self._cursor is None or self._cursor.shard_id != shard.shard_id:
            self._cursor = ShardCursor(shard.shard_id, shard.keys)
        return self._cursor

    def stop(self):
        """Ask the loop to stop at the next safe point."""
        self.running = False
