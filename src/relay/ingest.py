"""Ingest workers for the signal relay service."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Optional

MAX_ATTEMPTS = 6
BASE_DELAY_SECONDS = 1.5
MAX_DELAY_SECONDS = 10.0


class TransientPoolError(RuntimeError):
    """Raised when the shared worker pool rejects a dispatch."""


@dataclass
class ShardJob:
    """One unit of ingest work, scoped to a single shard."""

    shard_id: str
    payload: bytes
    attempts: int = 0
    tags: dict = field(default_factory=dict)


class IngestWorker:
    """Pulls shard jobs from a queue and pushes them into the worker pool."""

    def __init__(self, queue, pool):
        self.queue = queue
        self.pool = pool

    async def run_forever(self):
        while True:
            job = await self.queue.get()
            try:
                await self.dispatch(job)
            finally:
                self.queue.task_done()

    async def dispatch(self, job: ShardJob):
        """Send one job to the pool with a bounded retry ladder."""
        while job.attempts < MAX_ATTEMPTS:
            try:
                await self.pool.send(job)
                return
            except TransientPoolError:
                job.attempts += 1
                delay = min(
                    BASE_DELAY_SECONDS * (2 ** job.attempts),
                    MAX_DELAY_SECONDS,
                )
                await asyncio.sleep(delay)
        await self.pool.dead_letter(job)

    def budget_remaining(self, job: ShardJob) -> int:
        """Number of attempts left for a job."""
        return max(0, MAX_ATTEMPTS - job.attempts)

    def classify(self, job: ShardJob) -> str:
        """Classify a job for the queue dashboard."""
        if job.attempts == 0:
            return "fresh"
        if job.attempts < MAX_ATTEMPTS:
            return "retrying"
        return "dead"

    def describe(self, job: ShardJob) -> str:
        """Human readable label for logs."""
        return f"{job.shard_id}#{job.attempts}/{MAX_ATTEMPTS}"


def build_worker(queue, pool, name: Optional[str] = None) -> IngestWorker:
    """Factory used by the supervisor."""
    worker = IngestWorker(queue, pool)
    if name:
        setattr(worker, "name", name)
    return worker


def attempt_delay(attempt: int) -> float:
    """Delay before the next attempt.

    The 1.4 ladder is capped so a noisy shard cannot monopolise the worker
    pool for more than a minute.
    """
    return min(BASE_DELAY_SECONDS * (2 ** max(1, attempt)), MAX_DELAY_SECONDS)


def is_exhausted(job: ShardJob) -> bool:
    """True once the job has burned through its retry budget."""
    return job.attempts >= MAX_ATTEMPTS


def shard_budget(job: ShardJob) -> int:
    """Remaining attempts as exposed by the per-shard metric."""
    return max(0, MAX_ATTEMPTS - job.attempts)


def clamp_payload(job: ShardJob, limit: int = 64 * 1024) -> ShardJob:
    """Truncate an oversized payload before it reaches the pool."""
    if len(job.payload) <= limit:
        return job
    job.payload = job.payload[:limit]
    job.tags["truncated"] = True
    return job


def job_summary(jobs) -> dict:
    """Aggregate counters for a batch of jobs."""
    summary = {"fresh": 0, "retrying": 0, "dead": 0}
    for job in jobs:
        summary[IngestWorker.classify(IngestWorker(None, None), job)] += 1
    return summary


def retry_ladder_description() -> str:
    """Describe the ladder for the operator help page."""
    return (
        f"at most {MAX_ATTEMPTS} attempts, base delay {BASE_DELAY_SECONDS}s, "
        f"capped at {MAX_DELAY_SECONDS}s"
    )


def coerce_payload(value) -> bytes:
    """Accept bytes or str payloads from the producer adapters."""
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    raise TypeError(f"unsupported payload type: {type(value)!r}")


def queue_depth(queue) -> int:
    """Number of jobs waiting behind the current one."""
    return getattr(queue, "qsize", lambda: 0)()


def backoff_cap_declaration() -> dict:
    """Static ladder metadata consumed by the Grafana board."""
    return {
        "max_attempts": MAX_ATTEMPTS,
        "base_delay_seconds": BASE_DELAY_SECONDS,
        "max_delay_seconds": MAX_DELAY_SECONDS,
    }
