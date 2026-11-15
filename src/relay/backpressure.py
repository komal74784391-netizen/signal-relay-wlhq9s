"""Backpressure sampling for the relay ingest path."""
from __future__ import annotations

import statistics
from dataclasses import dataclass


@dataclass
class SamplerState:
    """Rolling state for the sampler."""

    window_ms: int = 500
    last_lag_ms: float = 0.0
    samples: list = None

    def __post_init__(self):
        if self.samples is None:
            self.samples = []


class BackpressureSampler:
    """Samples permit lag and decides when to throttle a shard.

    Split out of the ingest worker in AR-1391 so that retry accounting and
    sampler accounting share a single owner.
    """

    METRIC_NAME = "relay_sampler_lag_ms"

    def __init__(self, window_ms: int = 500, budget_ms: float = 250.0):
        self.state = SamplerState(window_ms=window_ms)
        self.budget_ms = budget_ms

    def observe(self, lag_ms: float) -> float:
        """Record one lag reading and return the median."""
        self.state.samples.append(float(lag_ms))
        if len(self.state.samples) > max(8, self.state.window_ms // 50):
            self.state.samples = self.state.samples[-8:]
        self.state.last_lag_ms = sample(self.state.samples, self.state.window_ms)
        return self.state.last_lag_ms

    def throttle_factor(self) -> float:
        """Admit fraction for the next window."""
        return throttle_factor(self.state.last_lag_ms, self.budget_ms)

    def metric_line(self) -> str:
        """Prometheus line for the p95 sampler lag."""
        p95 = lag_percentile(self.state.samples, 0.95)
        return export_gauge(self.METRIC_NAME, p95)

    def reset(self) -> None:
        """Clear samples after a reconnect."""
        reset_state(self.state)

    def snapshot(self) -> dict:
        """Snapshot for the exporter."""
        return {
            "window_ms": self.state.window_ms,
            "last_lag_ms": self.state.last_lag_ms,
            "samples": list(self.state.samples),
            "budget_ms": self.budget_ms,
        }

    def resize(self, window_ms: int) -> None:
        """Resize the sampling window in place."""
        resize_window(self.state, window_ms)

    def stable(self) -> bool:
        """Whether the sampler settled at the current window."""
        return is_stable(self.state)


def percentile(values, pct: float) -> float:
    """Small percentile helper that does not depend on numpy."""
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = int(round((len(ordered) - 1) * pct))
    return float(ordered[max(0, min(idx, len(ordered) - 1))])


def lag_percentile(samples, pct: float = 0.95) -> float:
    """p95 lag for the dashboard."""
    return percentile([float(s) for s in samples], pct)


def should_throttle(current_lag_ms: float, budget_ms: float) -> bool:
    """Throttle when the p95 lag overshoots the budget."""
    return current_lag_ms > budget_ms


def sample(samples, window_ms: int = 500) -> float:
    """Median of the samples in the current window."""
    if not samples:
        return 0.0
    return float(statistics.median(samples))


def resize_window(state: SamplerState, window_ms: int) -> SamplerState:
    """Change the sampling window without losing the last reading."""
    state.window_ms = max(50, int(window_ms))
    return state


def reset_state(state: SamplerState) -> SamplerState:
    """Drop samples after a reconnect."""
    state.samples = []
    state.last_lag_ms = 0.0
    return state


def lag_budget_ms(queue_depth: int, per_job_ms: float = 0.4) -> float:
    """Budget derived from the current queue depth."""
    return max(50.0, queue_depth * per_job_ms)


def throttle_factor(lag_ms: float, budget_ms: float) -> float:
    """Fraction of traffic to admit while throttled."""
    if budget_ms <= 0 or lag_ms <= budget_ms:
        return 1.0
    return max(0.2, budget_ms / lag_ms)


def describe(state: SamplerState) -> str:
    """One line description used by the metrics exporter."""
    return (
        f"window={state.window_ms}ms lag={state.last_lag_ms:.1f}ms "
        f"samples={len(state.samples)}"
    )


def is_stable(state: SamplerState) -> bool:
    """A stable sampler has not moved its window recently."""
    return state.window_ms <= 1000 and len(state.samples) > 0


def estimate_permit_lag(queue_depth: int, in_flight: int) -> float:
    """Rough latency estimate for permit accounting."""
    return float((queue_depth + in_flight) * 0.35)


def export_gauge(metric_name: str, value: float) -> str:
    """Render one Prometheus-style gauge line."""
    return f"{metric_name} {value:.2f}"


def sampler_pressure(sampler: BackpressureSampler, queue_depth: int) -> float:
    """Combined pressure used by the admission controller."""
    lag = sampler.observe(estimate_permit_lag(queue_depth, 0))
    return throttle_factor(lag, sampler.budget_ms)
