# Signal Relay - Release Checklist

Release train: 1.4.0
Owner: release engineering

## Pre-flight

- [ ] Freeze the ingest schema for the 1.4 train
- [ ] Re-run the 3x steady-state load test and attach the p99 table

## Hardening checklist

- **src/relay/ingest.py:88** — cap the per-shard retry budget
- **src/relay/backpressure.py:141** — expose sampler lag as a metric
- **src/relay/buffer.py:57** — assert the ring buffer never wraps twice per flush
- **src/relay/compact.py:206** — re-arm the compaction loop after a partial flush

## Sign-off

- [ ] Release desk sign-off recorded in the tracker