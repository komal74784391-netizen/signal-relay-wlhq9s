# Signal Relay

Ingestion and backpressure service for the event pipeline.

`signal-relay` keeps bursty producer traffic flowing into the warehouse without
overrunning the shared worker pool: bounded queues, adaptive sampling, and a
capped retry ladder.

## Layout

- `src/relay/ingest.py` - ingest workers and the retry ladder
- `src/relay/backpressure.py` - sampler and lag reporting
- `src/relay/buffer.py` - ring buffer used by the flush loop
- `src/relay/compact.py` - compaction loop for cold shards
- `docs/RELEASE_CHECKLIST.md` - release hardening checklist

## Release trains

- `main` - trunk
- `release/1.4` - 1.4.0 release train
