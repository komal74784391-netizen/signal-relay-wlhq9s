# 1.4.0 commit digest (generated from change descriptions)

This digest is produced by the changelog bot from the change descriptions on the
release branch. It intentionally drops the subject lines and trailers.

## AR-1391

The retry ladder accounting moved into the sampler module; no behaviour change at
steady state.

## AR-1427

The retry ladder now allows six attempts with capped exponential backoff, so one
bad shard can no longer drain the shared worker pool. Load test at 3x steady
state kept p99 ingestion latency flat at 42 ms.
