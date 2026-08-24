# ADR 0019 — Snowflake Streams + Triggered Tasks as the Trigger Gate

**Status:** Accepted
**Date:** 2026-08-04

## Context

The Dagster sensor ran `SELECT MAX(create_time)` against up to 20 Bronze tables
every 60 seconds. The warehouse's `AUTO_SUSPEND` is also 60 seconds. The two
periods collided: the polling query woke the warehouse roughly as often as the
warehouse tried to suspend, so it was effectively never idle and the account paid
for compute in order to discover that nothing had arrived.

The naive fix — poll less often — reduces the symptom without removing it. A
warehouse that wakes every five minutes to learn nothing happened is still a
warehouse being billed for nothing.

The real requirement is a gate whose *negative* answer is free.

## Decision

One `STREAM` per Bronze table (the 10 Tier-1 domains of
[ADR-0022](0022_tier_1_ingestion_scope.md)), plus one `TASK` scheduled every minute
with `WHEN SYSTEM$STREAM_HAS_DATA(...)`.

Snowflake evaluates the `WHEN` predicate in its control plane. When it is false,
no warehouse is engaged and nothing is billed. Only when there is genuinely new
data does the Task start the warehouse, write a signal row into
`CONFIG.PENDING_RUNS` via the shared stored procedure `CONFIG.SP_GATE_DOMAIN`,
and consume the stream with `INSERT ... SELECT FROM stream`.

The Dagster sensor then reads `CONFIG.PENDING_RUNS` — see
[ADR-0024](0024_sensor_without_global_watermark.md) — and runs the actual `dbt
run`. Tasks cannot execute an external CLI, so Dagster remains necessary; what it
stops doing is asking.

## Rationale

The Stream is defined over the real Bronze table, so it can only report "there is
data" **after Snowpipe has committed the row**. That eliminates by construction
the confirmation gap that any Kafka-side trigger would have to solve
heuristically: a message published to Kafka is not a row materialized in
Snowflake, and the interval between the two is not a constant you can debounce
your way around.

In other words: this design does not need to guess when the data landed, because
the thing doing the signalling is the landing itself.

## Alternatives considered

1. **A custom Kafka watcher calling the Dagster GraphQL API** — rejected. It
   requires a dedicated consumer group, rebalance handling and at-least-once
   semantics, and it still leaves the structural confirmation gap above, which
   could only be papered over with a heuristic delay rather than a guarantee.
2. **Raising `minimum_interval_seconds` on the original sensor** — rejected as a
   definitive fix. It reduces the frequency of the idle cost without removing it;
   the warehouse still cycles on and off having learned nothing.

## Consequences

- Removes the principal source of idle warehouse consumption.
- Adds 10 Streams, 10 Tasks and one stored procedure as new Snowflake objects to
  manage, version and keep in step with the domain list.
- Dagster is still required for the orchestration itself. The gate moved into
  Snowflake; the execution did not.

## See also

- `scripts/streams_and_tasks.sql` — the implementation, which cites this ADR in
  its header
- `dagster/pipeline/sensors.py` — the consumer side, citing this ADR twice
- `.claude/sdd/features/DESIGN_GOVERNANCA_CUSTO_DISPARO.md` — Decision 1
