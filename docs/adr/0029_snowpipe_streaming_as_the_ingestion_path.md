# ADR 0029 — Snowpipe Streaming as the Ingestion Path

**Status:** Accepted
**Date:** 2026-08-04 (migrated 2026-08-07, pipe topology verified 2026-08-12)

## Context

Kafka has to reach Snowflake. Three mechanisms exist, and they differ in latency,
in billing model, and in how much infrastructure they oblige the project to own.

The starting position was worse than it looked. The connector was
`snowflake-kafka-connector` v2.1.2 running with
`snowflake.ingestion.method=SNOWPIPE` — **classic, file-based Snowpipe** — while
every document in the project described the pipeline as "Snowpipe Streaming".
The architecture diagram said one thing and the configuration said another, and
nobody had checked which was true.

Classic Snowpipe writes records into staged files, then loads the files. That
adds a file-landing step between Kafka and the table, and it is the generation
Snowflake is discontinuing: v3 and earlier were given an 18-month end-of-life
window.

## Decision

Ingest through **Snowpipe Streaming**, via the v4
`SnowflakeStreamingSinkConnector`, in default pipe mode.

The connector writes rows directly into the Bronze tables. It does not declare
its own pipe: in this mode Snowflake creates and manages one `PIPE` per target
table. What the project owns is the privilege that lets the connector's role
trigger that creation.

```json
"connector.class": "com.snowflake.kafka.connector.SnowflakeStreamingSinkConnector",
"snowflake.streaming.classic.offset.migration": "skip"
```

## Rationale

**Row-level, not file-level.** Snowpipe Streaming commits rows. That is what
makes [ADR-0019](0019_streams_and_triggered_tasks_as_the_gate.md) possible: the
Stream sits on the real Bronze table, so it can only report "there is data"
*after* the row is committed. With file-based loading there is an extra staging
hop between "Kafka has it" and "Snowflake has it", and the trigger gate would
have to guess at the interval instead of observing it.

**Billing shape suits the workload.** Snowpipe Streaming bills by throughput.
Measured on this account, the **entire ingestion path costs 0.0005 credits** —
thirteen thousandths of one percent of consumption. Classic `PIPE` consumption
is 0.0000: unused, as intended. An earlier draft of the cost section warned that
serverless ingestion was unmeasured and "not a rounding error"; it is a rounding
error, and the measurement is what settled it.

**Not being on a deprecated generation** is the cheapest benefit to state and
the most expensive to skip.

## Alternatives considered

1. **Stay on classic Snowpipe (v2.1.2, `ingestion.method=SNOWPIPE`)** — the
   status quo. Rejected: deprecated generation, an extra file-staging hop, and
   it would leave the trigger-gate design without a reliable commit signal.
2. **`COPY INTO` from an external stage, on a schedule** — the batch path. Never
   a live candidate here, and worth saying why rather than pretending it was
   weighed: it obliges the project to own an object store, a file-landing
   convention and a scheduler for the load itself, all to reintroduce the
   latency this design is removing. It is the right answer for bulk backfill
   from files, which is not what this pipeline does.
3. **Snowpipe Streaming with a self-declared pipe** — rejected as needless
   ownership. Snowflake-managed pipes are one fewer object to version and keep
   in step with the domain list.

## Consequences

- **The connector's role needs `CREATE PIPE`.** This is not decorative: without
  it, nothing ingests. Until 2026-08-12 `scripts/00_account_bootstrap.sql` did
  **not** grant it, while the live account had held it since 2026-08-06 — the
  only divergence of that script by *omission* rather than by excess. Rebuilding
  the account from git would have produced working dbt models, working Tasks and
  dead ingestion. Fixed, and the failure mode is now documented at the grant.
- Pipe topology verified 2026-08-12: the 10 pipes exist in `BRONZE`, with
  `kind = STREAMING`, `is_snowflake_managed = true`, `owner = NULL`.
- Two v2.1.2 configuration keys had to be dropped in the move; see
  [ADR-0021](0021_kafka_connector_v4_schematization.md), which covers what the
  v4 connector does with the payload once it arrives.
- Ingestion cost is now a function of volume, which is what makes
  [ADR-0022](0022_tier_1_ingestion_scope.md) — ingesting 10 domains rather than
  20 — a cost decision rather than a tidiness one.

## What this ADR is really about

The documentation claimed Snowpipe Streaming for months while the configuration
said `SNOWPIPE`. The decision recorded here is partly the migration and partly
the correction: the architecture became what it had been described as.

## See also

- `connectors/snowflake_sink.json` — the sink configuration
- `scripts/00_account_bootstrap.sql` — the `CREATE PIPE` grant and its verification note
- `.claude/sdd/archive/MIGRACAO_INGESTAO_V4/` — DEFINE, DESIGN and BUILD_REPORT of the migration
