# Architecture Decision Records

Decisions behind this pipeline (PostgreSQL → Debezium → Kafka → Snowflake → dbt
→ Dagster), each with the context that forced it, the alternatives rejected, and
what it cost.

## Index

| ADR | Title | Status |
|-----|-------|--------|
| [0018](0018_dedicated_postgres_for_dagster_storage.md) | Dedicated Postgres for Dagster storage | Accepted |
| [0019](0019_streams_and_triggered_tasks_as_the_gate.md) | Snowflake Streams + Triggered Tasks as the trigger gate | Accepted |
| [0020](0020_resource_monitor_canonicalization.md) | Resource Monitor canonicalization | Resolved — the premise was false |
| [0021](0021_kafka_connector_v4_schematization.md) | Kafka Connector V4 with native schematization | Accepted |
| [0022](0022_tier_1_ingestion_scope.md) | Ingest 10 domains, not 20 | Accepted |
| [0024](0024_sensor_without_global_watermark.md) | Dagster sensor reads `PENDING_RUNS` without a global watermark | Accepted |
| [0025](0025_bronze_is_append_only.md) | Bronze is append-only | Accepted — convention, not yet enforced |
| [0026](0026_gold_aggregation_categories.md) | Gold models categorized by aggregation pattern | Accepted |
| [0027](0027_severity_convention.md) | Severity convention for tests, logging and cost actions | Accepted — ratified retroactively |
| [0028](0028_record_metadata_as_watermark.md) | `RECORD_METADATA` retained as watermark and dedup tiebreaker | Accepted — decided during Build |
| [0029](0029_snowpipe_streaming_as_the_ingestion_path.md) | Snowpipe Streaming as the ingestion path | Accepted |
| [0030](0030_avro_and_schema_registry_as_the_contract.md) | Avro + Schema Registry as the contract boundary | Accepted |

## On the numbering

The numbers are inherited, not sequential from 1. They come from the predecessor
repository (`sdd-kafka-snowflake`, "v5-delivery"), where these decisions were
first taken, and **the code in this repository already cites them by number** —
`docker-compose.yml` references ADR-0018, `scripts/streams_and_tasks.sql` and
`dagster/pipeline/sensors.py` reference ADR-0019 and ADR-0022. Renumbering would
have broken those references for no gain.

The original ADR files did not survive the move to this repository. What did
survive is the decision content, carried into the SDD design documents under
`.claude/sdd/`, each marked "substitui ADR-00XX". These files reconstruct the
records from that content, from the code that implements them, and from the
verification runs that later confirmed or refuted them.

Two gaps are deliberate and worth stating plainly:

- **ADR-0023 has no surviving record.** Nothing in this repository references it
  and no design document claims to supersede it. It is not reconstructed here,
  because inventing it would be worse than the gap.
- **ADR-0028 is new.** It records a decision taken during Build on 2026-08-07,
  from evidence rather than from design, which never received a number.
- **ADR-0029 and ADR-0030 are new**, continuing the inherited sequence. They
  formalize two decisions that were implemented and load-bearing but never
  written down: the ingestion path, and the contract boundary at the registry.

One decision was deliberately **not** written. Splitting the warehouse
(`CDC_WH_TRANSFORM` alongside `CDC_WH`) appears twice in
`.claude/sdd/features/DEFINE_GOVERNANCA_CUSTO_DISPARO.md`, both times as
*"aguardando contexto adicional, vira feature própria depois"* — deferred, with
no choice made and no alternative rejected. `CDC_WH_BI` does not appear in this
repository at all. There is nothing to record yet; it is tracked in the README
under **What's next** instead, which is where an open question belongs.

## Format

`Context → Decision → Rationale → Alternatives considered → Consequences → See
also`, following the convention of the sibling repository `sdd-kafka-databricks`.

Where a decision was later verified against the real account, the verification
result is recorded **in the ADR itself** rather than replacing the original
reasoning. ADR-0020 is the clearest case: the decision asked which of two
Resource Monitors was the real one, and the answer turned out to be neither.
The question is left standing next to its refutation, because the order in which
that happened is the useful part.
