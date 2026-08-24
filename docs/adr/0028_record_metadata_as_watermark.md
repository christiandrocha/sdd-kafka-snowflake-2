# ADR 0028 — `RECORD_METADATA` Retained as Watermark and Dedup Tiebreaker

**Status:** Accepted — decided during Build, from evidence
**Date:** 2026-08-07

## Context

The v1.0 design of the V4 migration
([ADR-0021](0021_kafka_connector_v4_schematization.md)) assumed that native
schematization would eliminate **both** VARIANT columns the old connector
produced: `RECORD_CONTENT` and `RECORD_METADATA`.

That assumption left the rewritten Bronze models with no basis for two things the
previous generation resolved through that VARIANT:

- the incremental filter, which read `RECORD_METADATA:CreateTime`;
- the deduplication tiebreaker, which read `RECORD_METADATA:offset`.

Without them, "which copy of this row is the current one" has no answer.

## Decision

Keep `RECORD_METADATA`. Use it as the incremental watermark and as the
deduplication tiebreaker, exactly as before.

The assumption was simply wrong, and the correction was to check rather than to
design around it.

## Rationale

Introspection of the 4.1.0 JAR during Build showed the literal `RECORD_METADATA`
present in `SnowflakeSinkServiceV2` and `ConnectorConfigDefinition`, with the
configuration keys `snowflake.metadata.{all,createtime,offset.and.partition,topic}`
still in place.

Confirmed afterwards against a real table: `RECORD_METADATA VARIANT` is the only
VARIANT column that survives schematization. `RECORD_CONTENT` is genuinely gone;
`RECORD_METADATA` never was.

Kafka offset is the right tiebreaker because it is the only strictly ordered
value available per partition. Event timestamps can tie; offsets cannot.

## Alternatives considered

1. **Derive a watermark from a business timestamp column** — rejected. Business
   timestamps come from the source system and can be null, backdated or equal
   across rows; the watermark has to be a property of ingestion, not of the
   record.
2. **Add a synthetic ingestion timestamp in the Bronze model** — rejected as
   redundant once `RECORD_METADATA` was confirmed present, and it would have been
   assigned at transform time rather than at landing time, which is the wrong
   moment.

## Consequences

- The Bronze models keep one VARIANT column and the `:` path extraction that goes
  with it. Schematization simplified the payload, not the metadata.
- `kafka_created_at`, derived from this column, is the incremental watermark for
  all ten Bronze models — which is why it is tested at `error` severity despite
  looking like a candidate for `warn`
  ([ADR-0027](0027_severity_convention.md)).

## What this ADR is really about

A design-time assumption about a third-party JAR was falsified by reading the
JAR. The cost of checking was minutes; the cost of not checking would have been
ten Bronze models with no valid incremental filter.

## See also

- `dbt/models/bronze/` — the models consuming `RECORD_METADATA`
- `.claude/sdd/archive/MIGRACAO_INGESTAO_V4/DESIGN_MIGRACAO_INGESTAO_V4.md` — Decision 3
