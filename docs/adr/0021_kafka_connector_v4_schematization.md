# ADR 0021 — Kafka Connector V4 with Native Schematization

**Status:** Accepted
**Date:** 2026-08-04 (validated empirically 2026-08-07)

## Context

Ingestion ran on the Snowflake Kafka connector v2.1.2, in classic Snowpipe mode —
a generation Snowflake has discontinued. Every domain landed as a single
`RECORD_CONTENT` VARIANT column, and every Bronze model started by unpacking it.

## Decision

Move to `SnowflakeStreamingSinkConnector` (v4) with:

```properties
snowflake.enable.schematization=true
snowflake.validation=client_side
snowflake.compatibility.enable.column.identifier.normalization=true
```

Columns arrive natively typed. `RECORD_CONTENT` disappears; `RECORD_METADATA`
does not — see [ADR-0028](0028_record_metadata_as_watermark.md).

## Rationale

**`client_side` over the v4 default `server_side`.** Client-side validation uses
the Schema Registry as the source of type information. The project already
maintains that registry with BACKWARD compatibility discipline, so the types are
curated. Server-side validation instead infers types from the first record it
observes, with a documented risk of demoting a type — `FLOAT64` inferred as
`NUMBER(38,0)` from a first record that happened to carry a whole number. The
registry is the better authority precisely because it was decided, not observed.

**Identifier normalization to upper case.** This was reverted once during the
design conversation, then reinstated after understanding the practical cost:
preserving the Avro field case would require double-quoting every column
reference in every dbt model. That is a permanent tax on all downstream SQL in
exchange for cosmetic fidelity to the source casing.

## Alternatives considered

1. **v3-compatible mode (`schematization=false`)** — rejected. It would carry the
   migration cost without the benefit; the point of moving is the natively typed
   columns.
2. **`validation=server_side`** (the v4 default) — rejected for the type
   inference risk described above.
3. **Preserve the original Avro case (`identifier.normalization=false`)** —
   rejected once the double-quoting cost across all dbt SQL was understood.

## Consequences

- All 10 Bronze models had to be rewritten. This was the single largest work item
  of the migration.
- Two v2.1.2 configuration keys do not exist in the 4.1.0 JAR and were dropped:
  `snowflake.schema.registry.url` (client-side validation reads
  `value.converter.schema.registry.url` instead) and the buffer trio
  `buffer.count.records` / `buffer.flush.time` / `buffer.size.bytes` (the
  client-side buffer no longer exists).
- ~~The `schematization=true` + `client_side` combination has no documented
  example and needs empirical validation.~~ **Validated 2026-08-07.** `DESC TABLE
  CDC_POC.BRONZE.RESTAURANTS` on the real account returns `AVERAGE_RATING FLOAT`,
  `NUM_REVIEWS NUMBER(38,0)`, `OPENING_TIME TIME(6)` — natively typed, with no
  `RECORD_CONTENT` column.

## See also

- `connectors/` — the sink configuration
- `.claude/sdd/archive/MIGRACAO_INGESTAO_V4/DESIGN_MIGRACAO_INGESTAO_V4.md` — Decision 1
- `.claude/sdd/archive/MIGRACAO_INGESTAO_V4/BUILD_REPORT_MIGRACAO_INGESTAO_V4.md` — the validation
