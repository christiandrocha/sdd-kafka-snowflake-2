# ADR 0025 — Bronze Is Append-Only

**Status:** Accepted — a documented convention, not yet a technical constraint
**Date:** 2026-08-04

## Context

Three separate mechanisms in this pipeline already depend on Bronze receiving
only `INSERT`s, and none of them said so anywhere:

- the Streams created with `APPEND_ONLY = TRUE`
  ([ADR-0019](0019_streams_and_triggered_tasks_as_the_gate.md)) — an append-only
  Stream does not surface updates or deletes at all;
- the `op != 'd'` delete filter in Silver, which assumes the delete marker is a
  new row rather than a mutation of an old one;
- Bronze's 1-day Time Travel window, sized for a table that only grows.

An implicit assumption holding up three mechanisms is not an assumption, it is an
undocumented contract.

## Decision

Declare formally that **no process may issue `UPDATE` or `DELETE` against any
`BRONZE.*` table**. The only valid write path into Bronze is `INSERT` via
Snowpipe.

## Rationale

The purpose is to prevent accidental violation, and the realistic violation is
not malice — it is a quick manual correction. Someone fixes one obviously wrong
row with an `UPDATE`, and the append-only Stream silently does not report it,
which means the fix never propagates to Silver and the two layers disagree from
then on with nothing failing.

Making the assumption explicit is the cheapest available defence against that.

## Alternatives considered

1. **Do not document it; rely on nobody violating it** — rejected. This is
   precisely the category of implicit premise that had already produced a real
   defect in this project: the sensor watermark bug of
   [ADR-0024](0024_sensor_without_global_watermark.md) was a different mechanism
   but the same failure mode — an unstated assumption nobody had checked.

## Consequences

- **There is no automatic enforcement.** This is a convention in a document, and
  a convention in a document does not stop a statement in a worksheet.
- The suggested follow-up is a grant-level constraint that would make it real:
  `REVOKE UPDATE, DELETE ON ALL TABLES IN SCHEMA BRONZE FROM ROLE CDC_ROLE`.
  Not applied yet.

## See also

- `dbt/models/bronze/schema.yml`
- `scripts/streams_and_tasks.sql` — the `APPEND_ONLY` Streams that depend on this
- `.claude/sdd/features/DESIGN_GOVERNANCA_QUALIDADE_DADOS.md` — Decision 1
