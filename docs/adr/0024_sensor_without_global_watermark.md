# ADR 0024 — Dagster Sensor Reads `PENDING_RUNS` Without a Global Watermark

**Status:** Accepted
**Date:** 2026-08-04

## Context

With the trigger gate moved into Snowflake
([ADR-0019](0019_streams_and_triggered_tasks_as_the_gate.md)), the Dagster sensor's
job became: read the signal rows the Tasks write into `CONFIG.PENDING_RUNS`, and
launch the corresponding `dbt run`.

The first rewritten version selected with `WHERE detected_at > cursor AND consumed
= FALSE` — a global watermark cursor, the conventional shape for a polling
consumer.

A comparison against an independent second opinion identified that this
reintroduces a bug the project had already had once: a high-volume domain writes a
recent `detected_at` and advances the shared cursor; a low-volume domain whose
`detected_at` is older than the now-advanced cursor is never selected again — even
though its row still has `consumed = FALSE`. The row is not lost, it is invisible,
which is worse, because nothing reports it.

## Decision

Remove the cursor comparison entirely.

```sql
SELECT ... FROM CONFIG.PENDING_RUNS WHERE consumed = FALSE
```

No timestamp filter. Rows are retired by setting `consumed = TRUE` in the update,
not by a moving time cut.

## Rationale

`consumed = FALSE` is already a sufficient idempotency guard on its own. Each row
is processed exactly once regardless of when its `detected_at` was written
relative to any other row's. The cursor was not adding safety; it was adding an
ordering assumption the data does not satisfy.

The general shape of the error is worth naming: a watermark is only safe when the
thing it orders is produced in that order. Here, ten independent Tasks write
whenever their own stream fires, so `detected_at` across domains is not a
monotonic sequence at all.

## Alternatives considered

1. **Keep the cursor, partitioned per domain** — would fix the bug. Rejected
   because it trades one global cursor for N cursors and their bookkeeping, with
   no benefit over simply deleting the cursor.

## Consequences

- Fixes a class of bug that had already occurred once in the original sensor.
- No automated regression test covers it. AT-003 in the DEFINE document specifies
  exactly the scenario — a high-volume domain advancing past a low-volume one —
  but it was not included in that Build cycle. Recorded as debt, still open.

## See also

- `dagster/pipeline/sensors.py` — `bronze_new_data_sensor`
- `.claude/sdd/features/DEFINE_GOVERNANCA_CUSTO_DISPARO.md` — AT-003, the missing
  regression test
- `.claude/sdd/features/DESIGN_GOVERNANCA_CUSTO_DISPARO.md` — Decision 2
