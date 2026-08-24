# ADR 0027 — Severity Convention for Tests, Logging and Cost Actions

**Status:** Accepted — ratified by Christian on 2026-08-11, retroactively
**Date:** Proposed 2026-08-04, accepted 2026-08-11

## Context

The project had no documented severity convention. `grep severity` returned only
code from an external package. Three separate contexts needed one and each was
deciding ad hoc:

- **dbt tests** — `error` vs `warn`
- **Dagster logging** — `log.warning` vs `log.info`
- **Resource Monitor `TRIGGER_ACTION`** — `SUSPEND` vs `SUSPEND_IMMEDIATE`

Without a criterion, severity gets chosen by whoever writes the line, which over
185 tests means it is chosen by the default.

## Decision

| Level | Criterion |
|---|---|
| `error` / `SUSPEND_IMMEDIATE` | The violation corrupts data, or leaves state inconsistent with no safe recovery |
| `warn` / `log.warning` | The data is degraded but the system remains predictable |
| `info` / `log.info` | Normal operation |

The practical test: **a dbt test is `error` if the violation, propagated, would
make a business metric objectively wrong.**

## Rationale

The criterion is deliberately about consequence rather than about which layer or
which column type the test sits on. A referential-integrity gap traced back to
the source database degrades a metric without making it wrong — the join drops
rows that the source itself never had. A duplicate on a MERGE key makes it wrong.

## Alternatives considered

None were evaluated. This is a first proposal rather than a choice between known
options, and the ADR records that honestly rather than inventing rejected
alternatives after the fact.

## Consequences

### The convention was applied before it was accepted

The original text of this decision said, in bold, **"do not apply to the 162
existing dbt tests until this Decision is explicitly accepted."**

That was violated. On 2026-08-10 the Silver and Gold build applied the convention
to 102 tests, and the `BUILD_REPORT` for that build records in plain text that it
was "the first application of the severity convention". The ratification of
2026-08-11 is therefore retroactive — it legalizes what had been in production for
a day.

The original sentence is kept here rather than deleted, because the order in
which this happened is the useful information.

### State at ratification

- **185 tests** in the project: 83 Bronze, 72 Silver, 30 Gold.
- **16 carry an explicit `severity: warn`** — 14 in `dbt/models/silver/schema.yml`,
  2 in `dbt/models/gold/schema.yml`.
- The other **169 run at the `error` default**.
- **The "162" no longer corresponds to anything.** That number is from
  2026-08-04 and predates the creation of the Silver and Gold layers.

### Retroactive application completed

The 83 Bronze tests — the only block that had never been through the criterion —
were reviewed one by one on 2026-08-11. **No severity changed:** all 83 remain
`error`, now by analysis rather than by omission of the default.

The reasoning by column class is in the header of `dbt/models/bronze/schema.yml`.
The summary is that every tested column in that layer is structural load: MERGE
key, delete filter, deduplication ordering, incremental watermark, or Gold
grouping key. The most obvious `warn` candidate, `kafka_created_at`, turned out to
be the incremental watermark for all ten models — see
[ADR-0028](0028_record_metadata_as_watermark.md).

With that, all 185 tests are classified.

### Coverage of the three contexts

Two of the three are exercised: dbt tests and Dagster logging. The third, the
Resource Monitor `TRIGGER_ACTION`, cannot be verified with the repository's
credentials — it depends on running `scripts/verify_governance.sql` as
`ACCOUNTADMIN` ([ADR-0020](0020_resource_monitor_canonicalization.md)).

## See also

- `dbt/models/bronze/schema.yml` — the per-column-class reasoning
- `.claude/sdd/features/DESIGN_GOVERNANCA_QUALIDADE_DADOS.md` — Decision 3
