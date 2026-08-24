# ADR 0022 — Ingest 10 Domains, Not 20

**Status:** Accepted
**Date:** 2026-08-04

## Context

The pipeline ingested 20 domains. An audit of the actual `ref()` and `source()`
calls across the dbt models — reading the models, not assuming from the names —
found that **10 of them never feed Silver or Gold**. They landed in Bronze and
stopped there.

Under the v4 connector's volume-based billing
([ADR-0021](0021_kafka_connector_v4_schematization.md)), ingesting data nobody
reads is a direct, recurring cost with no offsetting value.

## Decision

Remove from ingestion scope: `PAYMENTS`, `GPS_EVENTS`, `ORDER_STATUS`, `ROUTES`,
`RECEIPTS`, `SUPPORT_TICKETS`, `PRODUCTS`, `MENU_SECTIONS`, `RATINGS`,
`INVENTORY`.

The remaining 10 are "Tier 1" — the domains that actually reach a Gold
aggregation. Streams, Tasks and Bronze models are provisioned for those only.

## Rationale

Cost is charged on ingestion volume, not on transformation. So the saving has to
be taken at ingestion; skipping the models while still ingesting the topics would
have saved nothing.

`PAYMENTS` deserves a note: it looks load-bearing by name and is not. It was
confirmed orphaned by the project owner — the payment flow is covered by
`PAYMENT_EVENTS`, a different domain. This is the case that makes the point about
reading the models rather than trusting the names.

## Alternatives considered

1. **Keep ingesting all 20, simply not process Tier 2 in dbt** — rejected. It
   leaves the ingestion bill untouched, which is the entire cost being addressed.

## Consequences

- Fewer topics, fewer Streams, fewer Tasks, fewer Bronze models. The complexity
  reduction is real and compounds with every later change.
- **Demo capability for those 10 domains is gone.** If a client asks to see
  `INVENTORY` flowing end to end, the answer is that it is out of scope and would
  need re-enabling. This was accepted knowingly.

## See also

- `scripts/streams_and_tasks.sql` — provisioning restricted to the Tier-1 ten,
  citing this ADR
- `.claude/sdd/archive/MIGRACAO_INGESTAO_V4/DESIGN_MIGRACAO_INGESTAO_V4.md` — Decision 2
