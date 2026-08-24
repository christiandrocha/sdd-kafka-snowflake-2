# ADR 0026 — Gold Models Categorized by Aggregation Pattern

**Status:** Accepted
**Date:** 2026-08-04

## Context

Deciding which Gold models are worth incrementalizing requires knowing how each
one aggregates. A model that sums additively over a partitionable key can be made
incremental cheaply; a model computing a global ratio cannot, because the
denominator moves with every new row.

An earlier attempt categorized them by name and heuristic. Reading the actual SQL
corrected **two** of those categorizations.

## Decision

Categorize on evidence from the SQL, and record the result:

| Model | Current materialization | Category |
|---|---|---|
| `gold_payment_lifecycle` | incremental (already correct) | Additive-partitionable — the reference pattern |
| `gold_payments_by_status` | incremental (already correct) | Global ratio, bounded by low cardinality |
| `gold_payment_funnel` | table (full refresh, intentional) | Global ratio |
| `gold_driver_performance` | table | Additive-partitionable (not yet incrementalized) |
| `gold_revenue_per_restaurant` | table | Additive-partitionable (not yet incrementalized) |
| `gold_user_behavior` | table | Additive-partitionable per user — cumulative, **not** a sliding window |

The last row is one of the two corrections: the name suggests a time-windowed
behaviour metric, and the SQL is cumulative per user. Those incrementalize
differently.

## Rationale

Incrementalization is a decision about cost and correctness together — get the
category wrong and the model either recomputes needlessly or silently produces
wrong numbers on a partial refresh. That is not a judgement to make from a table
of names.

## Alternatives considered

1. **Categorize by name/heuristic without reading the SQL** — rejected. It had
   already been tried and produced at least two wrong categorizations, which is
   what prompted the audit.

## Consequences

- **Nothing was incrementalized by this decision.** It produces the
  categorization only, as the basis for a future change triggered by real volume
  rather than by anticipation.
- The two `table` models marked additive-partitionable are the candidates when
  that trigger arrives.

## See also

- `dbt/models/gold/` — the six models
- `.claude/sdd/features/DESIGN_GOVERNANCA_QUALIDADE_DADOS.md` — Decision 2
