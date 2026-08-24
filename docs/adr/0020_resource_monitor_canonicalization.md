# ADR 0020 — Resource Monitor Canonicalization

**Status:** Resolved on 2026-08-11 — the premise was false
**Date:** Proposed 2026-08-04, verified 2026-08-11

## Context

Two sources described two different Snowflake Resource Monitors, and nothing
reconciled them:

| Source | Monitor | Quota | Frequency | Level |
|---|---|---|---|---|
| `CLAUDE.md` | `cdc_trial_monitor` | 348 credits | `NEVER` | account |
| `scripts/snowflake_setup.sql` | `cdc_poc_monitor` | 20 credits | `MONTHLY` | warehouse |

A Resource Monitor is the emergency brake on credit spend. Having two
descriptions of it means not knowing what the brake is set to — and this is a
pipeline whose whole cost story ([ADR-0019](0019_streams_and_triggered_tasks_as_the_gate.md))
depends on the brake being real.

## Decision

Treat `scripts/snowflake_setup.sql` as canonical, because it is versioned and
re-runnable, and `CLAUDE.md` is prose. Then run `scripts/verify_governance.sql`
against the account, as `ACCOUNTADMIN`, **before any live demo**, to confirm the
actual state rather than the documented one.

## Rationale

Between a versioned artifact and a description of one, the artifact wins: it can
be re-applied, diffed and reviewed. But neither source is evidence of what the
account actually contains, which is why the decision is not "pick one" but "pick
one, then go look".

## Verification result (2026-08-11, as `ACCOUNTADMIN`)

**Neither monitor existed.**

`SHOW RESOURCE MONITORS` returned zero rows. The account-level `RESOURCE_MONITOR`
parameter was empty. `CDC_WH.resource_monitor` was null.

The question this ADR asked — *which* of the two monitors is real — was badly
formed. The answer was neither. `snowflake_setup.sql`, treated here as canonical,
was not even present in the repository, which is consistent with its never having
been executed.

All three layers where a brake could have lived (account monitor, account
parameter, warehouse binding) were empty.

## Consequences

- ~~Blocking for a live demo until confirmed.~~ **Resolved the same day.** The
  verification showed there was no brake at all, and `scripts/snowflake_setup.sql`
  — written on the spot, because it did not exist either — created
  `cdc_poc_monitor` at 09:54: 20 credits, `MONTHLY`, warehouse level, bound to
  `CDC_WH`, with `NOTIFY` at 50% and 75%, `SUSPEND` at 90% and `SUSPEND_IMMEDIATE`
  at 100%.
- Between the design date and that morning, what actually contained cost was
  behavioural, not enforced: `AUTO_SUSPEND = 60s` and the sensors consulting
  Prometheus before touching Snowflake.
- `CDC_WH` has `ENABLE_QUERY_ACCELERATION = true` with `SCALE_FACTOR = 2` — a
  billing path that charges beyond the warehouse's own compute. It is probably
  dormant in this workload, since the scans are too small for QAS to engage, but
  it is exactly the kind of spend a monitor would catch and there was nothing
  there to catch it.

## What this ADR is really about

The decision that mattered was not which monitor to believe. It was refusing to
close the question on documentation alone. Both documented answers were wrong,
and the only thing that revealed that was running the query against the account.

## See also

- `scripts/verify_governance.sql` — the verification
- `scripts/snowflake_setup.sql` — the monitor, as created on 2026-08-11
- `.claude/sdd/features/DESIGN_GOVERNANCA_CUSTO_DISPARO.md` — Decision 4
