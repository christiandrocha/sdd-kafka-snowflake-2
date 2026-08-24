# ADR 0018 — Dedicated Postgres for Dagster Storage

**Status:** Accepted
**Date:** 2026-08-04

## Context

Dagster stored its run history, event log and schedule state in SQLite. SQLite is
inadequate for concurrent writes and is fragile under crash — a hard stop of the
daemon can leave the database corrupted, taking the orchestrator's memory of what
ran with it.

The obvious fix is Postgres. This repository already runs one: the CDC source
database, the thing Debezium reads from. Reusing it would have cost nothing in
new infrastructure.

It would also have created a circular dependency. The source database going down
is exactly the incident the orchestrator needs to survive in order to report it.
Coupling them means the failure that matters most is the failure that also
blinds you to it.

## Decision

A separate `dagster-postgres` service in `docker-compose.yml`, with its own
volume (`dagster_postgres_data`), and **without** `wal_level=logical` — it is not
a CDC source and should not carry the replication configuration of one.

## Rationale

The data source and the orchestrator must be able to fail independently. That is
the whole of it: one container more, in exchange for the orchestrator staying up
to record what happened to the source.

Not setting `wal_level=logical` is deliberate rather than incidental. It makes
the two Postgres instances visibly different in configuration, so that neither is
mistaken for the other, and it keeps Debezium from ever being pointed at the
wrong one by accident.

## Alternatives considered

1. **Reuse the CDC source Postgres** — rejected for the circular dependency
   above.
2. **SQLite with WAL mode enabled** — WAL tolerates low concurrency and would
   probably have held. Rejected as too fragile for a project meant to serve as a
   client reference implementation: "probably holds" is not a property you
   demonstrate to someone.

## Consequences

- One more service to operate, with its own volume to back up.
- `run_retention` is still undefined in `dagster.yaml`. The run history grows
  without bound. Recorded as debt at design time and still open.

## See also

- `.claude/sdd/features/DESIGN_GOVERNANCA_CUSTO_DISPARO.md` — Decision 3, which
  supersedes this ADR's original text
- `docker-compose.yml` — the `dagster-postgres` service, which cites this ADR by
  number in two comments
