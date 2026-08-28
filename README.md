# sdd-kafka-snowflake-2

End-to-end **Change Data Capture** pipeline: PostgreSQL → Debezium → Kafka → Snowflake, with layered dbt modeling, Dagster orchestration, and observability through Prometheus + Grafana.

What sets this project apart is not the stack — it is the **control**. Each domain's CDC strategy lives in a metadata table in Snowflake rather than in SQL; pipeline triggering passes through a gate that queries Kafka before waking the warehouse; and every layer's invariants are tested, with severity chosen case by case.

> Source code comments and commit messages are written in Portuguese. This README is in English.

---

## TL;DR

A food-delivery company changes a row in Postgres — an order is placed, a driver
starts a shift, a payment closes. This project makes that change appear in
Snowflake, cleaned, typed and tested, without anyone running anything and without
a warehouse burning credits while it waits.

It is an end-to-end CDC pipeline for the Uber Eats Brazilian market: **10 domains
from PostgreSQL through Debezium and Kafka into Snowflake**, modelled in dbt
across Bronze/Silver/Gold, orchestrated by Dagster, with **185 tests** and a cost
gate that costs nothing when idle.

What sets it apart is not the stack — it is the **control**. Each domain's CDC
strategy lives in a metadata table rather than in SQL; the pipeline is woken by
Snowflake itself rather than by polling; and every layer's invariants are tested,
with severity chosen case by case rather than left at the default.

---

## The Problem

A CDC pipeline has two failure modes that do not look like failures.

**The first is cost.** The obvious way to know whether new data arrived is to ask,
and asking means waking a warehouse. The original design polled `MAX(create_time)`
across 20 Bronze tables every 60 seconds against a warehouse with
`AUTO_SUSPEND=60s`: the two periods collided and the warehouse was effectively
never idle. The account paid, continuously, to discover that nothing had happened.

**The second is silence.** Data that is wrong but plausible flows to the end of the
pipeline and gets reported. A schema drifts and a column is inferred one type
narrower. A low-volume domain falls behind a shared watermark and stops being
processed — its rows are not lost, they are invisible, which is worse, because
nothing raises a hand.

This repository is the answer to both, and its history is mostly the record of
finding those failures rather than avoiding them: a monitor everyone believed in
that did not exist, a deploy workflow that ran nine times and failed nine times
unread, a severity convention applied to 102 tests a day before it was accepted.
Each one is written down in [`docs/adr/`](docs/adr/) or under
[Known gaps](#known-gaps-and-unverified-claims), including the ones that make the
project look worse.

> **Dataset framing:** 215,082 records reached the landing tables across the ten
> domains — and 210,005 of them are `order_items`. The other nine hold fewer than
> 2,300 rows each. That is an architectural microcosm, not a production volume:
> what is being validated is idempotency, trigger economics and contract
> governance, the shape that has to hold when real volume arrives.
>
> Counted in Snowflake on 2026-08-28. The seed files under `tests/data/` are not
> versioned, and the local copy is smaller than what was loaded — count the
> warehouse, not the directory.

---

## Stack

| Layer | Technology | Decision |
|---|---|---|
| Source | PostgreSQL 15 (Docker) | `wal_level=logical` for Debezium CDC |
| CDC | Debezium 2.x | Change events per row, per domain |
| Broker | Kafka 3.x (Confluent 7.5.0) | With Zookeeper — matches the Confluent image set |
| Schema | Confluent Schema Registry 7.5.0 | **Avro + `BACKWARD`** — the contract boundary ([ADR-0030](docs/adr/0030_avro_and_schema_registry_as_the_contract.md)) |
| Ingestion | Snowflake Kafka Connector **v4** | Snowpipe Streaming, row-level commits ([ADR-0029](docs/adr/0029_snowpipe_streaming_as_the_ingestion_path.md)) |
| Schematization | `enable.schematization=true`, `validation=client_side` | Native typed columns, types from the registry ([ADR-0021](docs/adr/0021_kafka_connector_v4_schematization.md)) |
| Warehouse | Snowflake, `CDC_WH` | `AUTO_SUSPEND=60s`; `cdc_poc_monitor` as the credit brake ([ADR-0020](docs/adr/0020_resource_monitor_canonicalization.md)) |
| Trigger gate | Streams + Triggered Tasks | `SYSTEM$STREAM_HAS_DATA` evaluated free in the control plane ([ADR-0019](docs/adr/0019_streams_and_triggered_tasks_as_the_gate.md)) |
| Transformation | dbt-snowflake 1.7.5 | 26 models, 185 tests, severity per test ([ADR-0027](docs/adr/0027_severity_convention.md)) |
| CDC strategy | `CONFIG.TABLE_METADATA` + `resolve_cdc` | Strategy is data, not SQL — fed from the Avro `doc` field |
| Orchestration | Dagster 1.6 | Sensors gated on Prometheus before touching Snowflake |
| Dagster storage | PostgreSQL 15, dedicated | Separate from the CDC source, no circular dependency ([ADR-0018](docs/adr/0018_dedicated_postgres_for_dagster_storage.md)) |
| Observability | Prometheus 2.49 + Grafana 10.2 | Broker metrics via JMX and kafka-exporter |
| CI | GitHub Actions | `ruff` + `yamllint` + `dbt parse` with throwaway credentials |
| Methodology | Claude Code + AgentSpec | 5-phase SDD, artifacts under `.claude/sdd/` |

---

## Status

| Component | State | Last verified |
|---|---|---|
| CDC ingestion (Debezium → Kafka → Snowflake Sink) | Operational | 2026-08-10 |
| Bronze layer (10 dbt models) | Materialized | 2026-08-10 |
| Silver layer (10 dbt models) | Materialized | 2026-08-10 |
| Gold layer (6 dbt models) | Materialized | 2026-08-10 |
| dbt tests (185) | 0 errors, 10 warnings | 2026-08-10 |
| Full build + test | 63 s of dbt time (26 models, 185 tests) | 2026-08-10 |
| CI (`.github/workflows/ci.yml`) | Green on its first run, 45 s | 2026-08-11 |
| CD (`.github/workflows/deploy.yml`) | Failed 9 of 9 runs before being disabled; cannot deploy by design | 2026-08-07 → 08-10 |

All 10 warnings are referential-integrity checks traced back to the source database — see [Data quality](#data-quality).

---

## What works, and what is next

The table above is dated evidence; this is what it adds up to. Read this section
before the detail — [Cost governance](#cost-governance), [Data quality](#data-quality)
and [Known gaps](#known-gaps-and-unverified-claims) each go deep on one part of it.

### What works

| | Evidence |
|---|---|
| **End-to-end CDC**, PostgreSQL → Debezium → Kafka → Snowflake, for the 10 Tier-1 domains | 10 Snowflake-managed pipes in `BRONZE`, `kind = STREAMING`, verified 2026-08-12 ([ADR-0029](docs/adr/0029_snowpipe_streaming_as_the_ingestion_path.md)) |
| **26 dbt models** across Bronze, Silver and Gold | Materialized 2026-08-10; full build and test in 63 s |
| **185 tests**, every one classified by severity rather than by default | 0 errors, 10 warnings — all 10 traced to referential gaps in the source ([ADR-0027](docs/adr/0027_severity_convention.md)) |
| **Idle costs nothing.** The gate evaluates `SYSTEM$STREAM_HAS_DATA` in Snowflake's control plane; a false answer engages no warehouse | [ADR-0019](docs/adr/0019_streams_and_triggered_tasks_as_the_gate.md); the whole ingestion path measured at 0.0005 credits |
| **A credit brake exists.** `cdc_poc_monitor` — 20 credits, `MONTHLY`, warehouse level, `NOTIFY` at 50/75%, `SUSPEND` at 90%, `SUSPEND_IMMEDIATE` at 100% | Created 2026-08-11 after verification found the account had no monitor at all ([ADR-0020](docs/adr/0020_resource_monitor_canonicalization.md)) |
| **Adding a domain needs no code change.** Register the Avro subject with a populated `doc` and the sensor, `CONFIG.TABLE_METADATA` and `resolve_cdc` do the rest | [ADR-0030](docs/adr/0030_avro_and_schema_registry_as_the_contract.md) |
| **CI runs and is green**, on a workflow that needs no Snowflake, Kafka or host | `lint` + `validate`, 2026-08-11 |

### What is next

Ordered by what would hurt most if left alone.

| | Why it matters | Where |
|---|---|---|
| **There is no deploy target.** `deploy.yml` runs `docker compose up` inside an ephemeral runner; the stack dies with the job | The repository cannot deploy anywhere, by design and not by misconfiguration. It needs an SSH host or a registry plus a remote orchestrator before the workflow means anything | [CI/CD](#cicd) |
| **The gate solves the bursty case, not the streaming case** | Under continuous traffic, ten Tasks firing every minute keep `CDC_WH` on permanently — the 60-second minimum per resume — which is the behaviour the redesign existed to remove. This workload is bursty, so it holds today | [Cost governance](#cost-governance) |
| **`scripts/00_account_bootstrap.sql` has never been executed as a whole** | It is the only description of the account's shape. The `CREATE PIPE` omission found on 2026-08-12 is what an unrun script looks like: rebuilding from git would have produced working dbt and dead ingestion | [Snowflake scripts](#snowflake-scripts) |
| **No regression test for the watermark bug** | `AT-003` specifies exactly the scenario — a high-volume domain advancing past a low-volume one — and the bug has already happened once | [ADR-0024](docs/adr/0024_sensor_without_global_watermark.md) |
| **Bronze append-only is a convention, not a constraint** | Three mechanisms depend on it silently. The fix is one statement: `REVOKE UPDATE, DELETE ON ALL TABLES IN SCHEMA BRONZE FROM ROLE CDC_ROLE` | [ADR-0025](docs/adr/0025_bronze_is_append_only.md) |
| **Workload separation is undecided.** A second warehouse (`CDC_WH_TRANSFORM`) sits in the `DEFINE` as deferred, awaiting context | Everything shares `CDC_WH` today — ingestion, transformation and any BI query compete for the same compute and the same 60-second minimum. There is no ADR because no choice has been made | `DEFINE_GOVERNANCA_CUSTO_DISPARO.md` |
| **`run_retention` is undefined in `dagster.yaml`** | Run history grows without bound on the dedicated Postgres | [ADR-0018](docs/adr/0018_dedicated_postgres_for_dagster_storage.md) |
| **The Dagster asset graph is frozen at import** | Adding a dbt model requires restarting `dagster-daemon`; without it the pipeline runs successfully while ignoring the new layer. This happened on 2026-08-10 | [Operational fragility](#operational-fragility) |

Nothing above is a disclaimer. Each line is either a decision waiting on context
or a specific piece of work, and the ones that were closed are recorded in
[Known gaps](#known-gaps-and-unverified-claims) with what the call cost.

---

## Architecture

```
┌──────────────┐    WAL     ┌──────────────┐          ┌──────────────────┐
│  PostgreSQL  │──────────► │   Debezium   │────────► │      Kafka       │
│   (source)   │            │    source    │          │  10 CDC topics   │
└──────────────┘            └──────────────┘          └────────┬─────────┘
                                                               │
                                          ┌────────────────────┴────────┐
                                          │      Schema Registry        │
                                          │  (contracts + schematization)│
                                          └────────────────────┬────────┘
                                                               │
                                              ┌────────────────▼─────────────┐
                                              │  Snowflake Sink Connector    │
                                              │  (Snowpipe Streaming)        │
                                              └────────────────┬─────────────┘
                                                               │
┌──────────────────────────────────────────────────────────────▼─────────────┐
│  Snowflake · CDC_POC                                                       │
│                                                                            │
│   BRONZE ──────────────► SILVER ──────────────► GOLD                       │
│   raw append-only CDC    current entity state   6 aggregations             │
│   (incremental/merge)    (resolve_cdc)          (payments, drivers,        │
│                                                  restaurants, users)       │
│                                                                            │
│   CONFIG.TABLE_METADATA ── per-domain CDC strategy ─────────────┘          │
└────────────────────────────────────────────────────────────────────────────┘
              ▲                                            ▲
              │ orchestration                              │ metrics
      ┌───────┴────────┐                          ┌────────┴─────────┐
      │    Dagster     │◄─── cost gate ───────────│    Prometheus    │◄── JMX +
      │ sensors + jobs │     (checks Kafka)       │     Grafana      │  Kafka Exporter
      └────────────────┘                          └──────────────────┘
```

---

## Domain Map (10 domains)

Ten tables travel the full pipeline, from Postgres to Gold:

| Domain | Type | Key | Role |
|---|---|---|---|
| `orders` | entity | `order_id` | Hub — links the others by CPF, CNPJ and `driver_id` |
| `order_items` | fact | `order_item_id` | Largest volume (210k rows) |
| `payment_events` | fact | `event_id` | Payment lifecycle (event sourcing) |
| `users_mongo` | entity | `uuid` | Users (MongoDB origin); joins on CPF |
| `users_mssql` | entity | `uuid` | Extended profile (MSSQL origin); same CPF |
| `restaurants` | entity | `uuid` | Joins to `orders` on CNPJ |
| `drivers` | entity | `uuid` | Joins on `driver_id` |
| `driver_shifts` | entity | `shift_id` | Driver shifts |
| `search_events` | log | `search_id` | User searches |
| `recommendations` | log | `event_id` | ML recommendation events |

---

## Engineering decisions and tradeoffs

| Decision | Alternative considered | Why this |
|---|---|---|
| Snowpipe Streaming (v4 connector) | Classic file-based Snowpipe; `COPY INTO` from a stage on a schedule | Row-level commits are what let the trigger gate observe the landing instead of guessing at it. Measured cost of the whole ingestion path: 0.0005 credits |
| Avro + Schema Registry at `BACKWARD` | JSON without a registry (Debezium's default); types hardcoded in the Bronze models | One artifact serves three consumers: the sink's type authority, the compatibility gate, and — via the schema `doc` field — each domain's CDC strategy in `CONFIG.TABLE_METADATA` |
| Snowflake Streams + Triggered Tasks as the trigger gate | Custom Kafka watcher calling Dagster's GraphQL API | The Stream sits on the Bronze table, so it can only fire *after* Snowpipe commits — no heuristic debounce for the publish-vs-materialize gap. A false `WHEN` costs nothing |
| Dedicated `dagster-postgres` | Reuse the CDC source Postgres | The source going down is the incident the orchestrator has to survive in order to report it |
| Sensor filters on `consumed = FALSE` only | Global `detected_at` watermark cursor | A watermark is only safe when production is ordered; ten independent Tasks are not. The cursor had already made a low-volume domain invisible once |
| Connector V4, `validation=client_side` | V4 default `server_side` | Client-side reads types from the Schema Registry the project already curates; server-side infers from the first record, with a documented risk of demoting `FLOAT64` to `NUMBER(38,0)` |
| Upper-case identifier normalization | Preserve the original Avro case | Preserving case means double-quoting every column in every dbt model — a permanent tax on all downstream SQL |
| Ingest 10 domains, not 20 | Ingest all 20, skip Tier 2 in dbt | V4 bills on ingestion volume, not transformation. Skipping only the models saves nothing |
| `RECORD_METADATA` kept | Synthetic ingestion timestamp in Bronze | It is the watermark *and* the dedup tiebreaker; Kafka offset is the only strictly ordered value per partition. Verified present in the 4.1.0 JAR after the design assumed it gone |
| Bronze append-only, by convention | Rely on nobody issuing UPDATE | Three mechanisms already depended on it silently — `APPEND_ONLY` Streams, the `op != 'd'` filter, 1-day Time Travel |
| Severity chosen per test | Leave all 185 at the `error` default | A test is `error` if the violation, propagated, would make a business metric objectively wrong. Referential gaps inherited from the source degrade a metric; they do not falsify it |

> Full write-ups, with the alternatives and what each one cost, in
> [`docs/adr/`](docs/adr/) — 12 records. The numbering is inherited from the
> predecessor repository and cited by number in `docker-compose.yml`,
> `scripts/streams_and_tasks.sql` and `dagster/pipeline/sensors.py`.

---

## Layered modeling

### Bronze — raw CDC, idempotent

One model per domain, `incremental` with `merge`. Columns arrive **typed and uppercased** (the sink runs with `snowflake.enable.schematization=true`), so there is no VARIANT extraction. Each model:

- deduplicates by key within the batch, ordering by `source_ts_ms DESC, kafka_offset DESC`;
- uses `RECORD_METADATA:CreateTime` as the incremental watermark;
- **discards the Kafka tombstone.** The connector runs with `drop.tombstones=false`, so every DELETE emits two messages: the row carrying `__OP='d'`, and then a null-valued one that the sink materializes as an entirely null row. Without the `WHERE <key> IS NOT NULL` filter, the MERGE never matches on a null key and each DELETE leaves permanent garbage behind.

Control columns preserved across every layer: `op`, `source_ts_ms`, `kafka_offset`, `kafka_partition`, `kafka_created_at` — the lineage tying each row to the Kafka event that produced it.

**Delete rows and `not_null`.** A Debezium DELETE event carries **only the primary key** — every payload column arrives null. Bronze keeps those rows on purpose: they are the CDC history, and they are what Silver reads to decide what to discard. So an unscoped `not_null` on a payload column fails the moment a real DELETE is retained — not because the data is wrong, but because the protocol is behaving correctly. The 13 payload `not_null` tests therefore carry `where: "op IS DISTINCT FROM 'd'"`, which keeps the invariant and `error` severity while restating it as "a row that has **not** been deleted must have this column". The 50 tests on primary keys and control columns stay unscoped: both remain populated on a delete row.

### Silver — current state, metadata-driven

No Silver model contains CDC logic. They all have the same shape:

```sql
{{ resolve_cdc(ref('bronze_orders')) }}
```

The [`resolve_cdc`](dbt/macros/resolve_cdc.sql) macro reads the domain's strategy from `CONFIG.TABLE_METADATA` and resolves history accordingly:

| Strategy | Behavior |
|---|---|
| `upsert` | One row per key, most recent version; DELETE is discarded |
| `append` | No deduplication; only DELETE is removed |
| `log` | Nothing is discarded, DELETE included — historical record |

Changing a domain's strategy is an `UPDATE` on the metadata table, not a SQL deploy.

Three decisions worth reading before touching this:

- **Tie-breaking on `kafka_offset`.** Ordering by `source_ts_ms` alone (milliseconds) ties on cascading updates and batch loads, making `ROW_NUMBER` non-deterministic — the same `dbt run` could produce a different Silver.
- **`op IS DISTINCT FROM 'd'`, not `op != 'd'`.** In SQL, `NULL != 'd'` is NULL, not TRUE: the naive filter silently dropped every row with a null `op`.
- **`materialized='table'`, not `incremental`.** MERGE does not delete rows. In an incremental model a key deleted at the source would survive in Silver forever; with a rebuild it simply stops appearing. The cost is scanning Bronze on every run — cheap at current volume, and if it grows the answer is partitioned `delete+insert`, not `merge`.

The [`get_table_config`](dbt/macros/get_table_config.sql) macro loads the metadata with a static fallback, because the Dagster entrypoint runs `dbt parse` offline: without a connection, `execute` is false and the `config()` key would come out empty in the manifest.

### Gold — six aggregations

Each Gold model has its **own grain**, and the grain is the thing worth protecting: it is what breaks silently when someone removes a guard clause.

Layer defaults in `dbt_project.yml` state what each layer actually uses — `incremental` for bronze, `table` for silver and gold. Until 2026-08-10 all three declared `incremental` + `merge`, inherited from when the folders were empty, and no silver or gold model obeyed it. That was not dead letter but a trap: a new model dropped into `models/silver/` would inherit `merge` with no `unique_key`, and in that combination dbt degrades to a plain append, silently reinserting everything on each run.

| Model | Materialization | Grain | Aggregation pattern |
|---|---|---|---|
| `gold_payment_lifecycle` | incremental | `payment_id` | Additive-partitionable — reference pattern |
| `gold_payments_by_status` | incremental | `event_name` | Global ratio, low cardinality |
| `gold_payment_funnel` | table | funnel stage | Global ratio |
| `gold_driver_performance` | table | `driver_id` | Additive-partitionable |
| `gold_revenue_per_restaurant` | table | `restaurant_id` | Additive-partitionable |
| `gold_user_behavior` | table | `cpf` | Additive-partitionable, cumulative |

**The incremental pattern.** In `gold_payment_lifecycle` the watermark identifies *which* payments changed — it does not filter the rows that feed the aggregation. Once the affected `payment_id` values are known, the full history of each is re-read. Otherwise a lone `closed` event would produce a row with no `created_at`, and the MERGE would overwrite the good version with a mutilated one.

**Fan-out defenses.** `silver_drivers`, `silver_restaurants` and `silver_users_mongo` guarantee uniqueness on the **technical** key (`uuid`), not on the **business** key used in joins (`driver_id`, `restaurant_id`, `cpf`). In this dataset 95 CPFs have more than one registration. Left untreated, every order would be counted twice and `gasto_total` would inflate up to 2×. The three affected models collapse the dimension to one row per business key with `QUALIFY`, using the same deterministic criterion as `resolve_cdc`, and the `unique` test on the grain exists as the guard for that decision.

In `gold_user_behavior` the `cpf → user_id` bridge is deliberately **not** deduplicated — only the descriptive attributes are. Searches and recommendations belonging to a CPF's secondary `user_id` values still get counted.

**Orphans are flagged, not dropped.** Every fact-to-dimension join is a LEFT JOIN starting from the fact, with a `sem_cadastro` flag. An INNER JOIN would erase 55 drivers and 27 restaurants from the report without a trace.

---

## Cost governance

The warehouse is the expensive part of the account, and the pipeline is designed not to wake it without reason.

The trigger path has four hops — Kafka metrics in Prometheus, then ten Snowflake Streams with their gate Tasks, then `CONFIG.PENDING_RUNS`, then the Dagster sensor, then dbt. Each hop earns its place, but the chain has no end-to-end health check: if the gate tasks stop, or `PENDING_RUNS` stops being written, the pipeline goes quiet and *looks* fine. That is not hypothetical — on 2026-08-11 the sink sat with four dead tasks while Debezium stayed green, and the only reason anyone noticed was that someone was waiting for a specific row to appear.

Both Dagster sensors (`bronze_new_data_sensor` and `registry_new_subject_sensor`, 60-second interval) **query Prometheus before Snowflake**. With no new Kafka traffic, the sensor skips without opening a connection:

```
Sensor bronze_new_data_sensor skipped: Sem atividade no Kafka (via Prometheus) — Snowflake não consultado.
```

One limit of the gate design is worth stating before anyone calls it production-ready. The ten gate Tasks evaluate `SYSTEM$STREAM_HAS_DATA` for free, which is what makes idleness cost nothing — but when they fire they run on `CDC_WH`, which bills a 60-second minimum per resume. Under *continuous* traffic, ten tasks firing every minute keep the warehouse on permanently, which is the exact behaviour the redesign existed to remove. The design solves the bursty case, and this workload is bursty; it does not solve the streaming case.

At rest, running this pipeline costs zero credits. The design is completed by 1-day Time Travel on Bronze tables, now declared at database level rather than inherited, and by `cdc_poc_monitor` — a spending cap of 20 credits per month bound to `CDC_WH`, which notifies at 50% and 75%, suspends at 90% and suspends immediately at 100%.

**Both of those became true on 2026-08-11, and neither was true before.** For a week this section claimed an account-level Resource Monitor that did not exist. Running `scripts/verify_governance.sql` as `ACCOUNTADMIN` settled it: `SHOW RESOURCE MONITORS` returned zero rows, the account parameter was unset, and the warehouse's own field was null. All three places a spending cap could live were empty. The monitor described above was then created by `scripts/snowflake_setup.sql` — a file the README had referenced for just as long without it existing either.

The alerts became real on 2026-08-11. `NOTIFY_USERS` now points at the account's human login, whose email reports `IS_EMAIL_VERIFIED = true` — and that second condition is the one that matters, because `NOTIFY_USERS` only delivers to verified addresses. The three service identities have no email at all, so listing them would have been silently useless.

The ordering here is a trap worth naming. Setting `NOTIFY_USERS` against an unverified address is accepted without error: the monitor looks configured, and nothing is delivered. That is worse than leaving it unset, because it manufactures confidence. Verify first — only Snowsight triggers verification, while `ALTER USER … SET EMAIL` fills the field and leaves `IS_EMAIL_VERIFIED` false. Until this was done, the 50% and 75% triggers were decorative and the first signal this account gave was the warehouse suspending at 90%.

The same run surfaced something nobody had looked at: **both** warehouses have `ENABLE_QUERY_ACCELERATION = true` with a scale factor of 2, a path that bills credits beyond the warehouse's own compute, switched on by default rather than by decision. `QUERY_ACCELERATION_HISTORY` was then checked over 30 days and returned **nothing** — it has never engaged once, anywhere, which is what the scan sizes here predict. It is a cost path with no demonstrated benefit.

**Disabled on both warehouses on 2026-08-11**, confirmed by `SHOW WAREHOUSES` reporting `enable_query_acceleration = false` on `CDC_WH`. `COMPUTE_WH` had `AUTO_SUSPEND` cut from 300 to 60 in the same pass.

### What it actually costs

`CDC_WH` is an **X-Small** (1 credit/hour), billed per second with a **60-second minimum per resume**, `auto_suspend=60`.

Measured on 2026-08-10, across six hours that included building all three layers from scratch and running the full test suite several times:

```
579 queries · 72.7 seconds of execution time  ≈ 0.02 credits of pure compute
```

That figure is query time. The invoice for the same day, read from `ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY` as `ACCOUNTADMIN` on 2026-08-11, was **1.1188 credits** — fifty-six times more. Both numbers are correct and they measure different things: one is time spent executing, the other is time spent switched on. At the nominal X-Small rate that billed day is a little over an hour of warehouse uptime to run 72.7 seconds of queries, so **execution accounts for under 2% of what was actually paid.** Everything else is the 60-second minimum and the idle tail before auto-suspend. It is the sharpest available evidence for the argument below, and it was invisible until someone with `ACCOUNTADMIN` looked.

`CDC_WH` consumption since the warehouse was created on 2026-08-06 — one warehouse, not the account, which was already metering credits on 2026-08-04:

| Day | Credits |
|---|---|
| 2026-08-06 | 0.0252 |
| 2026-08-07 | 0.3846 |
| 2026-08-08 and 2026-08-09 | **absent from the results — zero** |
| 2026-08-10 | 1.1188 |
| 2026-08-11 | 0.1954 |
| **Total** | **≈ 1.72** |

The two missing days are the important ones. Nobody worked that weekend, the stack was up, and the warehouse billed nothing at all — which turns "at rest, this pipeline costs zero credits" from a claim about sensor behaviour into a measured fact about the invoice. The near-real-time cross-check in `INFORMATION_SCHEMA` agreed with the billed figures to four decimal places for the current day, so `ACCOUNT_USAGE` is not lagging here.

**That is one warehouse, not the account.** `METERING_HISTORY`, grouped by service on the same day, puts the whole account at roughly **3.8 credits** over 30 days, and the breakdown is not what this project's documentation would lead you to expect:

| Service | Credits | |
|---|---|---|
| `WAREHOUSE_METERING` | 3.4724 | all warehouses |
| `SNOWFLAKE_COCO_SNOWSIGHT` | 0.3474 | the cost of using the UI |
| `SNOWPIPE_STREAMING` | 0.0005 | this pipeline's entire ingestion path |
| `TELEMETRY_DATA_INGEST` | 0.0001 | |
| `PIPE` | 0.0000 | classic Snowpipe, unused |

Two things fall out of that table. The first is a correction: an earlier draft of this section warned that serverless ingestion was unmeasured and "not a rounding error". It is a rounding error — 0.0005 credits, thirteen thousandths of one percent. Snowpipe Streaming bills by throughput, and 2,211 events is not throughput. The instinct to distrust an unmeasured number was right; the guess about its size was wrong, and the measurement is what settles it.

The second is larger. Splitting warehouse metering by name gives `COMPUTE_WH` **1.8214** against `CDC_WH` **1.8076** — the default warehouse behind Snowsight worksheets costs marginally *more* than the entire data pipeline, and metering starts on 2026-08-04, two days before `CDC_WH` existed. Roughly half of this account's compute has nothing to do with this project. `cdc_poc_monitor` caps `CDC_WH` and only `CDC_WH`, so the larger consumer runs with no ceiling at all; an account-level monitor, or a second one bound to `COMPUTE_WH`, is what would actually close that. Three other warehouses exist besides those two — `SNOWFLAKE_LEARNING_WH`, `SYSTEM$STREAMLIT_NOTEBOOK_WH` and the `CLOUD_SERVICES_ONLY` bucket — none of them monitored either.

**The single highest-return change available was one number on a warehouse nobody was looking at.** `COMPUTE_WH` ran with `AUTO_SUSPEND = 300`, five times the 60 seconds set on `CDC_WH`. Every worksheet query kept it burning for five minutes after it finished, and interactive querying is exactly the pattern where the idle tail dwarfs the work. A short query with a 300-second tail costs about 0.085 credits; the same query at 60 seconds falls to the billing floor, roughly 0.017. Against the 1.82 credits already spent there, that is on the order of 1.4 credits — near 37% of everything this account had consumed, in one statement, applied on 2026-08-11:

```sql
ALTER WAREHOUSE COMPUTE_WH SET AUTO_SUSPEND = 60;
```

The saving is a projection from the billing arithmetic, not a measurement. Confirming it means re-reading `METERING_HISTORY` after a few days of normal use and comparing the daily `COMPUTE_WH` figure against the 1.82 credits it accumulated in its first six.

The trade-off is real but small here. Suspending sooner means more resumes, each carrying its own 60-second minimum, so the saving shrinks toward nothing if queries arrive less than a minute apart. A suspended warehouse also drops its local disk cache, which slows repeated exploration of the same table — on tables of a few thousand rows, that cache is not worth the uptime it costs.

Worth noticing what this says about the account as a whole: both warehouses are X-Small Gen2, both have query acceleration on with scale factor 2, and neither was monitored. None of that was chosen. The 60-second `AUTO_SUSPEND` on `CDC_WH` is the only deliberate setting in the group, applied to the one warehouse somebody thought to examine.

One caveat on the rate. `CDC_WH` reports `resource_constraint = STANDARD_GEN_2`, a second-generation warehouse, and the "1 credit/hour" above is the classic X-Small rate. Anything in this section derived from that rate — the uptime figure in particular — should be re-checked against Snowflake's current rate card before being quoted to anyone. The credit totals themselves come straight from the billing view and do not depend on it.

Full project, end to end:

| | 4 threads | 8 threads |
|---|---|---|
| `dbt run` (26 models) | 28.0 s | **25.6 s** |
| `dbt test` (185 tests) | 59.8 s | **37.4 s** |

The counter-intuitive part, and the reason no model is incrementalized beyond the two that genuinely need it: **the full rebuild is not the cost.** With a 60-second minimum per resume, a 7-second model build and a 0.5-second one bill identically. On the numbers above, the idle tail dominates the invoice — optimizing the rebuild would attack seconds that are already paid for.

The lever that does move the needle is the **number of resume episodes**, not the duration of each. That is exactly what the sensor gate protects. Raising `threads` from 4 to 8 helped for the same reason: the bottleneck is statement count divided by concurrency, not data volume — which is why tests (independent, 185 of them) gained 37% while models (constrained by the bronze → silver → gold DAG) gained only 9%.

The trigger for revisiting this is wall time crossing 60 seconds in a single invocation, not table size. If `silver_order_items` ever gets there, the answer is `incremental` with partitioned `delete+insert` — **not** `merge`, which would reintroduce the deleted-key-survives-forever bug.

The figures above are **execution time**, not billed credits — see [Known gaps](#known-gaps-and-unverified-claims) for what could not be measured and why.

---

## Data quality

188 dbt tests: 83 in Bronze, 72 in Silver, 30 in Gold, and 3 singular tests that check something the other 185 cannot.

### What the schema tests could not see

The 185 column tests — `unique`, `not_null`, `accepted_values`, `relationships` — all verify the *shape* of the data. None of them verifies that Gold reflects Silver, which is precisely the question an incremental model answers wrongly when it breaks.

That gap had a cost. On 2026-08-11 the `alvo` CTE in `gold_payment_lifecycle` skipped an event that arrived out of order: Silver held 4 events for a payment while Gold kept reporting 3, with `capturado_em` null. All 26 tests in the chain passed and dbt reported success. A test suite that cannot distinguish *correct Gold* from *stale Gold* is not testing the hardest thing the project does.

Three singular tests in `dbt/tests/` close that:

| Test | What it asserts | Why |
|---|---|---|
| `assert_gold_lifecycle_reconcilia_silver` | Event count and latest-event watermark match per `payment_id`, in both directions | Would have failed on the bug above |
| `assert_bronze_sem_backlog_do_landing` | Every distinct key the sink landed exists in the matching Bronze model, across all 10 domains | Catches "sink delivered, dbt never processed" and a stuck incremental watermark |
| `assert_table_metadata_sem_drift` | `CONFIG.TABLE_METADATA` matches what this commit expects, for the four columns that change behaviour | `cdc_strategy` is read at compile time, so a row edited in Snowflake changes a model's semantics with no diff in git |

The second one deserves a note on what it deliberately is *not*. `models/config/sources.yml` has declared freshness thresholds (warn 5 min, error 15 min) since it was written, and nothing in this project has ever run `dbt source freshness` — orphaned configuration. Wiring it up was the obvious move and would have been wrong: a clock-based test cannot tell "the stack is off on purpose" from "ingestion is broken", and this stack is off most of the time, so the check would live red. An alarm that is always red is the problem it was meant to solve. The backlog test asks the question that has an objective answer with the stack stopped.

Silver tests what Bronze cannot guarantee — the invariants CDC resolution adds:

1. **`unique` + `not_null` on the key.** In Bronze, `unique` passes thanks to per-batch deduplication; in Silver it holds across the entire history.
2. **`accepted_values` on `op` without `'d'`.** This is the test of the delete filter. If a domain's strategy switches to `log`, this test breaks on purpose.
3. **`not_null` on the ordering columns** (`source_ts_ms`, `kafka_offset`) — a null there means non-deterministic tie-breaking is back.

Gold tests the **grain** of each model, in `error` severity. That is the only test that catches fan-out returning if a `QUALIFY` is removed.

### Severity convention

| Severity | When | Example |
|---|---|---|
| `error` | Invariant guaranteed by this repository's code | `unique` on an entity key or model grain |
| `warn` | Referential integrity across domains | `order_items.order_id → orders` |

The reason for `warn` is concrete: the ten CDC streams are independent and have their own snapshot timing. An order arriving before its driver is normal latency, not a defect — failing the pipeline over it would be a false positive.

### Open findings (2026-08-10)

Warnings were traced back to the source, and **none is a pipeline defect**:

| Finding | Measurement | Source verification |
|---|---|---|
| 7,246 `order_items` rows with no matching order (85 distinct `order_id`) | None of those IDs exists in `bronze_orders` — not an effect of the delete filter | Source Postgres has exactly the same 7,246: `order_items` references 491 orders while the `orders` table holds only 414. The seeded database has no foreign key |
| 95 duplicate CPFs in `users_mongo` | `uuid` is unique, but the same person appears under several | The source holds 412 users across 216 distinct CPFs. Handled in Gold by collapsing the dimension before joining |
| `payment_id` cardinality is degenerate | 2,210 events spread over 8 distinct `payment_id`, with **zero** overlap against `orders.payment_key` | Property of the synthetic generator. The three payment models are structurally correct, but their numbers carry no business meaning on this dataset |
| `gold_user_behavior` covers 295 of 414 orders | The 119 missing ones are exactly the orphans of `orders.user_key → users_mongo.cpf` | Orders whose CPF has no user record; `gasto_total` is revenue attributable to a known user, not total revenue |

Silver reproduces the source row for row: 414 orders, 210,002 items, the same 7,246 orphans.

### Closed findings

| Finding | How it surfaced | Resolution |
|---|---|---|
| 3 `not_null` failures in Bronze | First full-project `dbt test` — the Bronze suite had not been run in months, so two of the three came from a DELETE that predated the session | Payload `not_null` tests scoped with `where: "op IS DISTINCT FROM 'd'"`. See the Bronze section |
| `event_type` vocabulary was wrong | An `accepted_values` warning | The list came from a code comment, not from the data. Measured and corrected in all four places that carried it |
| `table_type='log'` contradicting `cdc_strategy='upsert'` | Reading the metadata table | Both domains are append-only at the source (203 and 255 rows for the same number of distinct keys, zero deletes). The **label** was wrong, not the strategy; corrected in the seed, the fallback and the account, with an audit row in `METADATA_HISTORY` |

---

## Repository layout

```
connectors/           Debezium source + Snowflake sink (JSON configuration)
dagster/pipeline/     dbt assets, jobs, cost-gated sensors, resources
dbt/
  macros/             resolve_cdc, get_table_config, generate_schema_name
  models/bronze/      10 incremental models + schema.yml (83 tests)
  models/silver/      10 models via resolve_cdc + schema.yml (72 tests)
  models/gold/        6 aggregations + schema.yml (30 tests)
  models/config/      sources.yml
observability/        Prometheus (scrape + alerts), JMX exporter
scripts/              CONFIG schema bootstrap, streams/tasks, roles, governance
tests/                load generator for the source Postgres
docs/adr/             architecture decision records (12)
.claude/sdd/          specification workflow records (define → design → build → ship)
Makefile              stack, data loading, dbt and quality targets
.env.example          every variable the stack reads, with the secrets blank
```

---

## Running it

### Prerequisites

- Docker and Docker Compose
- A Snowflake account with RSA key-pair authentication
- `keys/` holding the private `.p8` key (outside version control)

### Configuration

Copy [`.env.example`](.env.example) to `.env` at the repository root and fill it in. `.env` is gitignored and must never be committed — CI has a step that fails if it ever is:

```bash
# Source PostgreSQL
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=
DATABASE_URL=

# Snowflake (key pair)
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PRIVATE_KEY_PATH=
SNOWFLAKE_DATABASE=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_ROLE=
SNOWFLAKE_URL=

# Kafka
SCHEMA_REGISTRY_URL=

# dbt
DBT_TARGET=dev

# Dagster storage
DAGSTER_PG_DB=
DAGSTER_PG_USER=
DAGSTER_PG_PASSWORD=
```

### Bring the stack up

```bash
docker compose up -d --build

# wait for Kafka Connect to respond
curl -sf http://localhost:8083/connectors

# register the connectors
./scripts/register_connectors.sh
```

Before the first dbt execution, run the Snowflake scripts in order: `bootstrap_config.sql` (creates the `CONFIG` schema), then `streams_and_tasks.sql`.

**Rotating the Snowflake key means re-running that last command.** `register_connectors.sh` resolves the private key with `envsubst` and `PUT`s the *resolved* configuration into Kafka Connect, so the key is copied into Connect's own config topic at registration time. Changing the key afterwards changes nothing there — the sink keeps presenting the old one and fails authentication the next time it starts, while every other consumer (dbt, Dagster, the CI workflow) picks the new key up automatically because they read the file at `SNOWFLAKE_PRIVATE_KEY_PATH`. Debezium is unaffected; it authenticates against Postgres, not Snowflake. Learned the hard way on 2026-08-11, during a rotation of the `DAGSTER_SERVICE_USER` key: four sink tasks dead with `JWT token is invalid` while Debezium stayed green.

**The key now lives in exactly one place.** Until that same day, `.env` carried the private key twice — as `SNOWFLAKE_PRIVATE_KEY_PATH` and, separately, as the whole PKCS8 body in `SNOWFLAKE_PRIVATE_KEY`, because the Kafka connector and `sync_metadata.py` need it as a string. The duplication cost a leaked key (a `grep` over `.env` printed it in full) and turned rotation into a five-location procedure. Both consumers now derive the string from the `.p8`, and the variable is gone from `.env`; setting it still works, for environments that have not migrated.

There is a wrinkle worth knowing, and it is why the duplication existed at all: `SNOWFLAKE_PRIVATE_KEY_PATH` holds a path *inside the containers* (`./keys:/secrets:ro`), because dbt and Dagster are what consume it — while `register_connectors.sh` runs on the host, where `/secrets` does not exist. The script tries the literal path first and falls back to the same filename under `keys/`.

### dbt

dbt lives inside the Dagster container, with the project bind-mounted:

```bash
# offline validation — opens no warehouse connection
docker compose exec dagster-daemon \
  bash -c "cd /opt/dagster/dbt && dbt parse --target dev"

# materialization and tests
docker compose exec dagster-daemon \
  bash -c "cd /opt/dagster/dbt && dbt run  --select silver gold --target dev"
docker compose exec dagster-daemon \
  bash -c "cd /opt/dagster/dbt && dbt test --select silver gold --target dev"
```

If `dagster-daemon` is stuck in a restart loop (for instance with `dagster-postgres` down), every `exec` dies along with the container. Use a throwaway container instead, immune to the loop:

```bash
docker compose run --rm --no-deps --entrypoint bash dagster-daemon \
  -c "cd /opt/dagster/dbt && dbt test --select gold --target dev"
```

### Services

| Service | Port | Role |
|---|---|---|
| `zookeeper` | 2181 | Kafka coordination |
| `kafka` | 9092 | Broker |
| `schema-registry` | 8081 | Contracts and schematization |
| `postgres` | 5432 | CDC source database |
| `dagster-postgres` | — | Dagster storage, separate from the source |
| `kafka-connect` | 8083 | Debezium source + Snowflake sink |
| `dagster` | 3000 | Webserver |
| `dagster-daemon` | — | Schedules and sensors |
| `kafka-ui` | 8080 | Topic inspection |
| `jmx-exporter` / `kafka-exporter` | — | Broker metrics |
| `prometheus` | 9090 | Scraping and alerts |
| `grafana` | 3001 | Dashboards |

---

## Snowflake scripts

Run manually through SnowSQL or a worksheet — deliberately **not** automated, because account-governance changes require `ACCOUNTADMIN` and human review:

**On a fresh account, run them in this order:** `00_account_bootstrap` → `bootstrap_config` → `streams_and_tasks` → `snowflake_setup` → `create_readonly_role` → `verify_governance`. Until 2026-08-11 that sequence had no first step, and the consequence was larger than a missing file: nothing in this repository created the database, the warehouse, the role or the three service users, so the project could not be rebuilt from git at all. Every other script assumed those objects into existence. It is also why `deploy.yml` could never have worked against a `production` environment — there was no way to create one.

| Script | Purpose |
|---|---|
| `scripts/00_account_bootstrap.sql` | Database, schemas, warehouse, role, grants and the three service users. **Never executed** |
| `scripts/bootstrap_config.sql` | Creates the `CONFIG` schema and seeds `TABLE_METADATA` |
| `scripts/streams_and_tasks.sql` | Streams and Tasks feeding the sensors |
| `scripts/create_readonly_role.sql` | Read-only role for external tooling |
| `scripts/snowflake_setup.sql` | Creates `cdc_poc_monitor` and pins Time Travel. First run 2026-08-11 |
| `scripts/verify_governance.sql` | Audits Resource Monitor, Time Travel and consumption |
| `scripts/sync_metadata.py` | Syncs Schema Registry → `TABLE_METADATA` |
| `scripts/init.sql` | Source Postgres initialization |

---

## Security

- `.env`, `keys/`, `*.p8`, `*.key` and `*.pem` are gitignored.
- Snowflake authentication uses an RSA key pair; the private key is mounted as a volume, never baked into an image.
- Service identities are separated by function (Dagster has its own, running as `CDC_ROLE`), plus a read-only role for query tooling.
- Grafana starts with default credentials (`admin`/`admin`) — change them before exposing anything beyond `localhost`.

---

## CI/CD

`.github/workflows/deploy.yml` triggers on pushes to `main` that touch `connectors/`, `dbt/` or the compose files, and assembles `.env` from GitHub Secrets.

**It ran nine times and failed nine times, and nobody noticed.** This section previously claimed the workflow had never executed — a claim repeated in three places and never checked against GitHub. `gh run list` says otherwise: every push to `main` touching the trigger paths between 2026-08-07 and 2026-08-10 fired it, and each run died in 8 to 17 seconds at the `Subir/atualizar stack` step, on the missing `docker-compose.prod.yml`. The rest of the pipeline never got to execute.

That inverts this repository's usual failure story. The recurring theme has been things nobody had run; here something ran on a schedule, failed every single time, and the red mark went unread for four days. An untested pipeline and an unwatched one fail the same way, and the second is harder to notice because the machinery looks alive.

Reviewed line by line on 2026-08-11, it turned out to have five defects, of which the missing file was merely the first to bite:

1. It runs `docker compose up` and polls `localhost` **on the GitHub runner**, which is ephemeral. The stack rises with the job and dies with it; nothing is left standing anywhere. This is not a configuration slip — a real deployment needs a target that outlives the job, and this project has none.
2. `SNOWFLAKE_PRIVATE_KEY` was never written to `.env`, so `envsubst` would have registered the Kafka sink with an empty key — a silent failure, since Debezium stays healthy either way.
3. `SNOWFLAKE_PRIVATE_KEY_PATH` pointed at `/run/secrets/rsa_key.p8`, which no step created.
4. `docker-compose.prod.yml` was referenced twice and never existed.
5. `register_connectors.sh --env prod` looks for `.env.prod`, while the workflow writes `.env`; the script would have aborted.

Defects 2 through 5 are fixed. Defect 1 is not fixable without a deployment target, so the trigger became `workflow_dispatch` — a broken pipeline that fires on every push is worse than one that waits to be called deliberately.

`ci.yml` is the half that does run. It needs no Snowflake, no Kafka and no host: it parses the dbt project with throwaway credentials (`dbt parse` never opens a connection), checks shell syntax, validates every YAML and connector JSON, and asserts that files referenced from executable lines of the workflows actually exist. A second job runs `ruff check .` over the Python and `yamllint` over `connectors/` and `observability/`; both are green, and the 26 findings that existed when ruff was introduced were cleared before the job was added rather than after — a gate that starts red teaches people to skip it. That last check is aimed squarely at the defect class this repository keeps producing — it is how `docker-compose.prod.yml` and `scripts/snowflake_setup.sql` went missing for months without anyone noticing.

---

## Known gaps and unverified claims

Everything below is either untested or waiting on a human decision. It is listed here rather than left implicit, because the expensive failures on this project have all come from something nobody had run yet.

### Closed on 2026-08-11 by one `ACCOUNTADMIN` run

The three service identities under `keys/` hold exactly one role each — `DAGSTER_SERVICE_USER` and `DATA_AGENTS_MCP_USER` on `CDC_ROLE`, `CURSOR_MCP_USER` on `CDC_ROLE_RO`. None can assume `ACCOUNTADMIN`, which is why these three sat unanswered for a week. A single execution of `scripts/verify_governance.sql` on a human's worksheet closed all of them, and the most consequential answer was the one nobody expected: the cost guardrail the project documented does not exist.

| Claim | Status | How to close it |
|---|---|---|
| ~~Billed credit consumption~~ | **Answered on 2026-08-11: ≈ 1.8 credits on `CDC_WH`, ≈ 3.8 across the whole account**, with two consecutive days at exactly zero on the pipeline. Half the account's compute belongs to `COMPUTE_WH`, not to this project, and the entire serverless ingestion path cost 0.0005. Figures, the reconciliation against query time, and the correction of an earlier overstatement are in the cost section above | Closed |
| ~~Account-level Resource Monitor~~ | **Answered on 2026-08-11: there was none, and now there is one.** Under `ACCOUNTADMIN`, `SHOW RESOURCE MONITORS` returned zero rows and `SHOW PARAMETERS LIKE 'RESOURCE_MONITOR' IN ACCOUNT` returned nothing, with `CDC_WH.resource_monitor` null. Zero rows under `CDC_ROLE` had been ambiguous; under `ACCOUNTADMIN` it was an answer. `cdc_poc_monitor` was created the same day at 09:54 and bound to the warehouse — note that it is a **warehouse-level** cap, so serverless consumption still has no ceiling | Closed |
| ~~The two-monitor hypothesis (`cdc_trial_monitor` vs `cdc_poc_monitor`)~~ | **Refuted.** Neither existed. The hypothesis assumed two monitors coexisting for different purposes; the truth was none. `cdc_poc_monitor` was to be created by `scripts/snowflake_setup.sql`, a file that did not exist — consistent with it never having run. That script was written and executed for the first time on 2026-08-11; `cdc_trial_monitor` was never real | Closed |

A fourth claim used to sit in this table and has left it. The 1-day Time Travel on Bronze was checked on 2026-08-11 with `SHOW TABLES IN SCHEMA CDC_POC.BRONZE` under `CDC_ROLE_RO` — it never needed `ACCOUNTADMIN`, and listing it as blocked was an error. All 20 objects in the schema report `retention_time = 1`: the 10 landing tables written by the connector and the 10 transient `BRONZE_*` tables built by dbt.

The number was right, but at the time it was not a design decision. dbt-snowflake materializes tables as `TRANSIENT` by default, and 1 day is the maximum a transient table can hold; the permanent landing tables sat at the account default, with nothing in `dbt_project.yml` setting a retention. The guarantee was real and would have changed silently if that default moved.

Half of that fragility is now gone. `scripts/snowflake_setup.sql` set `DATA_RETENTION_TIME_IN_DAYS = 1` on the database itself on 2026-08-11, confirmed by `SHOW PARAMETERS` reporting `level = DATABASE` instead of an inherited value — so a change to the account default no longer reaches Bronze. The transient ceiling on the dbt-built tables is untouched, but a ceiling behaves differently from a default: it cannot drift upward without someone changing the materialization.

### Untested code paths

| Path | Why it matters |
|---|---|
| `deploy.yml` | Ran 9 times, failed 9 times, unnoticed for four days — and is undeployable by design, since it targets the ephemeral runner. Four smaller defects fixed on 2026-08-11; trigger is now manual-only. A real one needs a deployment target this project does not have — see [CI/CD](#cicd) |
| Nobody watches the workflow status | The nine red runs are the evidence. `ci.yml` is only useful if someone reads it; a failing check that no one opens is indistinguishable from no check at all. The credit monitor now emails before it acts, which was the one half of this with a technical fix — GitHub Actions still notifies nobody |

The incremental MERGE used to head this table and was closed on 2026-08-11. A single `closed` event for payment `55555555-5555-5555-5555-555555555555` was inserted into `payment_events` in Postgres and left to travel the real path — Debezium, Kafka, the Snowflake sink — landing with `__OP = 'c'`. The chain then ran end to end: `gold_payment_lifecycle` reported `SUCCESS 1` and the table stayed at 8 rows, so the row was updated, not inserted. `total_eventos` went 2 → 3, `foi_fechado` false → true, `fechado_em` and `segundos_ate_fechamento` filled in. All 34 tests in the selection passed.

What that run actually proves is narrower than "the merge works", and more useful. `criado_em` and `autorizado_em` survived the update. The model reads the **full** history of every affected `payment_id` rather than only the new events, precisely so that a late `closed` arriving alone cannot overwrite a good row with a mutilated one; the comment at the top of the model has claimed that since it was written, and this is the first run to demonstrate it. The event carried an internal `timestamp` above the global watermark by design — the `alvo` CTE compares against `MAX(ultimo_evento_ms)` over the whole table, so an event older than the newest event of *any other* payment would have been skipped. That remains an untested edge, and a real out-of-order arrival would hit it.

### Decided on 2026-08-11

Four items sat here waiting on a human. All four were answered on the same day; what each one cost is recorded so the reasoning survives the decision.

| Item | The call |
|---|---|
| Test severity convention | **Ratified, retroactively.** Decision 3 had said in bold not to apply the convention before acceptance, and the Silver/Gold build applied it anyway, to 102 tests. Ratifying legalises a state that had been in production for a day; the original sentence is preserved in the decision text rather than deleted, because the order in which this happened is the useful part. Still open: the 83 Bronze tests never passed through the criterion — they sit at `error` by default, not by analysis |
| `payment_id` cardinality | **Accepted as scaffolding.** The three payment models are now marked as such in `dbt/models/gold/schema.yml`, so the warning travels with the model instead of living only here. Regenerating the seed would invalidate the cost and performance numbers measured above, and the models demonstrate the pattern correctly either way. The sharper symptom stands: `orders.payment_key` has 410 distinct values and **zero** intersection with `payment_events.payment_id`, so orders and payments cannot be joined in this database |
| Missing `DEFINE` and `DESIGN` for the Silver and Gold layers | **Debt accepted, not repaid.** They will not be written retroactively. `BUILD_REPORT_CAMADAS_SILVER_GOLD.md` already documents what exists and admits the deviation in its opening paragraph; a `DEFINE` written after the build would describe what was built rather than what was promised, adding form without control |
| `GOVERNANCA_QUALIDADE_DADOS` | **Closed — and the description here was wrong.** The feature was never missing code: its `DESIGN` file manifest lists exactly one file, itself, and states that no production code is generated. What was missing was the phase-3 record and the human acceptance Decision 3 demanded. Both now exist in `BUILD_REPORT_GOVERNANCA_QUALIDADE_DADOS.md` |

One thing surfaced while closing these. The `DEFINE` behind that last feature scored 11/15 on clarity; the Define gate published in `.claude/sdd/_index.md` is 12/15. It failed its own gate, produced a `DESIGN` anyway, and that `DESIGN` produced the severity convention that reached 102 tests in production. No single step did damage — the decisions hold up, and the convention proved correct in practice. The gate simply never stopped anything, and nobody noticed for a week. It is recorded in the `DEFINE` rather than fixed by rewriting the document to a passing score.

### Operational fragility

The Dagster asset graph is frozen at container import time. **Adding a dbt model requires restarting `dagster-daemon`** — without it, the pipeline keeps running successfully while silently ignoring the new layer. This actually happened on 2026-08-10: Gold models existed and were materializable by hand for about an hour while the sensor-triggered job still only knew about Bronze and Silver.

---

## Methodology — AgentSpec SDD

The repository follows a five-phase specification workflow — brainstorm, define, design, build, ship — with artifacts versioned under `.claude/sdd/`. Every delivered feature leaves behind its `DEFINE`, its `DESIGN`, a build report and a closing record, which keeps architectural decisions traceable long after the merge.

---

## What Evolved from sdd-kafka-snowflake

This repository is the second iteration. The first — `sdd-kafka-snowflake`,
referred to internally as *v5-delivery* — reached Snowflake and worked; what it
did not do was survive scrutiny about how it got there. Every row below is a
decision recorded in [`docs/adr/`](docs/adr/), with the predecessor's behaviour
taken from the `Context` section of the design document that superseded it.

| Component | sdd-kafka-snowflake | sdd-kafka-snowflake-2 |
|---|---|---|
| Ingestion | Connector v2.1.2, `ingestion.method=SNOWPIPE` — classic, file-based, on a deprecated generation | v4 `SnowflakeStreamingSinkConnector`, row-level commits ([ADR-0029](docs/adr/0029_snowpipe_streaming_as_the_ingestion_path.md)) |
| Payload | One `RECORD_CONTENT` VARIANT, unpacked at the top of every Bronze model | Natively typed columns; types read from the registry, not inferred ([ADR-0021](docs/adr/0021_kafka_connector_v4_schematization.md)) |
| Ingestion scope | 20 domains, 10 of which never fed Silver or Gold | 10 Tier-1 domains — the ones that reach a Gold aggregation ([ADR-0022](docs/adr/0022_tier_1_ingestion_scope.md)) |
| Pipeline trigger | Dagster sensor polling `MAX(create_time)` across 20 Bronze tables every 60s, colliding with `AUTO_SUSPEND=60s` | Streams + Triggered Tasks; a false `WHEN` engages no warehouse ([ADR-0019](docs/adr/0019_streams_and_triggered_tasks_as_the_gate.md)) |
| Sensor bookkeeping | Global `detected_at` watermark — a high-volume domain could advance it past a low-volume one, which then stopped being processed | `consumed = FALSE`, no timestamp filter ([ADR-0024](docs/adr/0024_sensor_without_global_watermark.md)) |
| Dagster storage | SQLite — fragile under concurrent writes and crash | Dedicated PostgreSQL, deliberately not the CDC source ([ADR-0018](docs/adr/0018_dedicated_postgres_for_dagster_storage.md)) |
| Credit brake | Two conflicting descriptions in two documents; **neither monitor existed** | `cdc_poc_monitor`, verified and created 2026-08-11 ([ADR-0020](docs/adr/0020_resource_monitor_canonicalization.md)) |
| Test severity | Whatever the dbt default gave | 185 tests classified case by case against a stated criterion ([ADR-0027](docs/adr/0027_severity_convention.md)) |
| Bronze invariant | Append-only assumed by three mechanisms, stated by none | Declared, with the enforcement grant identified as the next step ([ADR-0025](docs/adr/0025_bronze_is_append_only.md)) |
| Decision records | ADR numbers cited in code; **the files did not survive the move** | 12 records, numbering preserved so the citations resolve |

The through-line is not "v2 is faster". It is that in v1 the expensive parts were
assumptions nobody had checked — a monitor, a watermark, an ingestion method, a
grain. Most of what changed here started as somebody going to look.

---

## Interview Cheat Sheet

**On making idleness free:**
> "Polling asks a warehouse whether anything happened, and asking costs money.
> We moved the question into Snowflake: a Stream per Bronze table and a Task with
> `WHEN SYSTEM$STREAM_HAS_DATA`. The predicate is evaluated in the control plane,
> so a false answer engages no compute. And because the Stream sits on the real
> Bronze table, it can only fire after Snowpipe commits — there is no gap between
> 'Kafka has it' and 'Snowflake has it' to paper over with a debounce."

**On the watermark we deleted:**
> "The sensor used `detected_at > cursor AND consumed = FALSE`. That is the
> conventional shape and it is wrong here: ten Tasks write whenever their own
> stream fires, so `detected_at` across domains is not a monotonic sequence. A
> high-volume domain advances the shared cursor and a low-volume one falls behind
> it — the row is not lost, it is invisible. We removed the cursor entirely.
> `consumed = FALSE` was already sufficient; the cursor was adding an ordering
> assumption the data never satisfied."

**On the Schema Registry doing more than serialization:**
> "The registry is the type authority for the sink, the `BACKWARD` compatibility
> gate, and — through the Avro `doc` field — the CDC contract itself.
> `sync_metadata.py` parses it into `CONFIG.TABLE_METADATA`, and `resolve_cdc`
> reads that. So adding a domain needs no code change: register the subject with a
> populated `doc` and the pipeline picks it up."

**On test severity:**
> "A test is `error` if the violation, propagated, would make a business metric
> objectively wrong. Our 10 warnings are referential gaps inherited from the source
> database — they degrade a metric without falsifying it. The 83 Bronze tests were
> reviewed one by one and all stayed `error`: every tested column there is
> structural load — MERGE key, delete filter, dedup ordering, incremental
> watermark."

**On what this design does *not* solve:**
> "The gate makes idleness free, not throughput cheap. When those ten Tasks fire
> they run on `CDC_WH`, which bills a 60-second minimum per resume — under
> continuous traffic they would keep the warehouse on permanently, which is the
> behaviour we removed. It solves the bursty case, and this workload is bursty.
> Saying that out loud is the difference between a design and a claim."

**On the failure that taught the most:**
> "Two documents described two different Resource Monitors, so we went to check
> which was real. Neither existed — no account monitor, no account parameter, no
> warehouse binding. The question was badly formed and the only thing that revealed
> it was running the query. That is why the ADR keeps the original question next to
> its refutation instead of quietly rewriting history."

---

## License

[MIT](LICENSE) — free to use, modify, and learn from.

## Author

Built by [Christian Rocha](https://github.com/christiandrocha) as a hands-on exploration of CDC streaming into Snowflake, with cost governance as a first-class concern rather than an afterthought. Feedback and questions welcome via GitHub issues.
