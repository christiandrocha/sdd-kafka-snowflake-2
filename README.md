# sdd-kafka-snowflake

End-to-end **Change Data Capture** pipeline: PostgreSQL → Debezium → Kafka → Snowflake, with layered dbt modeling, Dagster orchestration, and observability through Prometheus + Grafana.

What sets this project apart is not the stack — it is the **control**. Each domain's CDC strategy lives in a metadata table in Snowflake rather than in SQL; pipeline triggering passes through a gate that queries Kafka before waking the warehouse; and every layer's invariants are tested, with severity chosen case by case.

> Source code comments and commit messages are written in Portuguese. This README is in English.

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
| CI/CD (`.github/workflows/deploy.yml`) | Never executed | — |

All 10 warnings are referential-integrity checks traced back to the source database — see [Data quality](#data-quality).

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

### Domains

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

Both Dagster sensors (`bronze_new_data_sensor` and `registry_new_subject_sensor`, 60-second interval) **query Prometheus before Snowflake**. With no new Kafka traffic, the sensor skips without opening a connection:

```
Sensor bronze_new_data_sensor skipped: Sem atividade no Kafka (via Prometheus) — Snowflake não consultado.
```

At rest, running this pipeline costs zero credits. The design is completed by 1-day Time Travel on Bronze tables — verified on 2026-08-11, though inherited from defaults rather than configured, see below — and by an account-level Resource Monitor that is still unverified. Both are audited by `scripts/verify_governance.sql`.

### What it actually costs

`CDC_WH` is an **X-Small** (1 credit/hour), billed per second with a **60-second minimum per resume**, `auto_suspend=60`.

Measured on 2026-08-10, across six hours that included building all three layers from scratch and running the full test suite several times:

```
579 queries · 72.7 seconds of execution time  ≈ 0.02 credits of pure compute
```

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

185 dbt tests: 83 in Bronze, 72 in Silver, 30 in Gold.

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
.claude/sdd/          specification workflow records (define → design → build → ship)
```

---

## Running it

### Prerequisites

- Docker and Docker Compose
- A Snowflake account with RSA key-pair authentication
- `keys/` holding the private `.p8` key (outside version control)

### Configuration

Create a `.env` at the repository root — it is gitignored and must never be committed:

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

| Script | Purpose |
|---|---|
| `scripts/bootstrap_config.sql` | Creates the `CONFIG` schema and seeds `TABLE_METADATA` |
| `scripts/streams_and_tasks.sql` | Streams and Tasks feeding the sensors |
| `scripts/create_readonly_role.sql` | Read-only role for external tooling |
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

**It has never run against a real environment.** Before first use, note that it still references two files absent from the repository: `docker-compose.prod.yml` and `scripts/snowflake_setup.sql`.

---

## Known gaps and unverified claims

Everything below is either untested or waiting on a human decision. It is listed here rather than left implicit, because the expensive failures on this project have all come from something nobody had run yet.

### Cannot be verified with the credentials in this repository

The three service identities under `keys/` hold exactly one role each — `DAGSTER_SERVICE_USER` and `DATA_AGENTS_MCP_USER` on `CDC_ROLE`, `CURSOR_MCP_USER` on `CDC_ROLE_RO`. None can assume `ACCOUNTADMIN`, so the following need a human on a Snowflake worksheet:

| Claim | Status | How to close it |
|---|---|---|
| Billed credit consumption | **Unknown.** `SNOWFLAKE.ACCOUNT_USAGE` is not authorized for `CDC_ROLE`; the `INFORMATION_SCHEMA` metering function runs but returns no rows | `scripts/verify_governance.sql` step 4, as `ACCOUNTADMIN` |
| Account-level Resource Monitor | **Unknown.** The warehouse's own `resource_monitor` field is null, so any protection would have to be account-level. `SHOW RESOURCE MONITORS` returns zero rows under `CDC_ROLE` — and zero rows there is indistinguishable from "none exists", which is the trap the script now warns about | `verify_governance.sql` steps 1 and 2, as `ACCOUNTADMIN` |
| The two-monitor hypothesis (`cdc_trial_monitor` vs `cdc_poc_monitor`) | **Open** since the script was written | Same run resolves it |

A fourth claim used to sit in this table and has left it. The 1-day Time Travel on Bronze was checked on 2026-08-11 with `SHOW TABLES IN SCHEMA CDC_POC.BRONZE` under `CDC_ROLE_RO` — it never needed `ACCOUNTADMIN`, and listing it as blocked was an error. All 20 objects in the schema report `retention_time = 1`: the 10 landing tables written by the connector and the 10 transient `BRONZE_*` tables built by dbt.

The number is right, but it was never a design decision. dbt-snowflake materializes tables as `TRANSIENT` by default, and 1 day is the maximum a transient table can hold; the permanent landing tables sit at the account default. Nothing in `dbt_project.yml` sets a retention. The guarantee is real today and would change silently if either default moved.

### Untested code paths

| Path | Why it matters |
|---|---|
| Incremental MERGE **updating an existing row** in `gold_payment_lifecycle` | The no-op path is verified: a second run with no new events merged 0 rows and left the table intact. The path that recomputes a payment after a late event has never run, because no new payment event has arrived since the model was built. One INSERT into `payment_events` at the source would close it |
| CI/CD (`.github/workflows/deploy.yml`) | Never executed against any environment, and still references two files absent from the repository: `docker-compose.prod.yml` and `scripts/snowflake_setup.sql` |

### Waiting on a decision

| Item | The decision |
|---|---|
| Test severity convention | Applied to 102 Silver and Gold tests, but Decision 3 of `DESIGN_GOVERNANCA_QUALIDADE_DADOS` is still marked *Proposed — requires human review*. Ratify it or change it |
| `payment_id` cardinality | 2,210 events over 8 identifiers makes three Gold models structurally correct and commercially meaningless. Regenerate the seed data, or accept those models as scaffolding |
| Missing `DEFINE` and `DESIGN` for the Silver and Gold layers | The build happened without them; `BUILD_REPORT_CAMADAS_SILVER_GOLD.md` opens by declaring the gap. Write them retroactively or accept the debt explicitly |
| `GOVERNANCA_QUALIDADE_DADOS` | Has a `DEFINE` and a `DESIGN` and was never built |

### Operational fragility

The Dagster asset graph is frozen at container import time. **Adding a dbt model requires restarting `dagster-daemon`** — without it, the pipeline keeps running successfully while silently ignoring the new layer. This actually happened on 2026-08-10: Gold models existed and were materializable by hand for about an hour while the sensor-triggered job still only knew about Bronze and Silver.

---

## Workflow

The repository follows a five-phase specification workflow — brainstorm, define, design, build, ship — with artifacts versioned under `.claude/sdd/`. Every delivered feature leaves behind its `DEFINE`, its `DESIGN`, a build report and a closing record, which keeps architectural decisions traceable long after the merge.
