# ADR 0030 — Avro + Schema Registry as the Contract Boundary

**Status:** Accepted
**Date:** 2026-08-04 (registry as metadata source added in the same cycle)

## Context

Debezium can emit JSON without any registry at all, and that is its path of least
resistance. It is also how a CDC pipeline acquires an untyped boundary: every
consumer downstream re-derives types from whatever the last message happened to
contain, and a source column changing from integer to decimal is discovered by a
dbt model producing wrong numbers rather than by anything failing.

This pipeline has three consumers of that boundary, not one:

- the **Snowflake sink**, which needs types to create natively typed columns;
- **dbt**, whose Bronze models assume those types;
- **`CONFIG.TABLE_METADATA`**, which drives each domain's CDC strategy —
  `resolve_cdc` reads it, so the strategy is data, not SQL.

## Decision

Avro on the wire, with the **Confluent Schema Registry as the contract**, at
`BACKWARD` compatibility.

```properties
# connectors/debezium.json
value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.url=http://schema-registry:8081

# docker-compose.yml
SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL: BACKWARD
```

And — the part that makes this more than a serialization choice — **the registry
is also the metadata source of truth.** The Avro schema's `doc` field carries the
CDC contract:

```
table_type=entity,cdc_strategy=upsert,unique_key=id
```

`scripts/sync_metadata.py` parses it and writes `CONFIG.TABLE_METADATA`;
`registry_new_subject_sensor` in Dagster notices new subjects and triggers the
sync. Registering a schema is therefore how a domain declares how it wants to be
processed.

## Rationale

**`BACKWARD` compatibility is the enforcement point.** A source change that
would break existing consumers is rejected at registration, in Kafka Connect,
before a single row reaches Snowflake. That converts the failure from "wrong
numbers in Gold, discovered later" into "the connector refuses the schema, now".

**The registry is the type authority the sink actually uses.** This is what makes
`snowflake.validation=client_side` viable in
[ADR-0021](0021_kafka_connector_v4_schematization.md): client-side validation
reads types from the registry — curated, versioned, compatibility-checked —
rather than inferring them from the first record observed, which carries a
documented risk of demoting `FLOAT64` to `NUMBER(38,0)` because the first row
happened to be whole.

**One boundary, three uses.** Serialization, type authority and processing
contract all resolve to the same artifact. That is why a new domain needs no
change to `resolve_cdc`, and none to any model, for its CDC strategy to be
resolved: register the subject with a populated `doc`, and the sensor, the
metadata table and the macro do the rest.

That is a claim about strategy resolution, not about onboarding. Bringing a
domain into the pipeline still edits the Debezium `table.include.list`, the
sink's `topics` and `snowflake.topic2table.map`, a Stream and a Task in
`streams_and_tasks.sql`, `sources.yml`, a Bronze model, a Silver model, both
`schema.yml` files, and three hardcoded domain lists — the `static_fallback` in
`get_table_config`, and the expectations in `assert_table_metadata_sem_drift` and
`assert_bronze_sem_backlog_do_landing`. Deriving those three from
`CONFIG.TABLE_METADATA` is the unclosed half of this decision.

## Alternatives considered

1. **JSON without a registry** (Debezium's default) — rejected. No enforcement
   point, no type authority for the sink, and the `doc` field has no equivalent,
   so the CDC strategy would have to move into SQL or into a hand-maintained
   table. It is the option that makes every later decision harder.
2. **JSON Schema or Protobuf in the registry** — both would give enforcement.
   Rejected in favour of Avro for the Debezium integration, which is the
   best-trodden path, and because the Snowflake connector's schematization is
   built around it.
3. **Types hardcoded in the dbt Bronze models** — rejected. It puts the contract
   downstream of the break: the model is where you would *discover* a source
   change, having already loaded it.
4. **`FULL` or `NONE` compatibility** — `NONE` removes the enforcement that is
   the point of this decision. `FULL` was not adopted: it forbids changes that
   are safe for this topology, where consumers are upgraded with the pipeline
   rather than independently.

## Consequences

- One more service to run (`cp-schema-registry`) and one more thing that can be
  down. The sensor handles it explicitly — `Schema Registry indisponível` is a
  skip reason, not a crash.
- Schema evolution becomes a deliberate act with a compatibility check attached,
  rather than a side effect of a source deploy.
- **The `doc` field is load-bearing and easy to leave empty.** A subject
  registered without it yields no metadata. `sync_metadata.py` treats an absent
  `doc` as "preserve what exists, never overwrite with defaults" and logs a
  `[SKIP]` — the one behaviour that keeps a careless registration from silently
  resetting a domain's CDC strategy.
- The registry holds state that is not in git. Losing it loses the subject
  history, though `CONFIG.TABLE_METADATA` retains the derived contract.

## See also

- `connectors/debezium.json` · `docker-compose.yml` — the converter and the compatibility level
- `scripts/sync_metadata.py` — the `doc` parser and the `[SKIP]` guard
- `dagster/pipeline/sensors.py` — `registry_new_subject_sensor`
- `dbt/macros/` — `resolve_cdc`, `get_table_config`, the consumers of the metadata table
