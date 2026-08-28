{% macro resolve_cdc(source_ref, model_name=none) %}
{#
    Resolve a Bronze CDC table into the entity's current state, for Silver.
    The strategy comes from CONFIG.TABLE_METADATA via get_config_for().

    Strategies:
        upsert -> deduplicate by unique_key, keep the most recent version and
                  discard rows deleted at the source
        append -> no deduplication, discard only the deleted row
        log    -> keep everything, deletes included, as a historical record

    Input contract: `source_ref` must point at a Bronze model of this project,
    exposing the control columns `op`, `source_ts_ms`, `kafka_offset` and
    `kafka_partition`. See dbt/models/bronze/bronze_*.sql.

    Use in a Silver model:
        {{ resolve_cdc(ref('bronze_orders')) }}
        {{ resolve_cdc(ref('bronze_orders'), model_name='silver_orders_enriched') }}

    PORTED FROM THE PREVIOUS PROJECT (v6, 2026-08-10). Three changes:

    1. Deterministic tie-break. The previous version ordered only by
       `source_ts_ms DESC`. Debezium emits that field in MILLISECONDS: two
       changes to the same row inside the same millisecond -- common in bulk
       loads and cascading UPDATEs -- tie, and ROW_NUMBER picks one of them
       non-deterministically. The same `dbt run` executed twice could produce
       a different Silver. The tie-break now continues on `kafka_offset DESC`,
       which is monotonic per partition, the same thing the Bronze models
       already do in their own deduplication.

    2. NULL-safe delete filter, PLUS tombstone discard. It was `op != 'd'`.
       In SQL, `NULL != 'd'` is NULL, not TRUE -- so any row with a null `op`
       was silently discarded despite not being a delete. Replaced with
       `IS DISTINCT FROM`, which treats NULL as a value.

       But that change alone opens a hole, measured on 2026-08-10: the
       connector runs with `drop.tombstones=false`, so every DELETE produces
       TWO messages -- the row rewritten with `__OP='d'` and, right after, a
       tombstone (null value) that the sink materializes as a row of all-null
       columns, unique_key included. Under `op != 'd'` that garbage vanished
       by accident (NULL != 'd' is NULL); under `IS DISTINCT FROM` it started
       leaking into Silver. Hence the `{{ id_col }} IS NOT NULL`: a row with
       no business key is not a CDC record, it is an artifact of Kafka's
       compaction protocol. The discard is now explicit and intentional, not
       a side effect of NULL semantics.

    3. The ranking helper column leaves the result via EXCLUDE, as before, but
       the CDC control columns (`op`, `source_ts_ms`, `kafka_*`) REMAIN in
       Silver on purpose: they are the lineage tying each row to the Kafka
       event that produced it, and the previous project's
       `gold_payment_lifecycle` depended on them.

    NOT TO BE CONFUSED with the deduplication in the Bronze models. There it is
    per ingestion batch (idempotency over Snowpipe Streaming redelivery); here
    it is over the entity's whole history (collapsing N versions into 1 state).
#}

{% set calling_model = model_name or this.name %}
{% set cfg = get_config_for(calling_model) %}
{% set strategy = cfg.get('cdc_strategy', 'upsert') %}
{% set id_col   = cfg.get('unique_key', 'id') %}

{% if strategy == 'upsert' %}

    {# Entity: one row per key, the most recent non-deleted state. #}
    WITH ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY {{ id_col }}
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _cdc_row_num
        FROM {{ source_ref }}
    )
    SELECT * EXCLUDE (_cdc_row_num)
    FROM ranked
    WHERE _cdc_row_num = 1
      AND op IS DISTINCT FROM 'd'
      AND {{ id_col }} IS NOT NULL

{% elif strategy == 'append' %}

    {# Fact: every row counts, no deduplication; only delete and tombstone go. #}
    SELECT *
    FROM {{ source_ref }}
    WHERE op IS DISTINCT FROM 'd'
      AND {{ id_col }} IS NOT NULL

{% elif strategy == 'log' %}

    {# Log/audit: nothing is discarded, deletes included -- only the tombstone,
       which carries no information at all (not even which key died). #}
    SELECT *
    FROM {{ source_ref }}
    WHERE {{ id_col }} IS NOT NULL

{% else %}

    {{ exceptions.raise_compiler_error(
        "unknown cdc_strategy '" ~ strategy ~ "' for model "
        ~ calling_model ~ ". Valid values: upsert | append | log. "
        ~ "Check CONFIG.TABLE_METADATA or run scripts/sync_metadata.py."
    ) }}

{% endif %}

{% endmacro %}
