{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = 'event_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: payment lifecycle events.
-- Merge on event_id -- idempotent over Snowpipe Streaming redelivery.
-- Real cycle: created -> authorized -> captured -> succeeded -> settled -> closed
--                                              -> refunded -> closed
--
-- v4: the scalar columns arrive typed. The `event` field does NOT -- it is
-- JSONB in Postgres and Debezium serializes it as a string
-- (io.debezium.data.Json), so schematization creates VARCHAR and PARSE_JSON
-- is still required.
--
-- The event's internal timestamp arrives as an int OR a float in scientific
-- notation, both epoch ms. The CAST through FLOAT covers both forms.

WITH source AS (
    SELECT
        EVENT_ID                           AS event_id,
        PAYMENT_ID                         AS payment_id,
        PARSE_JSON(EVENT):event_name::VARCHAR AS event_name,

        CAST(
            PARSE_JSON(EVENT):timestamp::FLOAT AS BIGINT
        )                                  AS event_timestamp_ms,

        TO_TIMESTAMP_NTZ(
            CAST(PARSE_JSON(EVENT):timestamp::FLOAT AS BIGINT) / 1000
        )                                  AS event_timestamp,

        DT_CURRENT_TIMESTAMP               AS dt_current_timestamp,

        __OP                               AS op,
        __SOURCE_TS_MS                     AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT     AS kafka_offset,
        RECORD_METADATA:partition::INT     AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT AS kafka_created_at

    FROM {{ source('bronze_raw', 'PAYMENT_EVENTS') }}

    -- Discards the Kafka tombstone: `drop.tombstones=false` in Debezium makes
    -- every DELETE emit, right after the `__OP='d'` row, a null-valued message
    -- that the sink materializes as an entirely null row. Without this filter
    -- it lands here, and since the MERGE on EVENT_ID never matches a null key,
    -- every DELETE leaves a permanent junk row in Bronze.
    WHERE EVENT_ID IS NOT NULL

    {% if is_incremental() %}
      AND RECORD_METADATA:CreateTime::BIGINT > (
        SELECT COALESCE(MAX(kafka_created_at), 0) FROM {{ this }}
    )
    {% endif %}
),

deduped AS (
    SELECT * EXCLUDE (_row_num)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY event_id
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped
