{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = 'event_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: ML recommendation events. Types observed in the data on
-- 2026-08-10: recommendation_served, add_to_cart, click, view. (Until that
-- date this comment said "view, click, purchase, dismiss" -- a guess that
-- never matched the source and that contaminated the Silver test.)
-- The source column is named TIMESTAMP; here it is quoted and comes out as
-- event_timestamp, so it does not collide with the TIMESTAMP type in the
-- generated SQL.
-- Merge on event_id -- idempotent over Snowpipe Streaming redelivery.
--
-- v4: columns arrive TYPED and UPPERCASE (schematization by the
-- SnowflakeStreamingSinkConnector), so there is no more
-- RECORD_CONTENT:field::TYPE extraction. RECORD_METADATA is still written by
-- the connector (the snowflake.metadata.* keys remain in JAR 4.1.0) and still
-- serves as the incremental watermark and the deduplication tie-break.

WITH source AS (
    SELECT
        EVENT_ID                           AS event_id,
        USER_ID                            AS user_id,
        PRODUCT_ID                         AS product_id,
        EVENT_TYPE                         AS event_type,
        "TIMESTAMP"                        AS event_timestamp,
        DT_CURRENT_TIMESTAMP               AS dt_current_timestamp,

        __OP                               AS op,
        __SOURCE_TS_MS                     AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT     AS kafka_offset,
        RECORD_METADATA:partition::INT     AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT AS kafka_created_at

    FROM {{ source('bronze_raw', 'RECOMMENDATIONS') }}

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
