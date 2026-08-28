{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = 'shift_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: per-shift driver performance (earnings, distance, orders).
-- Merge on shift_id -- idempotent over Snowpipe Streaming redelivery.
--
-- v4: columns arrive TYPED and UPPERCASE (schematization by the
-- SnowflakeStreamingSinkConnector), so there is no more
-- RECORD_CONTENT:field::TYPE extraction. RECORD_METADATA is still written by
-- the connector (the snowflake.metadata.* keys remain in JAR 4.1.0) and still
-- serves as the incremental watermark and the deduplication tie-break.

WITH source AS (
    SELECT
        SHIFT_ID                           AS shift_id,
        DRIVER_ID                          AS driver_id,
        CITY                               AS city,
        REGION                             AS region,
        SHIFT_TYPE                         AS shift_type,
        LOGIN_METHOD                       AS login_method,
        DEVICE_OS                          AS device_os,
        START_TIME                         AS start_time,
        END_TIME                           AS end_time,
        SHIFT_DURATION_MIN                 AS shift_duration_min,
        NUM_ORDERS                         AS num_orders,
        DISTANCE_COVERED_KM                AS distance_covered_km,
        EARNINGS_BRL                       AS earnings_brl,
        SHIFT_RATING                       AS shift_rating,
        ISSUES_REPORTED                    AS issues_reported,
        AVAILABLE                          AS available,
        DT_CURRENT_TIMESTAMP               AS dt_current_timestamp,

        __OP                               AS op,
        __SOURCE_TS_MS                     AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT     AS kafka_offset,
        RECORD_METADATA:partition::INT     AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT AS kafka_created_at

    FROM {{ source('bronze_raw', 'DRIVER_SHIFTS') }}

    -- Discards the Kafka tombstone: `drop.tombstones=false` in Debezium makes
    -- every DELETE emit, right after the `__OP='d'` row, a null-valued message
    -- that the sink materializes as an entirely null row. Without this filter
    -- it lands here, and since the MERGE on SHIFT_ID never matches a null key,
    -- every DELETE leaves a permanent junk row in Bronze.
    WHERE SHIFT_ID IS NOT NULL

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
                PARTITION BY shift_id
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped
