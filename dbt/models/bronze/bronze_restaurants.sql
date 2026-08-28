{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = 'uuid',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: restaurants. CNPJ is the restaurant_key used in orders.
-- Merge on uuid -- idempotent over Snowpipe Streaming redelivery.
--
-- v4: columns arrive TYPED and UPPERCASE (schematization by the
-- SnowflakeStreamingSinkConnector), so there is no more
-- RECORD_CONTENT:field::TYPE extraction. RECORD_METADATA is still written by
-- the connector (the snowflake.metadata.* keys remain in JAR 4.1.0) and still
-- serves as the incremental watermark and the deduplication tie-break.

WITH source AS (
    SELECT
        UUID                               AS uuid,
        RESTAURANT_ID                      AS restaurant_id,
        CNPJ                               AS cnpj,
        NAME                               AS name,
        ADDRESS                            AS address,
        CITY                               AS city,
        COUNTRY                            AS country,
        PHONE_NUMBER                       AS phone_number,
        CUISINE_TYPE                       AS cuisine_type,
        OPENING_TIME                       AS opening_time,
        CLOSING_TIME                       AS closing_time,
        AVERAGE_RATING                     AS average_rating,
        NUM_REVIEWS                        AS num_reviews,
        DT_CURRENT_TIMESTAMP               AS dt_current_timestamp,

        __OP                               AS op,
        __SOURCE_TS_MS                     AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT     AS kafka_offset,
        RECORD_METADATA:partition::INT     AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT AS kafka_created_at

    FROM {{ source('bronze_raw', 'RESTAURANTS') }}

    -- Discards the Kafka tombstone: `drop.tombstones=false` in Debezium makes
    -- every DELETE emit, right after the `__OP='d'` row, a null-valued message
    -- that the sink materializes as an entirely null row. Without this filter
    -- it lands here, and since the MERGE on UUID never matches a null key,
    -- every DELETE leaves a permanent junk row in Bronze.
    WHERE UUID IS NOT NULL

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
                PARTITION BY uuid
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped
