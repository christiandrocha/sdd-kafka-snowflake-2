{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = 'order_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: hub table. Links the domains through heterogeneous business keys:
-- user_key (CPF), restaurant_key (CNPJ), driver_key (string), payment_key and
-- rating_key (UUID). rating_key points at a removed Tier 2 domain.
-- Merge on order_id -- idempotent over Snowpipe Streaming redelivery.
--
-- v4: columns arrive TYPED and UPPERCASE (schematization by the
-- SnowflakeStreamingSinkConnector), so there is no more
-- RECORD_CONTENT:field::TYPE extraction. RECORD_METADATA is still written by
-- the connector (the snowflake.metadata.* keys remain in JAR 4.1.0) and still
-- serves as the incremental watermark and the deduplication tie-break.

WITH source AS (
    SELECT
        ORDER_ID                           AS order_id,
        ORDER_DATE                         AS order_date,
        TOTAL_AMOUNT                       AS total_amount,
        USER_KEY                           AS user_key,
        RESTAURANT_KEY                     AS restaurant_key,
        DRIVER_KEY                         AS driver_key,
        PAYMENT_KEY                        AS payment_key,
        RATING_KEY                         AS rating_key,
        DT_CURRENT_TIMESTAMP               AS dt_current_timestamp,

        __OP                               AS op,
        __SOURCE_TS_MS                     AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT     AS kafka_offset,
        RECORD_METADATA:partition::INT     AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT AS kafka_created_at

    FROM {{ source('bronze_raw', 'ORDERS') }}

    -- Discards the Kafka tombstone: `drop.tombstones=false` in Debezium makes
    -- every DELETE emit, right after the `__OP='d'` row, a null-valued message
    -- that the sink materializes as an entirely null row. Without this filter
    -- it lands here, and since the MERGE on ORDER_ID never matches a null key,
    -- every DELETE leaves a permanent junk row in Bronze.
    WHERE ORDER_ID IS NOT NULL

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
                PARTITION BY order_id
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped
